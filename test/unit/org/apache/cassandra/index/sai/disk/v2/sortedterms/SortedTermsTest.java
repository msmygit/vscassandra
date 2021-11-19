/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.index.sai.disk.v2.sortedterms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.block.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.block.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.block.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.DIRECT_MONOTONIC_BLOCK_SHIFT;

public class SortedTermsTest extends SaiRandomizedTest
{
    @Test
    public void testLexicographicException() throws Exception
    {
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        try (IndexOutputWriter trieWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.TRIE_DATA);
             IndexOutputWriter bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.TERMS_DATA);
             MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META));
             NumericValuesWriter blockFPWriter = new NumericValuesWriter(indexDescriptor.version.fileNameFormatter().format(IndexComponent.BLOCK_OFFSETS, null),
                                                                         indexDescriptor.openPerSSTableOutput(IndexComponent.BLOCK_OFFSETS),
                                                                         metadataWriter, true))
        {
            SortedTermsWriter writer = new SortedTermsWriter(bytesWriter, blockFPWriter, trieWriter
            );

            ByteBuffer buffer = Int32Type.instance.decompose(99999);
            ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
            byte[] bytes1 = ByteSourceInverse.readBytes(byteSource);

            writer.add(ByteComparable.fixedLength(bytes1));

            buffer = Int32Type.instance.decompose(444);
            byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
            byte[] bytes2 = ByteSourceInverse.readBytes(byteSource);

            assertThrows(IllegalArgumentException.class, () -> writer.add(ByteComparable.fixedLength(bytes2)));

            writer.finish();
        }
    }

    // verify DirectMonotonicReader works
    @Test
    public void testDirectMonotonicReader() throws Exception
    {
        int count = 1000;

        try (ByteBuffersDirectory dir = new ByteBuffersDirectory())
        {
            try (IndexOutput offsetsOut = dir.createOutput("offsets", IOContext.DEFAULT);
                 IndexOutput metaOut = dir.createOutput("meta", IOContext.DEFAULT))
            {
                DirectMonotonicWriter offsetsWriter = DirectMonotonicWriter.getInstance(metaOut, offsetsOut, count, DIRECT_MONOTONIC_BLOCK_SHIFT);
                for (int x = 0; x < count; x++)
                {
                    offsetsWriter.add(x);
                }
                offsetsWriter.finish();
            }

            IndexInput metaInput = dir.openInput("meta", IOContext.DEFAULT);
            DirectMonotonicReader.Meta meta = DirectMonotonicReader.loadMeta(metaInput, count, DIRECT_MONOTONIC_BLOCK_SHIFT);
            IndexInput offsetsInput = dir.openInput("offsets", IOContext.DEFAULT);
            RandomAccessInput offsetSlice = offsetsInput.randomAccessSlice(0, offsetsInput.length());

            LongValues offsetsReader = DirectMonotonicReader.getInstance(meta, offsetSlice);

            for (int x = 0; x < count; x++)
            {
                assertEquals(x, offsetsReader.get(x));
            }
        }
    }

    @Test
    public void testSeekToTerm() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<byte[]> terms = new ArrayList<>();
        SortedTermsMeta meta = writeTerms(descriptor, terms);

        // iterate on terms ascending
        withSortedTermsReader(descriptor, meta, reader ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                long pointId = reader.getPointId(ByteComparable.fixedLength(terms.get(x)));
                assertEquals(x, pointId);
            }
        });

        // iterate on terms descending
        withSortedTermsReader(descriptor, meta, reader ->
        {
            for (int x = terms.size() - 1; x >= 0; x--)
            {
                long pointId = reader.getPointId(ByteComparable.fixedLength(terms.get(x)));
                assertEquals(x, pointId);
            }
        });

        // iterate randomly
        withSortedTermsReader(descriptor, meta, reader ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                int target = nextInt(0, terms.size());

                long pointId = reader.getPointId(ByteComparable.fixedLength(terms.get(target)));
                assertEquals(target, pointId);
            }
        });
    }

    @Test
    public void testAdvance() throws IOException
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<byte[]> terms = new ArrayList<>();
        SortedTermsMeta meta = writeTerms(descriptor, terms);

        withSortedTermsCursor(descriptor, meta, cursor ->
        {
            int x = 0;
            while (cursor.advance())
            {
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS41));
                assertArrayEquals(terms.get(x), bytes);

                x++;
            }

            // assert we don't increase the point id beyond one point after the last item
            assertEquals(cursor.pointId(), terms.size());
            assertFalse(cursor.advance());
            assertEquals(cursor.pointId(), terms.size());
        });
    }

    @Test
    public void testReset() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<byte[]> terms = new ArrayList<>();
        SortedTermsMeta meta = writeTerms(descriptor, terms);

        withSortedTermsCursor(descriptor, meta, cursor ->
        {
            assertTrue(cursor.advance());
            assertTrue(cursor.advance());
            String term1 = cursor.term().byteComparableAsString(ByteComparable.Version.OSS41);
            cursor.reset();
            assertTrue(cursor.advance());
            assertTrue(cursor.advance());
            String term2 = cursor.term().byteComparableAsString(ByteComparable.Version.OSS41);
            assertEquals(term1, term2);
            assertEquals(1, cursor.pointId());
        });
    }

    @Test
    public void testSeekToPointId() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<byte[]> terms = new ArrayList<>();
        SortedTermsMeta meta = writeTerms(descriptor, terms);


        // iterate ascending
        withSortedTermsCursor(descriptor, meta, cursor ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                cursor.seekToPointId(x);
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS41));
                assertArrayEquals(terms.get(x), bytes);
            }
        });

        // iterate descending
        withSortedTermsCursor(descriptor, meta, cursor ->
        {
            for (int x = terms.size() - 1; x >= 0; x--)
            {
                cursor.seekToPointId(x);
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS41));
                assertArrayEquals(terms.get(x), bytes);
            }
        });

        // iterate randomly
        withSortedTermsCursor(descriptor, meta, cursor ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                int target = nextInt(0, terms.size());
                cursor.seekToPointId(target);
                ByteComparable term = cursor.term();

                byte[] bytes = ByteSourceInverse.readBytes(term.asComparableBytes(ByteComparable.Version.OSS41));
                assertArrayEquals(terms.get(target), bytes);
            }
        });
    }

    @Test
    public void testSeekToPointIdOutOfRange() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<byte[]> terms = new ArrayList<>();
        SortedTermsMeta meta = writeTerms(descriptor, terms);

        withSortedTermsCursor(descriptor, meta, cursor ->
        {
            assertThrows(IndexOutOfBoundsException.class, () -> cursor.seekToPointId(-2));
            assertThrows(IndexOutOfBoundsException.class, () -> cursor.seekToPointId(Long.MAX_VALUE));
        });
    }

    @Test
    public void testRandomLengthTerms() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();

        List<String> terms = new ArrayList<>();
        SortedTermsMeta meta = writeMixedLengthTerms(descriptor, terms);

        // iterate ascending
        withSortedTermsCursor(descriptor, meta, cursor ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                cursor.seekToPointId(x);

                String term = UTF8Type.instance.compose(UTF8Type.instance.fromComparableBytes(cursor.term().asPeekableBytes(ByteComparable.Version.OSS41),
                                                                                              ByteComparable.Version.OSS41));
                assertEquals(terms.get(x), term);
            }
        });

        // iterate descending
        withSortedTermsCursor(descriptor, meta, cursor ->
        {
            for (int x = terms.size() - 1; x >= 0; x--)
            {
                cursor.seekToPointId(x);
                String term = UTF8Type.instance.compose(UTF8Type.instance.fromComparableBytes(cursor.term().asPeekableBytes(ByteComparable.Version.OSS41),
                                                                                              ByteComparable.Version.OSS41));
                assertEquals(terms.get(x), term);
            }
        });

        // iterate randomly
        withSortedTermsCursor(descriptor, meta, cursor ->
        {
            for (int x = 0; x < terms.size(); x++)
            {
                int target = nextInt(0, terms.size());
                cursor.seekToPointId(target);
                String term = UTF8Type.instance.compose(UTF8Type.instance.fromComparableBytes(cursor.term().asPeekableBytes(ByteComparable.Version.OSS41),
                                                                                              ByteComparable.Version.OSS41));
                assertEquals(terms.get(target), term);
            }
        });
    }

    private SortedTermsMeta writeTerms(IndexDescriptor indexDescriptor, List<byte[]> terms) throws IOException
    {
        try (IndexOutputWriter trieWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.TRIE_DATA);
             IndexOutputWriter bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.TERMS_DATA);
             MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META));
             NumericValuesWriter blockFPWriter = new NumericValuesWriter(indexDescriptor.version.fileNameFormatter().format(IndexComponent.BLOCK_OFFSETS, null),
                                                                         indexDescriptor.openPerSSTableOutput(IndexComponent.BLOCK_OFFSETS),
                                                                         metadataWriter, true))
        {
            SortedTermsWriter writer = new SortedTermsWriter(bytesWriter, blockFPWriter, trieWriter);

            for (int x = 0; x < 1000 * 4; x++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(x);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
                byte[] bytes = ByteSourceInverse.readBytes(byteSource);
                terms.add(bytes);

                writer.add(ByteComparable.fixedLength(bytes));
            }

            return writer.finish();
        }
    }

    private SortedTermsMeta writeMixedLengthTerms(IndexDescriptor indexDescriptor, List<String> terms) throws IOException
    {
        try (IndexOutputWriter trieWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.TRIE_DATA);
             IndexOutputWriter bytesWriter = indexDescriptor.openPerSSTableOutput(IndexComponent.TERMS_DATA);
             MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META));
             NumericValuesWriter blockFPWriter = new NumericValuesWriter(indexDescriptor.version.fileNameFormatter().format(IndexComponent.BLOCK_OFFSETS, null),
                                                                         indexDescriptor.openPerSSTableOutput(IndexComponent.BLOCK_OFFSETS),
                                                                         metadataWriter, true))
        {
            SortedTermsWriter writer = new SortedTermsWriter(bytesWriter, blockFPWriter, trieWriter);

            for (int x = 0; x < 1000 * 4; x++)
            {
                terms.add(randomSimpleString(8, 256));
            }
            terms.sort(String::compareTo);
            for (String term : terms)
            {
                writer.add(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose(term), v));
            }

            return writer.finish();
        }
    }

    @FunctionalInterface
    public interface ThrowingConsumer<T> {
        void accept(T t) throws IOException;
    }

    private void withSortedTermsReader(IndexDescriptor indexDescriptor,
                                       SortedTermsMeta meta,
                                       ThrowingConsumer<SortedTermsReader> testCode) throws IOException
    {
        MetadataSource metadataSource = MetadataSource.loadGroupMetadata(indexDescriptor);
        NumericValuesMeta blockPointersMeta = new NumericValuesMeta(metadataSource.get(indexDescriptor.version.fileNameFormatter().format(IndexComponent.BLOCK_OFFSETS, null)));

        try (FileHandle trieHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TRIE_DATA);
             FileHandle termsData = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TERMS_DATA);
             FileHandle blockOffsets = indexDescriptor.createPerSSTableFileHandle(IndexComponent.BLOCK_OFFSETS))
        {
            LongArray.Factory blockOffsetsReaderFactory = new MonotonicBlockPackedReader(blockOffsets, blockPointersMeta);
            SortedTermsReader reader = new SortedTermsReader(termsData, blockOffsetsReaderFactory, trieHandle, meta);
            testCode.accept(reader);
        }
    }

    private void withSortedTermsCursor(IndexDescriptor descriptor,
                                       SortedTermsMeta meta,
                                       ThrowingConsumer<SortedTermsReader.Cursor> testCode) throws IOException
    {
        withSortedTermsReader(descriptor, meta, reader ->
        {
            try (SortedTermsReader.Cursor cursor = reader.openCursor(SSTableQueryContext.forTest()))
            {
                testCode.accept(cursor);
            }
        });
    }
}
