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

package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.block.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.block.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsMeta;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsReader;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifier;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.IndexInput;

public class V2PrimaryKeyMap implements PrimaryKeyMap
{
    public static class V2PrimaryKeyMapFactory implements PrimaryKeyMap.Factory
    {
        private final LongArray.Factory tokenReaderFactory;
        private final SortedTermsReader sortedTermsReader;
        private FileHandle token = null;
        private IndexInput termsDataBlockOffsets = null;
        private FileHandle termsData = null;
        private FileHandle termsTrie = null;
        private final IPartitioner partitioner;
        private final PrimaryKey.PrimaryKeyFactory primaryKeyFactory;
        private final SSTableUniqueIdentifier generation;
        private final long size;

        public V2PrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable)
        {
            String tokensComponentName = indexDescriptor.version.fileNameFormatter().format(IndexComponent.TOKEN_VALUES, null);
            String sortedBytesComponentName = indexDescriptor.version.fileNameFormatter().format(IndexComponent.SORTED_BYTES, null);
            try
            {
                MetadataSource metadataSource = MetadataSource.loadGroupMetadata(indexDescriptor);
                NumericValuesMeta tokensMeta = new NumericValuesMeta(metadataSource.get(tokensComponentName));
                size = tokensMeta.valueCount;
                token = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES);
                this.tokenReaderFactory = new BlockPackedReader(token, tokensMeta);
                SortedTermsMeta sortedTermsMeta = new SortedTermsMeta(metadataSource.get(sortedBytesComponentName));
                this.termsData = indexDescriptor.createPerSSTableFileHandle(IndexComponent.SORTED_BYTES);
                this.termsDataBlockOffsets = indexDescriptor.openPerSSTableInput(IndexComponent.BLOCK_POINTERS);
                this.termsTrie = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TRIE_DATA);
                this.sortedTermsReader = new SortedTermsReader(termsData, termsDataBlockOffsets, termsTrie, sortedTermsMeta);
                this.partitioner = sstable.metadata().partitioner;
                this.primaryKeyFactory = indexDescriptor.primaryKeyFactory;
                this.generation = indexDescriptor.descriptor.generation;
            }
            catch (Throwable t)
            {
                throw Throwables.unchecked(Throwables.close(t, token, termsData, termsDataBlockOffsets, termsTrie));
            }
        }

        @Override
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap(SSTableQueryContext context) throws IOException
        {
            final LongArray rowIdToToken = new LongArray.DeferredLongArray(() -> tokenReaderFactory.openTokenReader(0, context));
            return new V2PrimaryKeyMap(rowIdToToken, sortedTermsReader, partitioner, primaryKeyFactory, generation, size);
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(token, termsData, termsDataBlockOffsets, termsTrie);
        }
    }

    private final LongArray rowIdToToken;
    private final SortedTermsReader sortedTermsReader;
    private final SortedTermsReader.Cursor cursor;
    private final IPartitioner partitioner;
    private final PrimaryKey.PrimaryKeyFactory primaryKeyFactory;
    private final SSTableUniqueIdentifier generation;
    private final long size;
    private final ByteBuffer tokenBuffer = ByteBuffer.allocate(Long.BYTES);

    private V2PrimaryKeyMap(LongArray rowIdToToken,
                            SortedTermsReader sortedTermsReader,
                            IPartitioner partitioner,
                            PrimaryKey.PrimaryKeyFactory primaryKeyFactory,
                            SSTableUniqueIdentifier generation,
                            long size) throws IOException
    {
        this.rowIdToToken = rowIdToToken;
        this.sortedTermsReader = sortedTermsReader;
        this.cursor = sortedTermsReader.openCursor();
        this.partitioner = partitioner;
        this.primaryKeyFactory = primaryKeyFactory;
        this.generation = generation;
        this.size = size;
    }

    @Override
    public SSTableUniqueIdentifier generation()
    {
        return generation;
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId) throws IOException
    {
        tokenBuffer.putLong(rowIdToToken.get(sstableRowId));
        tokenBuffer.rewind();
        PrimaryKey key = primaryKeyFactory.createKey(partitioner.getTokenFactory().fromByteArray(tokenBuffer))
                                .withSSTableRowId(sstableRowId)
                                .withPrimaryKeySupplier(() -> supplier(sstableRowId))
                                          .withGeneration(generation);
//        System.out.println("primaryKeyFromRowId(rowId = " + sstableRowId + ", generation = " + generation + ") " + key);
        return key;
    }

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey key) throws IOException
    {
        return sortedTermsReader.getPointId(v -> key.asComparableBytes(v));
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(cursor, rowIdToToken);
    }

    private PrimaryKey supplier(long sstableRowId)
    {
        try
        {
//            System.out.println("supplier(" + sstableRowId + ")");
            cursor.seekToPointId(sstableRowId);
            return primaryKeyFactory.createKey(cursor.term().asPeekableBytes(ByteComparable.Version.OSS41))
                                    .withSSTableRowId(sstableRowId);
        }
        catch (IOException e)
        {
            throw Throwables.cleaned(e);
        }
    }
}
