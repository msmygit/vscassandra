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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.sun.jna.Pointer;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.BKDTreeRamBuffer;
import org.apache.cassandra.index.sai.disk.v1.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v2.PerIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.PerSSTableFileProvider;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v2.V2SSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.v2.V2SegmentBuilder;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.index.sai.SAITester.createIndexContext;
import static org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader.IndexState;
import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_BKD_LISTENER;
import static org.apache.lucene.index.PointValues.Relation.CELL_CROSSES_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

//@Seed(value = "3A64D2D8C02FD322:C75D376B778B2264")
//@Seed(value = "62A3A1AD9A3E879B:9F9A441E2D9A76DD")
//@Seed(value = "F87B6805568FCE9C:4CA80945456C4B57")
@Seed(value = "8B4C74C2A762013B:3F9F1582B48184F0")
public class BlockIndexWriterTest extends NdiRandomizedTest
{
    @Test
    public void test() throws Exception
    {
       /* List<Pair<ByteComparable, IntArrayList>> list = new ArrayList();
        list.add(add("aaabbb", new int[] {0, 2})); // 2
        list.add(add("aaabbbbbb", new int[] {1, 3})); // 2
        list.add(add("aaabbbccccc", new int[] {4, 5, 6})); // 3
        list.add(add("zzzzzzzggg", new int[] {10, 11, 12})); // 3

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput out = dir.createOutput("file", IOContext.DEFAULT);

        //IndexComponents comps = newIndexComponents();
        IndexDescriptor indexDescriptor = newIndexDescriptor();*/

//        IndexOutputWriter indexOut = comps.createOutput(comps.kdTree);
//        IndexOutputWriter postingsOut = comps.createOutput(comps.kdTreePostingLists);

//        BlockIndexWriter prefixBytesWriter = new BlockIndexWriter(out, indexOut, postingsOut);
//
//        TermsIterator terms = new MemtableTermsIterator(null,
//                                                        null,
//                                                        list.iterator());
//
//        int pointCount = 0;
//        while (terms.hasNext())
//        {
//            ByteComparable term = terms.next();
//            PostingList postings = terms.postings();
//            while (true)
//            {
//                long rowID = postings.nextPosting();
//                if (rowID == PostingList.END_OF_STREAM) break;
//                prefixBytesWriter.add(term, rowID);
//                pointCount++;
//            }
//        }

//        BlockIndexWriter.BlockIndexMeta meta = prefixBytesWriter.finish();
//
//        postingsOut.close();
//
//        try (IndexInput input = dir.openInput("file", IOContext.DEFAULT);
//             IndexInput input2 = dir.openInput("file", IOContext.DEFAULT))
//        {
//            FileHandle indexFile = comps.createFileHandle(comps.kdTree);
//            BlockIndexReader reader = new BlockIndexReader(input,
//                                                           input2,
//                                                           indexFile,
//                                                           meta);

//            reader.traverseForFilePointers(fixedLength(new BytesRef("aaa")),
//                                           fixedLength(new BytesRef("aaabbbcczzzz")));

//            reader.traverse(fixedLength(new BytesRef("aaa")),
//                            fixedLength(new BytesRef("aaabbbcczzzz")));

//            BytesRef foundTerm = reader.seekTo(new BytesRef("aaabbbccddddd"));
//            System.out.println("foundTerm="+foundTerm.utf8ToString());

//            BytesRef term3 = reader.seekTo(3);
//            System.out.println("term3="+term3.utf8ToString());
//
//            BytesRef term1 = reader.seekTo(1);
//            System.out.println("term1="+term1.utf8ToString());
//
//            BytesRef term6 = reader.seekTo(6);
//            System.out.println("term6="+term6.utf8ToString());
//
//            BytesRef term8 = reader.seekTo(8);
//            System.out.println("term8="+term8.utf8ToString());
//
//            List<String> results = new ArrayList<>();
//            for (int x=0; x < pointCount; x++)
//            {
//                BytesRef term = reader.seekTo(x);
//                results.add(term.utf8ToString());
//                System.out.println("x="+x+" term=" + term.utf8ToString());
//            }
//            System.out.println("results="+results);
        }
    //}

    @Test
    public void testRanges() throws Exception
    {
        TreeRangeSet<Integer> set = TreeRangeSet.create();
        set.add(Range.closed(0, 10));
        set.add(Range.open(50, 60));

        boolean contains = set.contains(10);
//        System.out.println("contains="+contains);

        boolean encloses = set.encloses(Range.open(0, 10));
//        System.out.println("encloses="+encloses);

        boolean encloses2 = set.encloses(Range.open(4, 15));

//        System.out.println("encloses2="+encloses2);
    }

    public static class ArrayIndexIterator implements BlockIndexReader.IndexIterator
    {
        final IndexState[] states;
        int idx = 0;

        public ArrayIndexIterator(IndexState[] states)
        {
            this.states = states;
        }

        @Override
        public IndexState next() throws IOException
        {
            if (idx >= states.length)
            {
                return null;
            }
            return states[idx++];
        }

        @Override
        public IndexState current()
        {
            return states[idx - 1];
        }

        @Override
        public void close() throws IOException
        {

        }
    }

    @Test
    public void testMergeIterator() throws Exception
    {
        List<BlockIndexReader.IndexIterator> iterators = new ArrayList<>();
        iterators.add(new ArrayIndexIterator(new IndexState[] {
        new IndexState(new BytesRef("aaa"), 10),
        new IndexState(new BytesRef("aaa"), 20),
        new IndexState(new BytesRef("ccc"), 100),
        new IndexState(new BytesRef("ddd"), 110),
        new IndexState(new BytesRef("nnn"), 2000)}));

        iterators.add(new ArrayIndexIterator(new IndexState[] {
        new IndexState(new BytesRef("bbb"), 4),
        new IndexState(new BytesRef("bbb"), 8),
        new IndexState(new BytesRef("ggg"), 1000),
        new IndexState(new BytesRef("ggg"), 1001)}));

        MergeIndexIterators blockReaders = new MergeIndexIterators(iterators);
        while (true)
        {
            IndexState state = blockReaders.next();
            if (state == null) break;

//            System.out.println("merge state="+state);
        }
    }

    private BlockIndexReader createPerIndexReader(String indexName, List<Pair<ByteComparable, LongArrayList>> list) throws Exception
    {
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        IndexContext indexContext = createIndexContext(indexName, UTF8Type.instance);

        BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);

        BlockIndexWriter blockIndexWriter = new BlockIndexWriter(fileProvider, false);

        TermsIterator terms = new MemtableTermsIterator(null,
                                                        null,
                                                        list.iterator());
        while (terms.hasNext())
        {
            ByteComparable term = terms.next();
            PostingList postings = terms.postings();
            while (true)
            {
                long rowID = postings.nextPosting();
                if (rowID == PostingList.END_OF_STREAM)
                {
                    break;
                }
                blockIndexWriter.add(term, rowID);
            }
        }

        BlockIndexMeta meta = blockIndexWriter.finish();

        return new BlockIndexReader(fileProvider, false, meta);
    }

    private BlockIndexReader createPerSSTableReader(List<Pair<ByteComparable, Long>> list) throws Exception
    {
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        BlockIndexFileProvider fileProvider = new PerSSTableFileProvider(indexDescriptor);

        BlockIndexWriter blockIndexWriter = new BlockIndexWriter(fileProvider, false);

        Iterator<Pair<ByteComparable, Long>> terms = list.iterator();

        while (terms.hasNext())
        {
            Pair<ByteComparable, Long> entry = terms.next();
            blockIndexWriter.add(entry.left, entry.right);
        }

        BlockIndexMeta meta = blockIndexWriter.finish();

        return new BlockIndexReader(fileProvider, false, meta);
    }

    private Row createRow(ColumnMetadata column, ByteBuffer value)
    {
        Row.Builder builder1 = BTreeRow.sortedBuilder();
        builder1.newRow(Clustering.EMPTY);
        builder1.addCell(BufferCell.live(column, 0, value));
        return builder1.build();
    }

    @Test
    public void testPerIndexWriter() throws Exception
    {
        NamedMemoryLimiter memoryLimiter = new NamedMemoryLimiter(1, "SSTable-attached Index Segment Builder");

        IndexDescriptor indexDescriptor = newIndexDescriptor();
        String indexName = "column";

        IndexContext indexContext = createIndexContext(indexName, UTF8Type.instance);

        V2SSTableIndexWriter writer = new V2SSTableIndexWriter(indexDescriptor,
                                                               indexContext,
                                                               memoryLimiter,
                                                               () -> true);

        ColumnMetadata column = ColumnMetadata.regularColumn("sai", "internal", "column", UTF8Type.instance);

        ByteBuffer term1 = UTF8Type.instance.decompose("a");
        Row row1 = createRow(column, term1);
        DecoratedKey key1 = dk("0");

        writer.addRow(PrimaryKey.factory().createKey(key1, Clustering.EMPTY, 0), row1);

        ByteBuffer term2 = UTF8Type.instance.decompose("b");
        Row row2 = createRow(column, term2);
        DecoratedKey key2 = dk("1");

        writer.addRow(PrimaryKey.factory().createKey(key2, Clustering.EMPTY, 1), row2);

        writer.flush();

        BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);

        BlockIndexMeta blockIndexMeta = (BlockIndexMeta) V2OnDiskFormat.instance.newIndexMetadataSerializer().deserialize(indexDescriptor, indexContext);
        BlockIndexReader reader = new BlockIndexReader(fileProvider, false, blockIndexMeta);

        BlockIndexReader.IndexIterator iterator = reader.iterator();

        IndexState state = iterator.next();
        assertNotNull(state);
        assertEquals("a", stringFromTerm(state.term));
        assertEquals(0, state.rowid);
        state = iterator.next();
        assertNotNull(state);
        assertEquals("b", stringFromTerm(state.term));
        assertEquals(1, state.rowid);
        assertNull(iterator.next());
    }

    @Test
    public void testMerge() throws Exception
    {
        V2SegmentBuilder builder = new V2SegmentBuilder(UTF8Type.instance, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER);
        builder.add(ByteBuffer.wrap("aaa".getBytes(StandardCharsets.UTF_8)), 100);
        builder.add(ByteBuffer.wrap("aaa".getBytes(StandardCharsets.UTF_8)), 101);
        builder.add(ByteBuffer.wrap("ccc".getBytes(StandardCharsets.UTF_8)), 1000);
        builder.add(ByteBuffer.wrap("ccc".getBytes(StandardCharsets.UTF_8)), 1001);

        IndexDescriptor indexDescriptor1 = newIndexDescriptor();

        IndexContext indexContext1 = createIndexContext("index1", UTF8Type.instance);

        BlockIndexMeta meta1 = builder.flush(indexDescriptor1, indexContext1);

        V2SegmentBuilder builder2 = new V2SegmentBuilder(UTF8Type.instance, V1OnDiskFormat.SEGMENT_BUILD_MEMORY_LIMITER);
        builder2.add(ByteBuffer.wrap("bbb".getBytes(StandardCharsets.UTF_8)), 200);
        builder2.add(ByteBuffer.wrap("bbb".getBytes(StandardCharsets.UTF_8)), 201);
        builder2.add(ByteBuffer.wrap("ddd".getBytes(StandardCharsets.UTF_8)), 2000);
        builder2.add(ByteBuffer.wrap("ddd".getBytes(StandardCharsets.UTF_8)), 2001);

        IndexDescriptor indexDescriptor2 = newIndexDescriptor();

        IndexContext indexContext2 = createIndexContext("index2", UTF8Type.instance);

        BlockIndexMeta meta2 = builder2.flush(indexDescriptor2, indexContext2);

        BlockIndexFileProvider fileProvider1 = new PerIndexFileProvider(indexDescriptor1, indexContext1);
        BlockIndexFileProvider fileProvider2 = new PerIndexFileProvider(indexDescriptor2, indexContext2);

        BlockIndexReader reader1 = new BlockIndexReader(fileProvider1, true, meta1);
        BlockIndexReader reader2 = new BlockIndexReader(fileProvider2, true, meta2);

        BlockIndexReader.IndexIterator iterator = reader1.iterator();
        BlockIndexReader.IndexIterator iterator2 = reader2.iterator();

        MergeIndexIterators merged = new MergeIndexIterators(Lists.newArrayList(iterator, iterator2));
        while (true)
        {
            IndexState state = merged.next();
            if (state == null) break;
//            System.out.println("  merged results term="+state.term.utf8ToString()+" rowid="+state.rowid);

        }
    }

    @Test
    public void testPerIndexSeekTo() throws Exception
    {
        List<Pair<ByteComparable, LongArrayList>> list = new ArrayList();
        list.add(add("aaa", new long[]{ 100 })); // 0
        list.add(add("aaabbb", new long[]{ 0, 1 })); // 0
        list.add(add("aaabbb", new long[]{ 2, 3, 4 })); // 1

        list.add(add("cccc", new long[]{ 15 })); // 2
        list.add(add("gggaaaddd", new long[]{ 200, 201, 203 })); // 3
        list.add(add("gggzzzz", new long[]{ 500, 501, 502, 503, 504, 505 })); // 4, 5
        list.add(add("zzzzz", new long[]{ 700, 780, 782, 790, 794, 799 })); //

        BlockIndexReader blockIndexReader = createPerIndexReader("index_test1", list);

        BlockIndexReader.BlockIndexReaderContext context = blockIndexReader.initContext();

        Pair<BytesRef, Long> pair = blockIndexReader.seekTo(new BytesRef("cccc"), context);
        assertEquals("cccc", stringFromTerm(pair.left));
        assertEquals(6, pair.right.intValue());

        Pair<BytesRef, Long> pair2 = blockIndexReader.seekTo(new BytesRef("gggzzzz"), context);
        assertEquals("gggzzzz", stringFromTerm(pair2.left));
        assertEquals(10, pair2.right.intValue());

        context.close();
        blockIndexReader.close();
    }

    @Test
    public void perSSTableSeekTo() throws Exception
    {
        List<Pair<ByteComparable, Long>> list = new ArrayList();
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("a"), v), 0L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("b"), v), 1L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("c"), v), 2L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("d"), v), 3L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("e"), v), 4L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("f"), v), 5L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("g"), v), 6L));
        list.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose("h"), v), 7L));

        try (BlockIndexReader reader = createPerSSTableReader(list);
             BlockIndexReader.BlockIndexReaderContext context = reader.initContext())
        {
            assertPair(reader.seekTo(new BytesRef("a"), context), "a", 0);
            assertPair(reader.seekTo(new BytesRef("b"), context), "b", 1);
            assertPair(reader.seekTo(new BytesRef("c"), context), "c", 2);
            assertPair(reader.seekTo(new BytesRef("d"), context), "d", 3);
            assertPair(reader.seekTo(new BytesRef("e"), context), "e", 4);
            assertPair(reader.seekTo(new BytesRef("f"), context), "f", 5);
            assertPair(reader.seekTo(new BytesRef("g"), context), "g", 6);
            assertPair(reader.seekTo(new BytesRef("h"), context), "h", 7);
            assertPair(reader.seekTo(new BytesRef("g"), context), "g", 6);
            assertPair(reader.seekTo(new BytesRef("f"), context), "f", 5);
            assertPair(reader.seekTo(new BytesRef("e"), context), "e", 4);
            assertPair(reader.seekTo(new BytesRef("d"), context), "d", 3);
            assertPair(reader.seekTo(new BytesRef("c"), context), "c", 2);
            assertPair(reader.seekTo(new BytesRef("b"), context), "b", 1);
            assertPair(reader.seekTo(new BytesRef("a"), context), "a", 0);

            assertEquals("a", stringFromTerm(reader.seekTo(0, context, true)));
            assertEquals("b", stringFromTerm(reader.seekTo(1, context, true)));
            assertEquals("c", stringFromTerm(reader.seekTo(2, context, true)));
            assertEquals("d", stringFromTerm(reader.seekTo(3, context, true)));
            assertEquals("e", stringFromTerm(reader.seekTo(4, context, true)));
            assertEquals("f", stringFromTerm(reader.seekTo(5, context, true)));
            assertEquals("g", stringFromTerm(reader.seekTo(6, context, true)));
            assertEquals("h", stringFromTerm(reader.seekTo(7, context, true)));
            assertEquals("h", stringFromTerm(reader.seekTo(7, context, true)));
            assertEquals("g", stringFromTerm(reader.seekTo(6, context, true)));
            assertEquals("f", stringFromTerm(reader.seekTo(5, context, true)));
            assertEquals("e", stringFromTerm(reader.seekTo(4, context, true)));
            assertEquals("d", stringFromTerm(reader.seekTo(3, context, true)));
            assertEquals("c", stringFromTerm(reader.seekTo(2, context, true)));
            assertEquals("b", stringFromTerm(reader.seekTo(1, context, true)));
            assertEquals("a", stringFromTerm(reader.seekTo(0, context, true)));
        }
    }

    private void assertPair(Pair<BytesRef, Long> pair, String key, long rowId)
    {
        assertEquals(key, stringFromTerm(pair.left));
        assertEquals(Long.valueOf(rowId), pair.right);
    }

    @Test
    public void randomPerSSTableSeekTo() throws Throwable
    {
        int numberOfIterations = randomIntBetween(10, 100);
        for (int iteration = 0; iteration < numberOfIterations; iteration++)
        {
//            System.out.println("iteration = " + iteration);
            doRandomPerTableSeekTo(iteration);
        }
    }

    private void doRandomPerTableSeekTo(int iteration) throws Throwable
    {
        int numberOfStrings = randomIntBetween(100, 2000);

        List<String> strings = new ArrayList<>();
        Set<String> duplicates = new HashSet<>();
        Map<Long, String> rowIdToStringMap = new HashMap<>();
        List<Pair<ByteComparable, Long>> data = new ArrayList();

        for (long index = 0; index < numberOfStrings; index++)
        {
            String string = randomSimpleString(2, 20);
            while (duplicates.contains(string))
                string = randomSimpleString(2, 20);
            duplicates.add(string);
            strings.add(string);
        }

        strings.sort(String::compareTo);

        for (long index = 0; index < numberOfStrings; index++)
        {
            String string = strings.get((int)index);
            rowIdToStringMap.put(index, string);
            data.add(Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose(string), v), index));
        }

        try (BlockIndexReader reader = createPerSSTableReader(data);
             BlockIndexReader.BlockIndexReaderContext context = reader.initContext())
        {
            for (int index = 0; index < randomIntBetween(500, 1500); index++)
            {
//                System.out.println("index = " + index);
//                if (iteration == 7 && index == 392)
//                    System.out.println();
                long rowId = nextLong(0, numberOfStrings);

                if (randomBoolean())
                {
                    Pair<BytesRef, Long> pair = reader.seekTo(new BytesRef(rowIdToStringMap.get(rowId)), context);

                    assertEquals(rowIdToStringMap.get(rowId), stringFromTerm(pair.left));
                    assertEquals(Long.valueOf(rowId), pair.right);
                }
                else
                {
                    assertEquals(rowIdToStringMap.get(rowId), stringFromTerm(reader.seekTo(rowId, context, true)));
                }
            }
        }
    }

    @Test
    public void testSameTerms() throws Exception
    {
        List<Pair<ByteComparable, LongArrayList>> list = new ArrayList();
        list.add(add("aaa", new long[]{ 100 })); // 0
        list.add(add("aaabbb", new long[]{ 0, 1 })); // 0
        list.add(add("aaabbb", new long[]{ 2, 3, 4 })); // 1

        list.add(add("cccc", new long[]{ 15 })); // 2
        list.add(add("gggaaaddd", new long[]{ 200, 201, 203 })); // 3
        list.add(add("gggzzzz", new long[]{ 500, 501, 502, 503, 504, 505 })); // 4, 5
        list.add(add("zzzzz", new long[]{ 700, 780, 782, 790, 794, 799 })); //

        BlockIndexReader blockIndexReader = createPerIndexReader("index_test1", list);

        BlockIndexReader.BlockIndexReaderContext context = blockIndexReader.initContext();

        Pair<BytesRef, Long> pair = blockIndexReader.seekTo(new BytesRef("cccc"), context);

//        System.out.println("term="+pair.left.utf8ToString()+" pointId="+pair.right);

        context.close();

        blockIndexReader.close();

//        BlockIndexWriter.RowPointIterator rowPointIterator = blockIndexReader.rowPointIterator();
//        while (true)
//        {
//            final BlockIndexWriter.RowPoint rowPoint = rowPointIterator.next();
//            if (rowPoint == null)
//            {
//                break;
//            }
//            System.out.println("rowPoint="+rowPoint);
//        }


        list = new ArrayList();
        list.add(add("cccc", new long[]{ 10, 11 })); // 2
        list.add(add("qqqqqaaaaa", new long[]{ 400, 405, 409 })); //
        list.add(add("zzzzzzzzzz", new long[] {20, 21, 24, 29, 30})); //

        BlockIndexReader blockIndexReader2 = createPerIndexReader("index_test12", list);

        blockIndexReader2.close();

//        BlockIndexReader.IndexIterator iterator = blockIndexReader.iterator();
//        BlockIndexReader.IndexIterator iterator2 = blockIndexReader2.iterator();
//
//        MergeBlockReaders merged = new MergeBlockReaders(Lists.newArrayList(iterator, iterator2));
//        while (true)
//        {
//            BlockIndexReader.IndexState state = merged.next();
//            if (state == null) break;
//            System.out.println("  merged results term="+state.term.utf8ToString()+" rowid="+state.rowid);
//        }

//        IndexDescriptor indexDescriptor = newIndexDescriptor();
//        String indexName = "index_test";
//
//        BlockIndexWriter blockIndexWriter = new BlockIndexWriter(indexName, indexDescriptor);
//
//        TermsIterator terms = new MemtableTermsIterator(null,
//                                                        null,
//                                                        list.iterator());
//
//        int pointCount = 0;
//        while (terms.hasNext())
//        {
//            ByteComparable term = terms.next();
//            PostingList postings = terms.postings();
//            while (true)
//            {
//                long rowID = postings.nextPosting();
//                if (rowID == PostingList.END_OF_STREAM)
//                {
//                    break;
//                }
//                blockIndexWriter.add(term, rowID);
//                pointCount++;
//            }
//        }
//
//        BlockIndexMeta meta = blockIndexWriter.finish();
//
//        BlockIndexReader blockIndexReader = new BlockIndexReader(indexDescriptor, indexName, meta);

        //final BlockIndexReader.IndexIterator iterator2 = blockIndexReader.iterator();

//        BlockIndexReader.IndexIterator iterator3 = new BlockIndexReader.IndexIterator()
//        {
//            final BlockIndexReader.IndexState state = new BlockIndexReader.IndexState();
//
//            @Override
//            public BlockIndexReader.IndexState next() throws IOException
//            {
//                BlockIndexReader.IndexState s1 = iterator2.next();
//                if (s1 != null)
//                {
//                    state.rowid = s1.rowid + 10_000;
//                    state.term = s1.term;
//                    return state;
//                }
//                else
//                {
//                    return null;
//                }
//            }
//
//            @Override
//            public BlockIndexReader.IndexState current()
//            {
//                return state;
//            }
//
//            @Override
//            public void close() throws IOException
//            {
//
//            }
//        };

//        MergeBlockReaders merged = new MergeBlockReaders(Lists.newArrayList(iterator, iterator3));
//        while (true)
//        {
//            BlockIndexReader.IndexState state = merged.next();
//            if (state == null) break;
//            System.out.println("  merged results term="+state.term.utf8ToString()+" rowid="+state.rowid);
//        }


//        List<Pair<BytesRef,Long>> results = new ArrayList<>();
//
//        while (true)
//        {
//            final BlockIndexReader.IndexState state = iterator.next();
//            if (state == null)
//            {
//                break;
//            }
//            results.add(Pair.create(BytesRef.deepCopyOf(state.term), state.rowid));
//            System.out.println("  results term="+state.term.utf8ToString()+" rowid="+state.rowid);
//        }
//
//        for (Pair<BytesRef,Long> pair : results)
//        {
//            System.out.println("term="+pair.left.utf8ToString()+" rowid="+pair.right);
//        }
//

//        List<PostingList.PeekablePostingList> lists = blockIndexReader.traverse(null,
//                                                                                true,
//                                                                                ByteComparable.fixedLength("cccc".getBytes(StandardCharsets.UTF_8)),
//                                                                                false);
//        PostingList postings = BlockIndexReader.toOnePostingList(lists);
//
//
//        while (true)
//        {
//            final long rowID = postings.nextPosting();
//            if (rowID == PostingList.END_OF_STREAM) break;
//            System.out.println("testSameTerms rowid=" + rowID);
//        }
    }

    @Test
    public void testPointer() throws Exception
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(10);
        Pointer pointer = com.sun.jna.Native.getDirectBufferPointer(buffer);

        Field field = Pointer.class.getDeclaredField("peer");
        field.setAccessible(true);

        Long pointerLong = field.getLong(pointer);

//        System.out.println("pointerLong="+pointerLong);
    }

    @Test
    public void randomTest() throws Exception
    {
        for (int x = 0; x < 2; x++)
        {
            doRandomTest();
        }
    }

    public void doRandomTest() throws Exception
    {
        List<Pair<ByteComparable, LongArrayList>> list = new ArrayList();
        int numValues = nextInt(500, 1000);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        int maxRowID = -1;
        int totalRows = 0;
        for (int x = 0; x < numValues; x++)
        {
            byte[] scratch = new byte[4];
            NumericUtils.intToSortableBytes(x, scratch, 0);

            int numRows = nextInt(1, 20);
            int rowID = nextInt(100);
            LongArrayList postings = new LongArrayList();
            for (int i = 0; i < numRows; i++)
            {
                maxRowID = Math.max(maxRowID, rowID);
                postings.add(rowID);
                totalRows++;
                buffer.addPackedValue(rowID, new BytesRef(scratch));
                rowID += nextInt(1, 100);
            }
//            System.out.println("term=" + x + " postings=" + postings);
            list.add(Pair.create(ByteComparable.fixedLength(scratch), postings));
        }

        int startIdx = nextInt(0, numValues - 2);
        ByteComparable start = list.get(startIdx).left;

        int queryMin = NumericUtils.sortableBytesToInt(ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41)), 0);

        int endIdx = nextInt(startIdx + 1, numValues - 1);
        ByteComparable end = list.get(endIdx).left;

        int queryMax = NumericUtils.sortableBytesToInt(ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41)), 0);

        String indexName = "index_yay";
        IndexContext indexContext = createIndexContext(indexName, UTF8Type.instance);

//        System.out.println("queryMin=" + queryMin + " queryMax=" + queryMax + " totalRows=" + totalRows + " maxRowID=" + maxRowID);

        IndexDescriptor indexDescriptor = newIndexDescriptor();

        final BKDReader bkdReader = finishAndOpenReaderOneDim(2,
                                                              buffer,
                                                              indexDescriptor,
                                                              maxRowID + 1,
                                                              totalRows,
                                                              indexName);

        final PostingList kdtreePostings = bkdReader.intersect(buildQuery(queryMin, queryMax), (QueryEventListener.BKDIndexEventListener) NO_OP_BKD_LISTENER, new SSTableQueryContext(new QueryContext()));

        IntArrayList kdtreePostingList = collect(kdtreePostings);
        kdtreePostings.close();
        bkdReader.close();

        IndexDescriptor indexDescriptor1 = newIndexDescriptor();
        BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor1, indexContext);
        BlockIndexWriter prefixBytesWriter = new BlockIndexWriter(fileProvider, false);

        TermsIterator terms = new MemtableTermsIterator(null,
                                                        null,
                                                        list.iterator());

        int pointCount = 0;
        while (terms.hasNext())
        {
            ByteComparable term = terms.next();
            PostingList postings = terms.postings();
            while (true)
            {
                long rowID = postings.nextPosting();
                if (rowID == PostingList.END_OF_STREAM) break;
                prefixBytesWriter.add(term, rowID);
                pointCount++;
            }
        }

        BlockIndexMeta meta = prefixBytesWriter.finish();

//        BlockIndexReader reader = new BlockIndexReader(comps,
//                                                       indexName,
//                                                       meta);
//
//        PostingList postings = reader.traverse(start, end);
//        IntArrayList results2 = collect(postings);
//
//        System.out.println("kdtreePostingList=" + kdtreePostingList);
//        System.out.println("results2=" + results2);
//
//        assertEquals(kdtreePostingList, results2);
    }

    //TODO Rig a generic method for this in TypeUtil or the like to
    // compose a type from a BytesRef
    private String stringFromTerm(BytesRef term)
    {
        ByteSource byteSource = ByteSource.fixedLength(term.bytes, 0, term.length);
        ByteBuffer byteBuffer = UTF8Type.instance.fromComparableBytes(ByteSource.peekable(byteSource), ByteComparable.Version.OSS41);
        return UTF8Type.instance.compose(byteBuffer);
    }

    private BKDReader.IntersectVisitor buildQuery(int queryMin, int queryMax)
    {
        return new BKDReader.IntersectVisitor()
        {
            @Override
            public boolean visit(byte[] packedValue)
            {
                int x = NumericUtils.sortableBytesToInt(packedValue, 0);
                boolean bb = x >= queryMin && x <= queryMax;
                return bb;
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
            {
                int min = NumericUtils.sortableBytesToInt(minPackedValue, 0);
                int max = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
                assert max >= min;

                if (max < queryMin || min > queryMax)
                {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                else if (min >= queryMin && max <= queryMax)
                {
                    return CELL_INSIDE_QUERY;
                }
                else
                {
                    return CELL_CROSSES_QUERY;
                }
            }
        };
    }

    private IntArrayList collect(PostingList postings) throws IOException
    {
        IntArrayList list = new IntArrayList();
        if (postings == null) return list;
        while (true)
        {
            long rowid = postings.nextPosting();
            if (rowid == PostingList.END_OF_STREAM) break;
            if (list.size() > 0)
            {
                if (list.get(list.size() - 1) == rowid)
                {
                    continue;
                }
            }
            list.add((int) rowid);
        }
        return list;
    }

    private BKDReader finishAndOpenReaderOneDim(int maxPointsPerLeaf,
                                                BKDTreeRamBuffer buffer,
                                                IndexDescriptor indexDescriptor,
                                                int maxRowID,
                                                int totalRows,
                                                String index
                                                ) throws IOException
    {
        IndexContext indexContext = createIndexContext(index, Int32Type.instance);

        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                 indexContext,
                                                                 Integer.BYTES,
                                                                 maxRowID,
                                                                 totalRows,
                                                                 new IndexWriterConfig("test", 2, 8),
                                                                 false);

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(buffer.asPointValues());
        final long bkdPosition = metadata.get(IndexComponent.KD_TREE).root;
        assertThat(bkdPosition, is(greaterThan(0L)));
        final long postingsPosition = metadata.get(IndexComponent.KD_TREE_POSTING_LISTS).root;
        assertThat(postingsPosition, is(greaterThan(0L)));

        FileHandle kdtreeHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE, indexContext);
        FileHandle kdtreePostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE_POSTING_LISTS, indexContext);
        return new BKDReader(indexContext,
                             kdtreeHandle,
                             bkdPosition,
                             kdtreePostingsHandle,
                             postingsPosition);

//        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
//                                                                 columnContext,
//                                                                 maxPointsPerLeaf,
//                                                                 Integer.BYTES,
//                                                                 maxRowID,
//                                                                 totalRows,
//                                                                 new IndexWriterConfig("test", 2, 8),
//                                                                 false);
//
//        IndexComponent kdtree = IndexComponent.create(IndexComponent.Type.KD_TREE, index);
//        IndexComponent kdtreePostings = IndexComponent.create(IndexComponent.Type.KD_TREE_POSTING_LISTS, index);
//
//        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(buffer.asPointValues());
//        final long bkdPosition = metadata.get(IndexComponent.Type.KD_TREE).root;
//        assertThat(bkdPosition, is(greaterThan(0L)));
//        final long postingsPosition = metadata.get(IndexComponent.Type.KD_TREE_POSTING_LISTS).root;
//        assertThat(postingsPosition, is(greaterThan(0L)));
//
//        FileHandle kdtreeHandle = indexDescriptor.createFileHandle(kdtree);
//        FileHandle kdtreePostingsHandle = indexDescriptor.createFileHandle(kdtreePostings);
//        return new BKDReader(columnContext,
//                             kdtreeHandle,
//                             bkdPosition,
//                             kdtreePostingsHandle,
//                             postingsPosition,
//                             PrimaryKeyMap.IDENTITY);

//        IndexContext columnContext = SAITester.createIndexContext(index, Int32Type.instance);
//        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
//                                                                 maxPointsPerLeaf,
//                                                                 Integer.BYTES,
//                                                                 maxRowID,
//                                                                 totalRows,
//                                                                 new IndexWriterConfig("test", 2, 8),
//                                                                 false);
//
//
//
//        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(buffer.asPointValues());
//        final long bkdPosition = metadata.get(IndexComponents.NDIType.KD_TREE).root;
//        assertThat(bkdPosition, is(greaterThan(0L)));
//        final long postingsPosition = metadata.get(IndexComponents.NDIType.KD_TREE_POSTING_LISTS).root;
//        assertThat(postingsPosition, is(greaterThan(0L)));
//
//        FileHandle kdtree = indexComponents.createFileHandle(indexComponents.kdTree);
//        FileHandle kdtreePostings = indexComponents.createFileHandle(indexComponents.kdTreePostingLists);
//        return new BKDReader(indexDescriptor,
//                             kdtree,
//                             bkdPosition,
//                             kdtreePostings,
//                             postingsPosition,
//                             PrimaryKeyMap.IDENTITY);
    }

    public static Pair<ByteComparable, LongArrayList> add(String term, long[] array)
    {
        LongArrayList list = new LongArrayList();
        list.add(array, 0, array.length);
        return Pair.create(v -> UTF8Type.instance.asComparableBytes(UTF8Type.instance.decompose(term), v), list);
    }
}
