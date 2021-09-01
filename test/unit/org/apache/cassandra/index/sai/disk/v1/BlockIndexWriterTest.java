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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.sun.scenario.effect.Merge;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDTreeRamBuffer;
import org.apache.cassandra.index.sai.disk.v1.kdtree.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_BKD_LISTENER;
import static org.apache.lucene.index.PointValues.Relation.CELL_CROSSES_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

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
        System.out.println("contains="+contains);

        boolean encloses = set.encloses(Range.open(0, 10));
        System.out.println("encloses="+encloses);

        boolean encloses2 = set.encloses(Range.open(4, 15));

        System.out.println("encloses2="+encloses2);
    }

    @Test
    public void testSameTerms() throws Exception
    {
        List<Pair<ByteComparable, LongArrayList>> list = new ArrayList();
        list.add(add("aaa", new long[]{ 100 })); // 0
        list.add(add("aaabbb", new long[]{ 0, 1 })); // 0
        list.add(add("aaabbb", new long[]{ 2, 3, 4 })); // 1
        list.add(add("cccc", new long[]{ 10, 11 })); // 2

        list.add(add("cccc", new long[]{ 15 })); // 3
        list.add(add("gggaaaddd", new long[]{ 200, 201, 203 })); // 3, 4
        list.add(add("gggzzzz", new long[]{ 500, 501, 502, 503, 504, 505 })); // 5, 6
        list.add(add("qqqqqaaaaa", new long[]{ 400, 405, 409 })); // 3, 4
        list.add(add("zzzzz", new long[]{ 700, 780, 782, 790, 794, 799 })); // 6, 7
        list.add(add("zzzzzzzzzz", new long[] {20, 21, 24, 29, 30})); // 8, 9

        IndexDescriptor indexDescriptor = newIndexDescriptor();
        String indexName = "index_test";

        BlockIndexWriter blockIndexWriter = new BlockIndexWriter(indexName, indexDescriptor);

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
                blockIndexWriter.add(term, rowID);
                pointCount++;
            }
        }

        BlockIndexWriter.BlockIndexMeta meta = blockIndexWriter.finish();

        BlockIndexReader blockIndexReader = new BlockIndexReader(indexDescriptor, indexName, meta);

        PostingList postings = blockIndexReader.traverse(null,
                                               true,
                                               ByteComparable.fixedLength("cccc".getBytes(StandardCharsets.UTF_8)),
                                               false);

        while (true)
        {
            final long rowID = postings.nextPosting();
            if (rowID == PostingList.END_OF_STREAM) break;
            System.out.println("testSameTerms rowid=" + rowID);
        }
    }

    @Test
    public void randomTest() throws Exception
    {
        for (int x = 0; x < 1; x++)
        {
            doRandomTest();
        }
    }

    public void doRandomTest() throws Exception
    {
        List<Pair<ByteComparable, LongArrayList>> list = new ArrayList();
        int numValues = nextInt(10000, 12000);
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
            System.out.println("term=" + x + " postings=" + postings);
            list.add(Pair.create(ByteComparable.fixedLength(scratch), postings));
        }

        int startIdx = nextInt(0, numValues - 2);
        ByteComparable start = list.get(startIdx).left;

        int queryMin = NumericUtils.sortableBytesToInt(ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41)), 0);

        int endIdx = nextInt(startIdx + 1, numValues - 1);
        ByteComparable end = list.get(endIdx).left;

        int queryMax = NumericUtils.sortableBytesToInt(ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41)), 0);

        String indexName = "index_yay";

        System.out.println("queryMin=" + queryMin + " queryMax=" + queryMax + " totalRows=" + totalRows + " maxRowID=" + maxRowID);

        IndexDescriptor indexDescriptor = newIndexDescriptor();

        final BKDReader bkdReader = finishAndOpenReaderOneDim(2,
                                                              buffer,
                                                              indexDescriptor,
                                                              maxRowID + 1,
                                                              totalRows,
                                                              indexName);

        final List<PostingList.PeekablePostingList> kdtreePostingsList = bkdReader.intersect(buildQuery(queryMin, queryMax), (QueryEventListener.BKDIndexEventListener) NO_OP_BKD_LISTENER, new QueryContext());
        final PriorityQueue<PostingList.PeekablePostingList> kdtreePostingsQueue = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));
        kdtreePostingsQueue.addAll(kdtreePostingsList);
        PostingList kdtreePostings = MergePostingList.merge(kdtreePostingsQueue);

        IntArrayList kdtreePostingList = collect(kdtreePostings);
        kdtreePostings.close();
        bkdReader.close();

        IndexDescriptor comps = newIndexDescriptor();
        BlockIndexWriter prefixBytesWriter = new BlockIndexWriter(indexName, comps);

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

        BlockIndexWriter.BlockIndexMeta meta = prefixBytesWriter.finish();

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

    private BKDReader.IntersectVisitor buildQuery(int queryMin, int queryMax)
    {
        return new BKDReader.IntersectVisitor()
        {
            @Override
            public boolean visit(byte[] packedValue)
            {
                int x = NumericUtils.sortableBytesToInt(packedValue, 0);
                boolean bb = x >= queryMin && x <= queryMax;
                if (bb) System.out.println("visit value="+x+" bb="+bb);
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
        IndexContext columnContext = SAITester.createIndexContext(index, Int32Type.instance);

        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                 columnContext,
                                                                 maxPointsPerLeaf,
                                                                 Integer.BYTES,
                                                                 maxRowID,
                                                                 totalRows,
                                                                 new IndexWriterConfig("test", 2, 8),
                                                                 false);

        IndexComponent kdtree = IndexComponent.create(IndexComponent.Type.KD_TREE, index);
        IndexComponent kdtreePostings = IndexComponent.create(IndexComponent.Type.KD_TREE_POSTING_LISTS, index);

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(buffer.asPointValues());
        final long bkdPosition = metadata.get(IndexComponent.Type.KD_TREE).root;
        assertThat(bkdPosition, is(greaterThan(0L)));
        final long postingsPosition = metadata.get(IndexComponent.Type.KD_TREE_POSTING_LISTS).root;
        assertThat(postingsPosition, is(greaterThan(0L)));

        FileHandle kdtreeHandle = indexDescriptor.createFileHandle(kdtree);
        FileHandle kdtreePostingsHandle = indexDescriptor.createFileHandle(kdtreePostings);
        return new BKDReader(columnContext,
                             kdtreeHandle,
                             bkdPosition,
                             kdtreePostingsHandle,
                             postingsPosition,
                             PrimaryKeyMap.IDENTITY);

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
        return Pair.create(ByteComparable.fixedLength(UTF8Type.instance.decompose(term)), list);
    }
}
