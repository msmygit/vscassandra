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

package org.apache.cassandra.index.sai.disk.v3;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import com.google.common.primitives.Longs;
import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDTreeRamBuffer;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.BytesRef;

import static org.apache.cassandra.index.sai.disk.v1.kdtree.BKDReaderTest.buildQuery;
import static org.apache.cassandra.index.sai.disk.v3.BlockTermsTest.collect;
import static org.apache.cassandra.index.sai.disk.v3.BlockTermsTest.finishAndOpenReaderOneDim;
import static org.apache.cassandra.index.sai.disk.v3.BlockTermsTest.toBytes;
import static org.apache.cassandra.index.sai.disk.v3.BlockTermsTest.toInt;
import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_BKD_LISTENER;

public class BlockTerms2Test extends SaiRandomizedTest
{
    @Test
    @Seed("88E15363629EA9C0")
    public void testRandom() throws Exception
    {
        for (int x = 0; x < 1; x++)
        {
            doRandomTest();
        }
    }

    private void doRandomTest() throws Exception
    {
        final IndexDescriptor indexDescriptor2 = newIndexDescriptor();
        final String index2 = newIndex();
        final IndexContext indexContext2 = SAITester.createIndexContext(index2, Int32Type.instance);
        final BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor2, indexContext2, false);

        List<Pair<ByteComparable, IntArrayList>> list = new ArrayList();
        int numValues = nextInt(30, 40);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(1, Integer.BYTES);

        int maxRowID = -1;
        int totalRows = 0;
        for (int x = 0; x < numValues; x++)
        {
            byte[] scratch = toBytes(x).bytes;

            int numRows = nextInt(1, 50);
            int rowID = nextInt(100);
            com.carrotsearch.hppc.IntArrayList postings = new com.carrotsearch.hppc.IntArrayList();
            for (int i = 0; i < numRows; i++)
            {
                maxRowID = Math.max(maxRowID, rowID);
                postings.add(rowID);
                totalRows++;
                buffer.addPackedValue(rowID, new BytesRef(scratch));

                writer.add(new BytesRef(scratch), rowID);

                rowID += nextInt(1, 100);
            }
            //System.out.println("term=" + x + " postings("+postings.size()+")=" + postings);
            list.add(Pair.create(ByteComparable.fixedLength(scratch), postings));
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        final BKDReader bkdReader = finishAndOpenReaderOneDim(10,
                                                              buffer,
                                                              indexDescriptor,
                                                              indexContext,
                                                              maxRowID + 1,
                                                              totalRows);

        int queryIterations = 0;
        for (int x = 0; x < 100; x++)
        {
            int startIdx = nextInt(0, numValues - 2);
            ByteComparable start = list.get(startIdx).left;
            int queryMin = toInt(start);

            int endIdx = nextInt(startIdx + 1, numValues - 1);
            ByteComparable end = list.get(endIdx).left;
            int queryMax = toInt(end);

            // System.out.println("!!!! queryMin=" + queryMin + " queryMax=" + queryMax + " totalRows=" + totalRows + " maxRowID=" + maxRowID);

            final PostingList kdtreePostings = bkdReader.intersect(buildQuery(queryMin, queryMax), (QueryEventListener.BKDIndexEventListener) NO_OP_BKD_LISTENER, new QueryContext());
            final LongArrayList kdtreePostingList = collect(kdtreePostings);

            LongArrayList postingList = null;
            try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor2, indexContext2, false);
                 BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor2,
                                                                  indexContext2,
                                                                  indexFiles,
                                                                  components))
            {
                PostingList postings = reader.search(toBytes(queryMin), toBytes(queryMax), (QueryEventListener.BKDIndexEventListener) NO_OP_BKD_LISTENER, new QueryContext());
//                PostingList postings = reader.searchLeaves(toBytes(queryMin), toBytes(queryMax));
                postingList = collect(postings);
            }

            queryIterations++;

//            System.out.println("postingLis1=" + kdtreePostingList);
//            System.out.println("postingLis2=" + postingList);
//            System.out.println("postings equals=" + postingList.equals(kdtreePostingList));

            assertArrayEquals("queryIterations="+queryIterations, kdtreePostingList.toLongArray(), postingList.toLongArray());
            kdtreePostings.close();
        }
        bkdReader.close();
    }

    @Test
    public void testSameAndUniques() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        final BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        int rowid = 0;

        for (int x = 0; x < 30; x++)
        {
            writer.add(toBytes(rowid), rowid++);
        }

        for (int x = 0; x < 44; x++)
        {
            // rowid starts at 31
            System.out.println("30 rowid = "+rowid);
            writer.add(toBytes(30), rowid++);
        }

        for (int x = 0; x < 10; x++)
        {
            System.out.println("post 30 rowid = "+rowid);
            writer.add(toBytes(rowid), rowid++);
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            PostingList postings = reader.search(toBytes(15), toBytes(30), (QueryEventListener.BKDIndexEventListener) NO_OP_BKD_LISTENER, new QueryContext());
            LongArrayList list = collect(postings);


            long[] expectedRowids = LongStream.rangeClosed(15, 73).toArray();
            assertArrayEquals(expectedRowids, list.toLongArray());

            // # 2
            postings = reader.search(toBytes(15), toBytes(80), (QueryEventListener.BKDIndexEventListener) NO_OP_BKD_LISTENER, new QueryContext());
            list = collect(postings);

            expectedRowids = LongStream.rangeClosed(15, 73).toArray();
            long[] combinedExpected = Longs.concat(expectedRowids, LongStream.rangeClosed(74, 80).toArray());

            assertArrayEquals(combinedExpected, list.toLongArray());
        }
    }

    @Test
    public void testUniques() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        final BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        final int count = 100;

        for (int rowid = 0; rowid < count; rowid++)
        {
            writer.add(toBytes(rowid), rowid);
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            PostingList postings = reader.search(toBytes(15), toBytes(25), (QueryEventListener.BKDIndexEventListener) NO_OP_BKD_LISTENER, new QueryContext());
            LongArrayList list = collect(postings);

            System.out.println("list="+list);

            long[] expectedRowids = LongStream.rangeClosed(15, 25).toArray();

            assertArrayEquals(expectedRowids, list.toLongArray());
        }
    }

    @Test
    public void testSimpleSameTerms() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        final BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        int rowid = 0;

        for (int x = 0; x < 25; x++)
        {
            writer.add(toBytes(10), rowid++);
        }

        for (int x = 0; x < 40; x++)
        {
            writer.add(toBytes(20), rowid++);
        }

        for (int x = 0; x < 5; x++)
        {
            writer.add(toBytes(30), rowid++);
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
//            PostingList postings = reader.search(toBytes(9), toBytes(11));
//
//            LongArrayList list = collect(postings);
//
//            long[] rowidMatches = LongStream.rangeClosed(0, 24).toArray();
//
//            System.out.println("list="+list);
//
//            assertArrayEquals(rowidMatches, list.toLongArray());

            PostingList postings = reader.search(toBytes(19), toBytes(30), (QueryEventListener.BKDIndexEventListener) NO_OP_BKD_LISTENER, new QueryContext());

            LongArrayList list = collect(postings);

            long[] rowidMatches = LongStream.rangeClosed(25, 69).toArray();

            System.out.println("list2="+list);

            assertArrayEquals(rowidMatches, list.toLongArray());
        }
    }
}
