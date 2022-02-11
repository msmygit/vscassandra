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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.Seed;
import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDTreeRamBuffer;
import org.apache.cassandra.index.sai.disk.v1.kdtree.NumericIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValueLong;

import static org.apache.cassandra.index.sai.disk.v1.kdtree.BKDReaderTest.buildQuery;
import static org.apache.cassandra.index.sai.metrics.QueryEventListeners.NO_OP_BKD_LISTENER;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class BlockTermsTest extends SaiRandomizedTest
{
    @Test
    public void testRandomBytes() throws Exception
    {
        for (int i = 0; i < 50; i++)
        {
            doTestRandomBytes();
        }
    }

    public void doTestRandomBytes() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        final BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        final int count = nextInt(1, 1000);

        int value = 0;

        IntArrayList expectedPointValues = new IntArrayList();

        long rowid = 0;
        for (int i = 0; i < count; i++)
        {
            int valueCount = nextInt(1, 50);
            for (int x = 0; x < valueCount; x++)
            {
                expectedPointValues.add(value);
                writer.add(toBytes(value), rowid++);
            }
            value++;
        }

        System.out.println("expectedPointValues="+expectedPointValues);
        System.out.println("numPoints="+expectedPointValues.size());

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            final BlockTerms.Reader.BytesCursor bytesCursor = reader.cursor();

            for (int pointid = 0; pointid < expectedPointValues.size(); pointid++)
            {
                BytesRef bytes = bytesCursor.seekToPointId(pointid);
                int expVal = expectedPointValues.getInt(pointid);
                int val = toInt(bytes);

                assertEquals(expVal, val);
            }
        }
    }

    @Test
    public void testBytesBlock() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        final BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        final int count = 35;

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
            final BlockTerms.Reader.BytesCursor bytesCursor = reader.cursor();
            for (int pointid = 0; pointid < count; pointid++)
            {
                BytesRef bytes = bytesCursor.seekToPointId(pointid);
                int value = toInt(bytes);
                assertEquals(pointid, value);
            }
        }
    }

    @Test
    public void testBytesBlockConsecutiveSameTerms() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        final BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        final int count = 25;

        for (int rowid = 0; rowid < count; rowid++)
        {
            writer.add(toBytes(10), rowid);
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            final BlockTerms.Reader.BytesCursor bytesCursor = reader.cursor();
            for (int pointid = 0; pointid < count; pointid++)
            {
                BytesRef bytes = bytesCursor.seekToPointId(pointid);
                int value = toInt(bytes);
                assertEquals(10, value);
            }
        }
    }

    @Test
    public void testPointsIteratorNoOrderMap() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        for (int rowid = 0; rowid < 20; rowid++)
        {
            writer.add(toBytes(10), rowid);
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            try (BlockTerms.Reader.PointsIterator pointsIterator = reader.pointsIterator(null, 0))
            {
                LongArrayList rowids = new LongArrayList();
                while (pointsIterator.next())
                {
                    long pointid = pointsIterator.pointId();
                    long rowid = pointsIterator.rowId();
                    BytesRef term = pointsIterator.term();
                    int value = toInt(term);

                    assertEquals(10, value);

                    rowids.add(rowid);

                    System.out.println("pointid=" + pointid + " rowid=" + rowid + " value=" + value);
                }
                assertArrayEquals(LongStream.rangeClosed(0, 19).toArray(), rowids.toLongArray());
            }
        }
    }

    @Test
    public void testPointsIteratorOrderMap() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        final BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        // first block has an order map
        // 2nc block has no order map
        final int[] terms = new int[] {10, 10, 10, 10, 10, 20, 20, 20, 20, 20,
                                         30, 31, 32, 33, 34, 35, 36, 37, 38, 39};
        final long[] rowids = new long[] {100, 101, 102, 103, 104, 10, 11, 12, 13, 14,
                                         30, 31, 32, 33, 34, 35, 36, 37, 38, 39};

        writer.add(toBytes(10), 100);
        writer.add(toBytes(10), 101);
        writer.add(toBytes(10), 102);
        writer.add(toBytes(10), 103);
        writer.add(toBytes(10), 104);
        writer.add(toBytes(20), 10);
        writer.add(toBytes(20), 11);
        writer.add(toBytes(20), 12);
        writer.add(toBytes(20), 13);
        writer.add(toBytes(20), 14);

        for (int rowid = 30; rowid < 40; rowid++)
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
            // TODO: add assert that there's an order map for the blocks
            try (BlockTerms.Reader.PointsIterator pointsIterator = reader.pointsIterator(null, 0))
            {
                long pointExp = 0;

                while (pointsIterator.next())
                {
                    long pointid = pointsIterator.pointId();

                    assertEquals(pointExp, pointid);

                    long rowid = pointsIterator.rowId();
                    BytesRef term = pointsIterator.term();
                    int value = toInt(term);

                    assertEquals(terms[(int)pointid], value);
                    assertEquals(rowids[(int)pointid], rowid);

                    pointExp++;

                    // System.out.println("pointid=" + pointid +" value=" + value+ " rowid=" + rowid);
                }
            }
        }
    }

    @Test
    public void testPointsIteratorNoOrderMap2() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        for (int rowid = 0; rowid < 20; rowid++)
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
            try (BlockTerms.Reader.PointsIterator pointsIterator = reader.pointsIterator(null, 0))
            {
                LongArrayList rowids = new LongArrayList();
                long rowidExp = 0;
                while (pointsIterator.next())
                {
                    long pointid = pointsIterator.pointId();
                    long rowid = pointsIterator.rowId();
                    BytesRef term = pointsIterator.term();
                    int value = toInt(term);

                    assertEquals(rowidExp, value);
                    rowidExp++;

                    rowids.add(rowid);

                    // System.out.println("pointid=" + pointid + " rowid=" + rowid + " value=" + value);
                }
                assertArrayEquals(LongStream.rangeClosed(0, 19).toArray(), rowids.toLongArray());
            }
        }
    }

    @Test
    public void testSegments() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        for (int rowid = 0; rowid < 20; rowid++)
        {
            writer.add(toBytes(10), rowid);
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            PostingList postings = reader.search(toBytes(10), toBytes(20));

            assertEquals(1, countPostingLists(postings));
            assertArrayEquals(LongStream.rangeClosed(0, 19).toArray(), collect(postings).toLongArray());
        }
    }

    @Test
    public void testRandom() throws Exception
    {
        for (int x = 0; x < 5; x++)
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

        List<Pair<ByteComparable, com.carrotsearch.hppc.IntArrayList>> list = new ArrayList();
        int numValues = nextInt(5, 200);
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

        for (int x = 0; x < 100; x++)
        {
            int startIdx = nextInt(0, numValues - 2);
            ByteComparable start = list.get(startIdx).left;
            int queryMin = toInt(start);

            int endIdx = nextInt(startIdx + 1, numValues - 1);
            ByteComparable end = list.get(endIdx).left;
            int queryMax = toInt(end);

            //System.out.println("queryMin=" + queryMin + " queryMax=" + queryMax + " totalRows=" + totalRows + " maxRowID=" + maxRowID);

            final PostingList kdtreePostings = bkdReader.intersect(buildQuery(queryMin, queryMax), (QueryEventListener.BKDIndexEventListener) NO_OP_BKD_LISTENER, new QueryContext());
            final LongArrayList kdtreePostingList = collect(kdtreePostings);

            LongArrayList postingList = null;
            try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor2, indexContext2, false);
                 BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor2,
                                                                  indexContext2,
                                                                  indexFiles,
                                                                  components))
            {
                PostingList postings = reader.search(toBytes(queryMin), toBytes(queryMax));
                postingList = collect(postings);
            }

//            System.out.println("postingList=" + postingList);
//            System.out.println("kdtreePostingList=" + kdtreePostingList);
//            System.out.println("postings equals=" + postingList.equals(kdtreePostingList));

            assertArrayEquals(kdtreePostingList.toLongArray(), postingList.toLongArray());
            kdtreePostings.close();
        }
        bkdReader.close();
    }

    private static BKDReader finishAndOpenReaderOneDim(int maxPointsPerLeaf,
                                                       BKDTreeRamBuffer buffer,
                                                       IndexDescriptor indexDescriptor,
                                                       IndexContext context,
                                                       int maxRowID,
                                                       int totalRows) throws IOException
    {
        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                 context,
                                                                 maxPointsPerLeaf,
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

        FileHandle kdtree = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE, context);
        FileHandle kdtreePostings = indexDescriptor.createPerIndexFileHandle(IndexComponent.KD_TREE_POSTING_LISTS, context);
        return new BKDReader(context,
                             kdtree,
                             bkdPosition,
                             kdtreePostings,
                             postingsPosition);
    }

    @Test
    public void testAddTermsOutOfOrderException() throws Exception
    {
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        writer.add(toBytes(20), 20);
        assertThrows(IllegalArgumentException.class, () -> writer.add(toBytes(19), 10));
    }

    @Test
    public void testCursorIndexOutOfBounds() throws Exception
    {
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        // 10 is the block value for 20 points
        for (int rowid = 0; rowid < 20; rowid++)
        {
            writer.add(toBytes(10), rowid);
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            BlockTerms.Reader.BytesCursor bytesCursor = reader.cursor();
            assertEquals(toBytes(10), bytesCursor.seekToPointId(15));
            assertEquals(toBytes(10), bytesCursor.seekToPointId(19));

            assertThrows(IndexOutOfBoundsException.class, () -> bytesCursor.seekToPointId(20));
        }
    }

    @Test
    public void testAddRowIDsOutOfOrderException() throws Exception
    {

    }

    /**
     * Tests same value 2 posting blocks and out of row id order values.
     */
    @Test
    public void testMultiSameBlockThreeValues() throws Exception
    {
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        // 10 is the block value for 20 points
        for (int rowid = 0; rowid < 20; rowid++)
        {
            writer.add(toBytes(10), rowid);
        }

        writer.add(toBytes(20), 100);
        writer.add(toBytes(20), 101);
        writer.add(toBytes(21), 50);
        writer.add(toBytes(21), 51);

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            PostingList postings = reader.search(toBytes(10), toBytes(11));
            assertEquals(1, countPostingLists(postings));
            assertArrayEquals(LongStream.rangeClosed(0, 19).toArray(), collect(postings).toLongArray());

            assertEquals(24, reader.meta.pointCount);
            assertEquals(3, reader.meta.numPostingBlocks);
            assertEquals(4, reader.meta.maxTermLength);
            assertEquals(10, reader.meta.postingsBlockSize);
            assertEquals(3, reader.meta.distinctTermCount);

            assertNull(reader.openOrderMap(0));
            assertNull(reader.openOrderMap(1));

            IntArrayList orderMap0 = reader.openOrderMap(2);
            assertEquals(2, orderMap0.getInt(0));
            assertEquals(3, orderMap0.getInt(1));
            assertEquals(0, orderMap0.getInt(2));
            assertEquals(1, orderMap0.getInt(3));
        }
    }

    /**
     * Tests multiple value multiple postings blocks
     */
    @Test
    public void testMultiSameBlockTwoValues() throws Exception
    {
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);
        final IndexDescriptor indexDescriptor = newIndexDescriptor();

        final BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        // 10 the block value for points
        for (int rowid = 0; rowid < 25; rowid++)
        {
            writer.add(toBytes(10), rowid);
        }

        for (int rowid = 25; rowid < 54; rowid++)
        {
            writer.add(toBytes(20), rowid);
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            PostingList postings3 = reader.search(toBytes(5), toBytes(47));
            assertEquals(3, countPostingLists(postings3));
            // System.out.println("postings3="+collect(postings3));
            assertArrayEquals(LongStream.rangeClosed(0, 53).toArray(), collect(postings3).toLongArray());

            LongArrayList sameValuePostings = collect(reader.sameValuePostings());
            // block 2 doesn't have same values
            assertArrayEquals(new long[] {0, 1, 3, 4, 5}, sameValuePostings.toLongArray());

            assertEquals(54, reader.meta.pointCount);
            assertEquals(6, reader.meta.numPostingBlocks);
            assertEquals(4, reader.meta.maxTermLength);
            assertEquals(10, reader.meta.postingsBlockSize);
            assertEquals(2, reader.meta.distinctTermCount);

            // verify raw bytes
            try (BlockTerms.Reader.BytesCursor bytesCursor = reader.cursor())
            {
                for (int x = 0; x < 25; x++)
                {
                    BytesRef term = bytesCursor.seekToPointId(x);
                    int value = toInt(term);
                    assertEquals(10, value);
                }
                for (int x = 25; x < 54; x++)
                {
                    BytesRef term = bytesCursor.seekToPointId(x);
                    int value = toInt(term);
                    assertEquals(20, value);
                }
            }

            // verify block postings
            long[][] blockRowids = new long[][]
                                   { LongStream.rangeClosed(0, 19).toArray(),
                                     LongStream.rangeClosed(0, 19).toArray(),
                                     LongStream.rangeClosed(20, 29).toArray(),
                                     LongStream.rangeClosed(30, 53).toArray(),
                                     LongStream.rangeClosed(30, 53).toArray(),
                                     LongStream.rangeClosed(30, 53).toArray() };

            for (int x = 0; x < reader.meta.numPostingBlocks; x++)
            {
                try (PostingList postings = reader.openBlockPostings(x))
                {
                    LongArrayList list = collect(postings);
                    assertArrayEquals(blockRowids[x], list.toLongArray());
                }
            }

            // verify reader min block term has 1 value
            try (BlockTerms.Reader.MinTermsIterator minTermsIterator = reader.minTermsIterator())
            {
                if (minTermsIterator.hasNext())
                {
                    Pair<ByteSource, Long> pair = minTermsIterator.next();
                    int minValue = toInt((version -> pair.left));

                    assertEquals(10, minValue);

                    int minBlock = (int) (pair.right.longValue() >> 32);
                    int maxBlock = (int) pair.right.longValue();

                    assertEquals(0, minBlock);
                    assertEquals(2, maxBlock);
                }
                else
                {
                    assertTrue(false);
                }

                if (minTermsIterator.hasNext())
                {
                    Pair<ByteSource, Long> pair = minTermsIterator.next();
                    int minValue = toInt((version -> pair.left));

                    assertEquals(20, minValue);

                    int minBlock = (int) (pair.right.longValue() >> 32);
                    int maxBlock = (int) pair.right.longValue();

                    maxBlock = maxBlock < 0 ? maxBlock * -1 : maxBlock;

                    assertEquals(3, minBlock);
                    assertEquals(5, maxBlock);
                }
                else
                {
                    assertTrue(false);
                }
            }
        }
    }

    /**
     * All values the same tests multi-block postings
     */
    @Test
    public void testMultiSameBlockOneValue() throws Exception
    {
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        BlockTerms.Writer writer = new BlockTerms.Writer(10, indexDescriptor, indexContext, false);

        // all points have value 10
        for (int rowid = 0; rowid < 45; rowid++)
        {
            writer.add(toBytes(10), rowid);
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            LongArrayList sameValuePostings = collect(reader.sameValuePostings());
            assertArrayEquals(new long[] {0, 1, 2, 3, 4}, sameValuePostings.toLongArray());

            PostingList postings3 = reader.search(toBytes(5), toBytes(47));
            assertEquals(1, countPostingLists(postings3));
            assertArrayEquals(LongStream.rangeClosed(0, 44).toArray(), collect(postings3).toLongArray());

            assertEquals(45, reader.meta.pointCount);
            assertEquals(5, reader.meta.numPostingBlocks);
            assertEquals(4, reader.meta.maxTermLength);
            assertEquals(10, reader.meta.postingsBlockSize);
            assertEquals(1, reader.meta.distinctTermCount);

            // verify raw bytes are all the same value
            try (BlockTerms.Reader.BytesCursor bytesCursor = reader.cursor())
            {
                for (int x = 0; x < 45; x++)
                {
                    BytesRef term = bytesCursor.seekToPointId(x);
                    int value = toInt(term);
                    assertEquals(10, value);
                }
            }

            // verify reader min block term has 1 value
            try (BlockTerms.Reader.MinTermsIterator minTermsIterator = reader.minTermsIterator())
            {
                int count = 0;
                while (minTermsIterator.hasNext())
                {
                    Pair<ByteSource, Long> pair = minTermsIterator.next();
                    int minValue = toInt((version -> pair.left));

                    assertEquals(10, minValue);

                    int minBlock = (int) (pair.right.longValue() >> 32);
                    int maxBlock = (int) pair.right.longValue();

                    if (maxBlock < 0)
                        maxBlock *= -1;

                    assertEquals(0, minBlock);
                    assertEquals(4, maxBlock);

                    count++;
                }
                assertEquals(1, count);
            }

            // verify postings
            long[] rowidMatches = LongStream.rangeClosed(0, 44).toArray();
            for (int x = 0; x < reader.meta.numPostingBlocks; x++)
            {
                try (PostingList postings = reader.openBlockPostings(x))
                {
                    LongArrayList list = collect(postings);
                    assertArrayEquals(rowidMatches, list.toLongArray());
                }
            }
        }
    }

    /**
     * 45 unique integer values tests single block postings
     */
    @Test
    public void testUniques() throws Exception
    {
        int blockSize = 10;

        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        BlockTerms.Writer writer = new BlockTerms.Writer(blockSize, indexDescriptor, indexContext, false);

        for (int x = 0; x < 45; x++)
        {
            int val = x;
            BytesRef term = toBytes(val);
            writer.add(term, x);
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            assertNull(reader.sameValuePostings());

            PostingList postings3 = reader.search(toBytes(11), toBytes(47));
            //System.out.println("postings3="+collect(postings3));
            assertArrayEquals(LongStream.rangeClosed(11, 44).toArray(), collect(postings3).toLongArray());

            PostingList postings2 = reader.search(toBytes(5), toBytes(15));
            assertArrayEquals(LongStream.rangeClosed(5, 15).toArray(), collect(postings2).toLongArray());

            PostingList postings1 = reader.search(toBytes(0), toBytes(5));
            assertArrayEquals(new long[] {0, 1, 2, 3, 4, 5}, collect(postings1).toLongArray());

            // test start block filtering
            PostingList filterPostings1 = reader.filterBlock(toBytes(5), toBytes(44), 0, new MutableValueLong());
            LongArrayList filterPostings1List = collect(filterPostings1);
            assertArrayEquals(new long[]{ 5, 6, 7, 8, 9 }, filterPostings1List.toLongArray());

            // test end block filtering
            filterPostings1 = reader.filterBlock(toBytes(0), toBytes(42), reader.meta.numPostingBlocks - 1, new MutableValueLong());
            filterPostings1List = collect(filterPostings1);
            assertArrayEquals(new long[]{ 40, 41, 42 }, filterPostings1List.toLongArray());

            // verify reader meta data
            assertEquals(45, reader.meta.pointCount);
            assertEquals(5, reader.meta.numPostingBlocks);
            assertEquals(4, reader.meta.maxTermLength);
            assertEquals(10, reader.meta.postingsBlockSize);
            assertEquals(45, reader.meta.distinctTermCount);

            // Verify reader min block terms
            IntArrayList blockMinValues = getBlockMinValues(reader);
            assertArrayEquals(new int[]{ 0, 10, 20, 30, 40 }, blockMinValues.toIntArray());

            // TODO: should searchTermsIndex return N-1 min block id
            // search the trie for min and max block ids
            Pair<Integer, Integer> rangePair = reader.searchTermsIndex(toBytes(5), toBytes(30));
            assertEquals(1, rangePair.left.intValue());
            assertEquals(3, rangePair.right.intValue());

            // use the bytes min max filtering API
            Pair<Long, Long> filterPair = reader.filterPoints(toBytes(5), null, 0, blockSize - 1);
            assertEquals(Pair.create(5l, 9l), filterPair);

            // verify block postings
            final long[][] blockRowids = new long[][]
                                   { new long[]{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 },
                                     new long[]{ 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 },
                                     new long[]{ 20, 21, 22, 23, 24, 25, 26, 27, 28, 29 },
                                     new long[]{ 30, 31, 32, 33, 34, 35, 36, 37, 38, 39 },
                                     new long[]{ 40, 41, 42, 43, 44 } };
            for (int x = 0; x < reader.meta.numPostingBlocks; x++)
            {
                try (PostingList postings = reader.openBlockPostings(x))
                {
                    LongArrayList list = collect(postings);
                    assertArrayEquals(list.toLongArray(), blockRowids[x]);
                }
            }

            // open a cursor and iterate on each term by point id
            try (BlockTerms.Reader.BytesCursor bytesCursor = reader.cursor())
            {
                for (int x = 0; x < 45; x++)
                {
                    BytesRef term = bytesCursor.seekToPointId(x);

                    ByteSource.Peekable peekable = ByteComparable.fixedLength(term.bytes, term.offset, term.length).asPeekableBytes(ByteComparable.Version.OSS41);
                    ByteBuffer buffer2 = Int32Type.instance.fromComparableBytes(peekable, ByteComparable.Version.OSS41);
                    int value = Int32Type.instance.compose(buffer2);
                    assertEquals(x, value);
                }
            }
        }
    }

    public IntArrayList getBlockMinValues(BlockTerms.Reader reader) throws Exception
    {
        IntArrayList blockMinValues = new IntArrayList();
        try (BlockTerms.Reader.MinTermsIterator minTermsIterator = reader.minTermsIterator())
        {
            while (minTermsIterator.hasNext())
            {
                Pair<ByteSource, Long> pair = minTermsIterator.next();
                int minValue = toInt((version -> pair.left));
                blockMinValues.add(minValue);
            }
        }
        return blockMinValues;
    }

    public static LongArrayList collect(PostingList postings) throws IOException
    {
        if (postings == null) return null;
        LongArrayList list = new LongArrayList();
        while (true)
        {
            long rowid = postings.nextPosting();
            if (rowid == PostingList.END_OF_STREAM) break;
            list.add(rowid);
        }
        postings.close();
        return list;
    }

    public static int countPostingLists(PostingList list)
    {
        if (list instanceof MergePostingList)
        {
            MergePostingList merge = (MergePostingList)list;
            return merge.postingListCount();
        }
        else
        {
            return 1;
        }
    }

    public static int toInt(BytesRef term)
    {
        ByteSource.Peekable peekable = ByteComparable.fixedLength(term.bytes, term.offset, term.length).asPeekableBytes(ByteComparable.Version.OSS41);
        ByteBuffer buffer2 = Int32Type.instance.fromComparableBytes(peekable, ByteComparable.Version.OSS41);
        return Int32Type.instance.compose(buffer2);
    }

    public static int toInt(ByteComparable term)
    {
        ByteBuffer buffer = Int32Type.instance.fromComparableBytes(term.asPeekableBytes(ByteComparable.Version.OSS41), ByteComparable.Version.OSS41);
        return Int32Type.instance.compose(buffer);
    }

    public static BytesRef toBytes(int val)
    {
        ByteBuffer buffer = Int32Type.instance.decompose(val);
        ByteSource source = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
        byte[] bytes = ByteSourceInverse.readBytes(source);
        return new BytesRef(bytes);
    }
}
