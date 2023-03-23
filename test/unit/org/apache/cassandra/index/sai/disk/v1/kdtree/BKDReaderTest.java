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
package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.segment.SegmentMetadata;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.postings.PostingList;
import org.apache.cassandra.index.sai.utils.SAIRandomizedTester;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.lucene.index.PointValues.Relation.CELL_CROSSES_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;
import static org.apache.lucene.index.PointValues.Relation.CELL_OUTSIDE_QUERY;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BKDReaderTest extends SAIRandomizedTester
{
    private final BKDReader.IntersectVisitor NONE_MATCH = new BKDReader.IntersectVisitor()
    {
        @Override
        public boolean visit(byte[] packedValue)
        {
            return false;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_OUTSIDE_QUERY;
        }
    };

    private final BKDReader.IntersectVisitor ALL_MATCH = new BKDReader.IntersectVisitor()
    {
        @Override
        public boolean visit(byte[] packedValue)
        {
            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_INSIDE_QUERY;
        }
    };

    private final BKDReader.IntersectVisitor ALL_MATCH_WITH_FILTERING = new BKDReader.IntersectVisitor()
    {
        @Override
        public boolean visit(byte[] packedValue)
        {
            return true;
        }

        @Override
        public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
        {
            return CELL_CROSSES_QUERY;
        }
    };

    private IndexDescriptor indexDescriptor;
    private String index;
    private IndexContext indexContext;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        index = newIndex();
        indexContext = SAITester.createIndexContext(index, Int32Type.instance);
    }

    @Test
    public void testAdvance() throws IOException
    {
        doTestAdvance(false);
    }

    @Test
    public void testAdvanceCrypto() throws IOException
    {
        doTestAdvance(true);
    }

    private void doTestAdvance(boolean crypto) throws IOException
    {
        final int numRows = between(1000, 2000);
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(Integer.BYTES);

        byte[] scratch = new byte[4];
        for (int docID = 0; docID < numRows; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        final BKDReader reader = finishAndOpenReaderOneDim(2, buffer);

        PostingList intersection = performIntersection(reader, NONE_MATCH);
        assertNull(intersection);

        intersection = performIntersection(reader, ALL_MATCH);
        assertEquals(numRows, intersection.size());
        assertEquals(100, intersection.advance(100));
        assertEquals(200, intersection.advance(200));
        assertEquals(300, intersection.advance(300));
        assertEquals(400, intersection.advance(400));
        assertEquals(401, intersection.advance(401));
        long expectedRowID = 402;
        for (long id = intersection.nextPosting(); expectedRowID < 500; id = intersection.nextPosting())
        {
            assertEquals(expectedRowID++, id);
        }
        assertEquals(PostingList.END_OF_STREAM, intersection.advance(numRows + 1));

        intersection.close();
    }

    @Test
    public void testResourcesReleaseWhenQueryDoesntMatchAnything() throws Exception
    {
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(Integer.BYTES);
        byte[] scratch = new byte[4];
        for (int docID = 0; docID < 1000; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }
        // add a gap between 1000 and 1100
        for (int docID = 1000; docID < 2000; docID++)
        {
            NumericUtils.intToSortableBytes(docID + 100, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }

        final BKDReader reader = finishAndOpenReaderOneDim(50, buffer);

        final PostingList intersection = performIntersection(reader, buildQuery(1017, 1096));
        assertNull(intersection);
    }

    private PostingList performIntersection(BKDReader reader, BKDReader.IntersectVisitor visitor)
    {
        QueryEventListener.BKDIndexEventListener bkdIndexEventListener = mock(QueryEventListener.BKDIndexEventListener.class);
        when(bkdIndexEventListener.postingListEventListener()).thenReturn(mock(QueryEventListener.PostingListEventListener.class));
        return reader.intersect(visitor, bkdIndexEventListener, mock(QueryContext.class));
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
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue)
            {
                int min = NumericUtils.sortableBytesToInt(minPackedValue, 0);
                int max = NumericUtils.sortableBytesToInt(maxPackedValue, 0);
                assert max >= min;

                if (max < queryMin || min > queryMax)
                {
                    return Relation.CELL_OUTSIDE_QUERY;
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

    private BKDReader finishAndOpenReaderOneDim(int maxPointsPerLeaf, BKDTreeRamBuffer buffer) throws IOException
    {
        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                 indexContext,
                                                                 maxPointsPerLeaf,
                                                                 Integer.BYTES,
                                                                 Math.toIntExact(buffer.numRows()),
                                                                 new IndexWriterConfig("test", 2, 8));

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
    }

    private BKDReader finishAndOpenReaderOneDim(int maxPointsPerLeaf, MutableOneDimPointValues values, int numRows) throws IOException
    {
        final NumericIndexWriter writer = new NumericIndexWriter(indexDescriptor,
                                                                 indexContext,
                                                                 maxPointsPerLeaf,
                                                                 Integer.BYTES,
                                                                 Math.toIntExact(numRows),
                                                                 new IndexWriterConfig("test", 2, 8));

        final SegmentMetadata.ComponentMetadataMap metadata = writer.writeAll(values);
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
    }
}
