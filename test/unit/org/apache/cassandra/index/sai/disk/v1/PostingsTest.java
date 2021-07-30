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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.lucene.store.IndexInput;

public class PostingsTest extends NdiRandomizedTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private IndexDescriptor indexDescriptor;
    private String index;
    private IndexComponent postingLists;

    @Before
    public void setup() throws Throwable
    {
        indexDescriptor = newIndexDescriptor();
        index = newIndex();
        postingLists = IndexComponent.create(IndexComponent.Type.POSTING_LISTS, index);
    }

    @Test
    public void testSingleBlockPostingList() throws Exception
    {
        final int blockSize = 1 << between(3, 8);
        final ArrayPostingList expectedPostingList = new ArrayPostingList(new int[]{ 10, 20, 30, 40, 50, 60 });

        long postingPointer;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, index, blockSize, false))
        {
            postingPointer = writer.write(expectedPostingList);
            writer.complete();
        }

//<<<<<<< HEAD
        SharedIndexInput sharedInput = new SharedIndexInput(indexDescriptor.openInput(postingLists));
        SAICodecUtils.validate(sharedInput);
        sharedInput.seek(postingPointer);
//=======
//        IndexInput input = indexDescriptor.openInput(postingLists);
//        SAICodecUtils.validate(input);
//        input.seek(postingPointer);
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)

        final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expectedPostingList, sharedInput);
        assertEquals(1, summary.offsets.length());

        CountingPostingListEventListener listener = new CountingPostingListEventListener();
        try (PostingsReader reader = new PostingsReader(sharedInput, postingPointer, listener))
        {
            expectedPostingList.reset();
            assertEquals(expectedPostingList.getOrdinal(), reader.getOrdinal());
            assertEquals(expectedPostingList.size(), reader.size());

            long actualRowID;
            while ((actualRowID = reader.nextPosting()) != PostingList.END_OF_STREAM)
            {
                assertEquals(expectedPostingList.nextPosting(), actualRowID);
                assertEquals(expectedPostingList.getOrdinal(), reader.getOrdinal());
            }
            assertEquals(PostingList.END_OF_STREAM, expectedPostingList.nextPosting());
            assertEquals(0, listener.advances);
            reader.close();
            assertEquals(expectedPostingList.size(), listener.decodes);
        }

//<<<<<<< HEAD
        sharedInput = new SharedIndexInput(indexDescriptor.openInput(postingLists));
//=======
//        input = indexDescriptor.openInput(postingLists);
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)
        listener = new CountingPostingListEventListener();
        try (PostingsReader reader = new PostingsReader(sharedInput, postingPointer, listener))
        {
            assertEquals(50, reader.advance(PrimaryKeyMap.IDENTITY.primaryKeyFromRowId(45)));
            assertEquals(60, reader.advance(PrimaryKeyMap.IDENTITY.primaryKeyFromRowId(60)));
            assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
            assertEquals(2, listener.advances);
            reader.close();
            assertEquals(reader.size(), listener.decodes); // nothing more was decoded
        }
    }

    @Test
    public void testMultiBlockPostingList() throws Exception
    {
        final int numPostingLists = 1 << between(1, 5);
        final int blockSize = 1 << between(5, 10);
        final int numPostings = between(1 << 11, 1 << 15);
        final ArrayPostingList[] expected = new ArrayPostingList[numPostingLists];
        final long[] postingPointers = new long[numPostingLists];

        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, index, blockSize, false))
        {
            for (int i = 0; i < numPostingLists; ++i)
            {
                final int[] postings = randomPostings(numPostings);
                final ArrayPostingList postingList = new ArrayPostingList(postings);
                expected[i] = postingList;
                postingPointers[i] = writer.write(postingList);
            }
            writer.complete();
        }

        try (IndexInput input = indexDescriptor.openInput(postingLists))
        {
            SAICodecUtils.validate(input);
        }

        for (int i = 0; i < numPostingLists; ++i)
        {
//<<<<<<< HEAD
            SharedIndexInput sharedInput = new SharedIndexInput(indexDescriptor.openInput(postingLists));
            sharedInput.seek(postingPointers[i]);
//=======
//            IndexInput input = indexDescriptor.openInput(postingLists);
//            input.seek(postingPointers[i]);
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)
            final ArrayPostingList expectedPostingList = expected[i];
            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expectedPostingList, sharedInput);
            assertTrue(summary.offsets.length() > 1);

            final CountingPostingListEventListener listener = new CountingPostingListEventListener();
            try (PostingsReader reader = new PostingsReader(sharedInput, postingPointers[i], listener))
            {
                expectedPostingList.reset();
                assertEquals(expectedPostingList.getOrdinal(), reader.getOrdinal());
                assertEquals(expectedPostingList.size(), reader.size());

                assertPostingListEquals(expectedPostingList, reader);
                assertEquals(0, listener.advances);
            }

            // test skipping to the last block
//<<<<<<< HEAD
            sharedInput = new SharedIndexInput(indexDescriptor.openInput(postingLists));
            try (PostingsReader reader = new PostingsReader(sharedInput, postingPointers[i], listener))
//=======
//            input = indexDescriptor.openInput(postingLists);
//            try (PostingsReader reader = new PostingsReader(input, postingPointers[i], listener))
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)
            {
                long tokenToAdvance = -1;
                expectedPostingList.reset();
                for (int p = 0; p < numPostings - 7; ++p)
                {
                    tokenToAdvance = expectedPostingList.nextPosting();
                }

                expectedPostingList.reset();
                assertEquals(expectedPostingList.advance(PrimaryKeyMap.IDENTITY.primaryKeyFromRowId(tokenToAdvance)),
                             reader.advance(PrimaryKeyMap.IDENTITY.primaryKeyFromRowId(tokenToAdvance)));

                assertPostingListEquals(expectedPostingList, reader);
                assertEquals(1, listener.advances);
            }
        }
    }

    @Test
    public void testAdvance() throws Exception
    {
        final int blockSize = 4; // 4 postings per FoR block
        final int maxSegmentRowID = 30;
        final int[] postings = IntStream.range(0, maxSegmentRowID).toArray(); // 30 postings = 7 FoR blocks + 1 VLong block
        final ArrayPostingList expected = new ArrayPostingList(postings);

        long fp;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, index, blockSize, false))
        {
            fp = writer.write(expected);
            writer.complete();
        }

//<<<<<<< HEAD
        try (SharedIndexInput sharedInput = new SharedIndexInput(indexDescriptor.openInput(postingLists)))
//=======
//        try (IndexInput input = indexDescriptor.openInput(postingLists))
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)
        {
            SAICodecUtils.validate(sharedInput);
            sharedInput.seek(fp);

            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expected, sharedInput);
            assertEquals((int) Math.ceil((double) maxSegmentRowID / blockSize), summary.offsets.length());

            for (int i = 0; i < summary.maxValues.length(); i++)
            {
                assertEquals(Math.min(maxSegmentRowID - 1, (i + 1) * blockSize - 1), summary.maxValues.get(i));
            }
        }

        // exact advance
        testAdvance(fp, expected, new int[]{ 3, 7, 11, 15, 19 });
        // non-exact advance
        testAdvance(fp, expected, new int[]{ 2, 6, 12, 17, 25 });

        // exact advance
        testAdvance(fp, expected, new int[]{ 3, 5, 7, 12 });
        // non-exact advance
        testAdvance(fp, expected, new int[]{ 2, 7, 9, 11 });
    }

    @Test
    public void testAdvanceOnRandomizedData() throws IOException
    {
        final int blockSize = 4;
        final int numPostings = nextInt(64, 64_000);
        final int[] postings = randomPostings(numPostings);

        final ArrayPostingList expected = new ArrayPostingList(postings);

        long fp = -1;
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, index, blockSize, false))
        {
            fp = writer.write(expected);
            writer.complete();
        }

//<<<<<<< HEAD
        try (SharedIndexInput sharedInput = new SharedIndexInput(indexDescriptor.openInput(postingLists)))
//=======
//        try (IndexInput input = indexDescriptor.openInput(postingLists))
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)
        {
            SAICodecUtils.validate(sharedInput);
            sharedInput.seek(fp);

            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expected, sharedInput);
            assertEquals((int) Math.ceil((double) numPostings / blockSize), summary.offsets.length());

            for (int i = 0; i < summary.maxValues.length(); i++)
            {
                assertEquals(postings[Math.min(numPostings - 1, (i + 1) * blockSize - 1)], summary.maxValues.get(i));
            }
        }

        testAdvance(fp, expected, postings);
    }

    @Test
    public void testNullPostingList() throws IOException
    {
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, index, false))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(null);
            writer.complete();
        }
    }

    @Test
    public void testEmptyPostingList() throws IOException
    {
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, index, false))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(new ArrayPostingList(new int[0]));
        }
    }

    @Test
    public void testNonAscendingPostingList() throws IOException
    {
        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, index, false))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(new ArrayPostingList(new int[]{ 1, 0 }));
        }
    }

    private void testAdvance(long fp, ArrayPostingList expected, int[] targetIDs) throws IOException
    {
        expected.reset();
        final CountingPostingListEventListener listener = new CountingPostingListEventListener();
        PostingsReader reader = openReader(fp, listener);
        for (int i = 0; i < 2; ++i)
        {
            assertEquals(expected.nextPosting(), reader.nextPosting());
            assertEquals(expected.getOrdinal(), reader.getOrdinal());
        }

        for (int target : targetIDs)
        {
            final long actualRowId = reader.advance(PrimaryKeyMap.IDENTITY.primaryKeyFromRowId(target));
            final long expectedRowId = expected.advance(PrimaryKeyMap.IDENTITY.primaryKeyFromRowId(target));

            assertEquals(expectedRowId, actualRowId);

            assertEquals(expected.getOrdinal(), reader.getOrdinal());
        }

        // check if iterator is correctly positioned
        assertPostingListEquals(expected, reader);
        // check if reader emitted all events
        assertEquals(targetIDs.length, listener.advances);

        reader.close();
    }

    private PostingsReader openReader(long fp, QueryEventListener.PostingListEventListener listener) throws IOException
    {
//<<<<<<< HEAD
        SharedIndexInput sharedInput = new SharedIndexInput(indexDescriptor.openInput(postingLists));
        sharedInput.seek(fp);
        return new PostingsReader(sharedInput, fp, listener);
//=======
//        IndexInput input = indexDescriptor.openInput(postingLists);
//        input.seek(fp);
//        return new PostingsReader(input, fp, listener);
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)
    }

    private PostingsReader.BlocksSummary assertBlockSummary(int blockSize, PostingList expected, SharedIndexInput sharedInput) throws IOException
    {
        final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(sharedInput, sharedInput.getFilePointer());
        assertEquals(blockSize, summary.blockSize);
        assertEquals(expected.size(), summary.numPostings);
        assertTrue(summary.offsets.length() > 0);
        assertEquals(summary.offsets.length(), summary.maxValues.length());
        return summary;
    }

    private int[] randomPostings(int numPostings)
    {
        final AtomicInteger rowId = new AtomicInteger();
        // postings with duplicates
        return IntStream.generate(() -> rowId.getAndAdd(randomIntBetween(0, 4)))
                        .limit(numPostings)
                        .toArray();
    }

    static class CountingPostingListEventListener implements QueryEventListener.PostingListEventListener
    {
        int advances;
        int decodes;

        @Override
        public void onAdvance()
        {
            advances++;
        }

        @Override
        public void postingDecoded(long postingsDecoded)
        {
            this.decodes += postingsDecoded;
        }
    }
}
