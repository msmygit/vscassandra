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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.ReversePostingList;
import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.IntArrayReversePostingList;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.lucene.store.IndexInput;

public class PostingsTest extends NdiRandomizedTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testRandomReversePostingList() throws Exception
    {
       for (int x=0; x < 100; x++)
       {
           System.out.println("ROUND "+x);
           doTestReversePostingList();
       }
    }

    private void doTestReversePostingList() throws Exception
    {
        final IndexComponents indexComponents = newIndexComponents();
        final int blockSize = 4;//nextInt(2, 10);
        System.out.println("blockSize="+blockSize);
        //long[] array = new long[] {83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83, 83};

        //long[] array = new long[] {36, 125, 212, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258, 258};

        long[] array = new long[nextInt(10, 1000)];
        long current = nextInt(0, 100);

        for (int x = 0; x < array.length; x++)
        {
            array[x] = current;
            if (nextInt(0, 10) == 0)
            {
                int rounds = nextInt(0, Math.min(array.length - x, 10));
                for (int y = 0; y < rounds || x < array.length; y++)
                {
                    x++;
                    if (x >= array.length) break;
                    array[x] = current;
                }
            }
            else
            {
                current += nextInt(0, 100);
            }
        }
        long start = array[0];
        System.out.println("array=" + Arrays.toString(array));
        final ArrayPostingList expectedPostingList = new ArrayPostingList(array);

        IntArrayReversePostingList revExpectedPostingList = new IntArrayReversePostingList(array);

        long postingPointer;
        try (PostingsWriter writer = new PostingsWriter(indexComponents, blockSize, false))
        {
            postingPointer = writer.write(expectedPostingList);
            writer.complete();
        }

        // check straight iteration first
//        IndexInput input = indexComponents.openBlockingInput(indexComponents.postingLists);
//        input.seek(postingPointer);
//        CountingPostingListEventListener listener = new CountingPostingListEventListener();
//        ReversePostingsReader reader = new ReversePostingsReader(input, postingPointer, listener);
//        //reader.lastPosInBlock(1);
//        while (true)
//        {
//            final long posting1 = revExpectedPostingList.nextPosting();
//            final long posting2 = reader.nextPosting();
//            //System.out.println("posting2="+posting2);
//
//            System.out.println("posting1="+posting1+" posting2="+posting2);
//
//            assertEquals(posting1, posting2);
//
//            if (posting2 == ReversePostingList.REVERSE_END_OF_STREAM)
//            {
//                break;
//            }
//
//            final long ordinal1 = revExpectedPostingList.getOrdinal();
//            final long ordinal2 = reader.getOrdinal();
//
//            assertEquals(ordinal1, ordinal2);
//        }
//
//        reader.close();

        revExpectedPostingList = new IntArrayReversePostingList(array);

        IndexInput input = indexComponents.openBlockingInput(indexComponents.postingLists);
        input.seek(postingPointer);
        CountingPostingListEventListener listener = new CountingPostingListEventListener();
        ReversePostingsReader reader = new ReversePostingsReader(input, postingPointer, listener);
        long actualRowID = array[array.length-1];
        long actualRowID2 = array[array.length-1];

        long target;
        long ordinal1, ordinal2;

//        long target = 83;
//        actualRowID = reader.advance(target);
//        actualRowID2 = revExpectedPostingList.advance(target);
//
//        assertEquals(actualRowID2, actualRowID);
//
//        long ordinal1 = revExpectedPostingList.getOrdinal();
//        long ordinal2 = reader.getOrdinal();
//
//        //assertEquals(ordinal1, ordinal2);
//        System.out.println("ordinal1="+actualRowID+" ordinal2="+ordinal2);
//
//        System.out.println("advanced actualRowID="+actualRowID);

        int count = 0;

        while (true)
        {
            if (nextInt(0, 5) == 0)
            {
                System.out.println("random advance actualRowID="+actualRowID);
                if (actualRowID == -1) break;
                target = nextLong(start, actualRowID + 1);
                if (target != actualRowID)
                {
                    System.out.println("random advance target=" + target);
                    actualRowID2 = reader.advance(target);
                    actualRowID = revExpectedPostingList.advance(target);
                }
            }
            else
            {
                actualRowID = revExpectedPostingList.nextPosting();

                actualRowID2 = reader.nextPosting();
                System.out.println("count="+(count+1)+" actualRowID=" + actualRowID+" actualRowID2=" + actualRowID2);
            }
            assertEquals(actualRowID, actualRowID2);
            count++;

            if (actualRowID == ReversePostingList.REVERSE_END_OF_STREAM)
            {
                break;
            }

            ordinal1 = revExpectedPostingList.getOrdinal();
            ordinal2 = reader.getOrdinal();

            assertEquals(ordinal1, ordinal2);
        }

        reader.close();
    }

    @Test
    public void testSingleBlockPostingList() throws Exception
    {
        final IndexComponents indexComponents = newIndexComponents();
        final int blockSize = 1 << between(3, 8);
        final ArrayPostingList expectedPostingList = new ArrayPostingList(new int[]{ 10, 20, 30, 40, 50, 60 });

        long postingPointer;
        try (PostingsWriter writer = new PostingsWriter(indexComponents, blockSize, false))
        {
            postingPointer = writer.write(expectedPostingList);
            writer.complete();
        }

        IndexInput input = indexComponents.openBlockingInput(indexComponents.postingLists);
        SAICodecUtils.validate(input);
        input.seek(postingPointer);

        final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expectedPostingList, input);
        assertEquals(1, summary.offsets.length());

        CountingPostingListEventListener listener = new CountingPostingListEventListener();
        PostingsReader reader = new PostingsReader(input, postingPointer, listener);

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
        assertEquals(reader.size(), listener.decodes);

        input = indexComponents.openBlockingInput(indexComponents.postingLists);
        listener = new CountingPostingListEventListener();
        reader = new PostingsReader(input, postingPointer, listener);

        assertEquals(50, reader.advance(45));

        assertEquals(60, reader.advance(60));
        assertEquals(PostingList.END_OF_STREAM, reader.nextPosting());
        assertEquals(2, listener.advances);
        reader.close();

        assertEquals(reader.size(), listener.decodes); // nothing more was decoded
    }

    @Test
    public void testMultiBlockPostingList() throws Exception
    {
        final IndexComponents indexComponents = newIndexComponents();
        final int numPostingLists = 1 << between(1, 5);
        final int blockSize = 1 << between(5, 10);
        final int numPostings = between(1 << 11, 1 << 15);
        final ArrayPostingList[] expected = new ArrayPostingList[numPostingLists];
        final long[] postingPointers = new long[numPostingLists];

        try (PostingsWriter writer = new PostingsWriter(indexComponents, blockSize, false))
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

        try (IndexInput input = indexComponents.openBlockingInput(indexComponents.postingLists))
        {
            SAICodecUtils.validate(input);
        }

        for (int i = 0; i < numPostingLists; ++i)
        {
            IndexInput input = indexComponents.openBlockingInput(indexComponents.postingLists);
            input.seek(postingPointers[i]);
            final ArrayPostingList expectedPostingList = expected[i];
            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expectedPostingList, input);
            assertTrue(summary.offsets.length() > 1);

            final CountingPostingListEventListener listener = new CountingPostingListEventListener();
            try (PostingsReader reader = new PostingsReader(input, postingPointers[i], listener))
            {
                expectedPostingList.reset();
                assertEquals(expectedPostingList.getOrdinal(), reader.getOrdinal());
                assertEquals(expectedPostingList.size(), reader.size());

                assertPostingListEquals(expectedPostingList, reader);
                assertEquals(0, listener.advances);
            }

            // test skipping to the last block
            input = indexComponents.openBlockingInput(indexComponents.postingLists);
            try (PostingsReader reader = new PostingsReader(input, postingPointers[i], listener))
            {
                long tokenToAdvance = -1;
                expectedPostingList.reset();
                for (int p = 0; p < numPostings - 7; ++p)
                {
                    tokenToAdvance = expectedPostingList.nextPosting();
                }

                expectedPostingList.reset();
                assertEquals(expectedPostingList.advance(tokenToAdvance),
                             reader.advance(tokenToAdvance));

                assertPostingListEquals(expectedPostingList, reader);
                assertEquals(1, listener.advances);
            }
        }
    }

    @Test
    public void testAdvance() throws Exception
    {
        final IndexComponents indexComponents = newIndexComponents();
        final int blockSize = 4; // 4 postings per FoR block
        final int maxSegmentRowID = 30;
        final int[] postings = IntStream.range(0, maxSegmentRowID).toArray(); // 30 postings = 7 FoR blocks + 1 VLong block
        final ArrayPostingList expected = new ArrayPostingList(postings);

        long fp;
        try (PostingsWriter writer = new PostingsWriter(indexComponents, blockSize, false))
        {
            fp = writer.write(expected);
            writer.complete();
        }

        try (IndexInput input = indexComponents.openBlockingInput(indexComponents.postingLists))
        {
            SAICodecUtils.validate(input);
            input.seek(fp);

            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expected, input);
            assertEquals((int) Math.ceil((double) maxSegmentRowID / blockSize), summary.offsets.length());

            for (int i = 0; i < summary.maxValues.length(); i++)
            {
                assertEquals(Math.min(maxSegmentRowID - 1, (i + 1) * blockSize - 1), summary.maxValues.get(i));
            }
        }

        // exact advance
        testAdvance(indexComponents, fp, expected, new int[]{ 3, 7, 11, 15, 19 });
        // non-exact advance
        testAdvance(indexComponents, fp, expected, new int[]{ 2, 6, 12, 17, 25 });

        // exact advance
        testAdvance(indexComponents, fp, expected, new int[]{ 3, 5, 7, 12 });
        // non-exact advance
        testAdvance(indexComponents, fp, expected, new int[]{ 2, 7, 9, 11 });
    }

    @Test
    public void testAdvanceOnRandomizedData() throws IOException
    {
        final IndexComponents indexComponents = newIndexComponents();
        final int blockSize = 4;
        final int numPostings = nextInt(64, 64_000);
        final int[] postings = randomPostings(numPostings);

        final ArrayPostingList expected = new ArrayPostingList(postings);

        long fp;
        try (PostingsWriter writer = new PostingsWriter(indexComponents, blockSize, false))
        {
            fp = writer.write(expected);
            writer.complete();
        }

        try (IndexInput input = indexComponents.openBlockingInput(indexComponents.postingLists))
        {
            SAICodecUtils.validate(input);
            input.seek(fp);

            final PostingsReader.BlocksSummary summary = assertBlockSummary(blockSize, expected, input);
            assertEquals((int) Math.ceil((double) numPostings / blockSize), summary.offsets.length());

            for (int i = 0; i < summary.maxValues.length(); i++)
            {
                assertEquals(postings[Math.min(numPostings - 1, (i + 1) * blockSize - 1)], summary.maxValues.get(i));
            }
        }

        testAdvance(indexComponents, fp, expected, postings);
    }

    @Test
    public void testNullPostingList() throws IOException
    {
        final IndexComponents indexComponents = newIndexComponents();
        try (PostingsWriter writer = new PostingsWriter(indexComponents, false))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(null);
            writer.complete();
        }
    }

    @Test
    public void testEmptyPostingList() throws IOException
    {
        final IndexComponents indexComponents = newIndexComponents();
        try (PostingsWriter writer = new PostingsWriter(indexComponents, false))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(new ArrayPostingList(new int[0]));
        }
    }

    @Test
    public void testNonAscendingPostingList() throws IOException
    {
        final IndexComponents indexComponents = newIndexComponents();
        try (PostingsWriter writer = new PostingsWriter(indexComponents, false))
        {
            expectedException.expect(IllegalArgumentException.class);
            writer.write(new ArrayPostingList(new int[]{ 1, 0 }));
        }
    }

    private void testAdvance(IndexComponents indexComponents, long fp, ArrayPostingList expected, int[] targetIDs) throws IOException
    {
        expected.reset();
        final CountingPostingListEventListener listener = new CountingPostingListEventListener();
        PostingsReader reader = openReader(indexComponents, fp, listener);
        for (int i = 0; i < 2; ++i)
        {
            assertEquals(expected.nextPosting(), reader.nextPosting());
            assertEquals(expected.getOrdinal(), reader.getOrdinal());
        }

        for (int target : targetIDs)
        {
            final long actualRowId = reader.advance(target);
            final long expectedRowId = expected.advance(target);

            assertEquals(expectedRowId, actualRowId);

            assertEquals(expected.getOrdinal(), reader.getOrdinal());
        }

        // check if iterator is correctly positioned
        assertPostingListEquals(expected, reader);
        // check if reader emitted all events
        assertEquals(targetIDs.length, listener.advances);

        reader.close();
    }

    private PostingsReader openReader(IndexComponents indexComponents, long fp, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        IndexInput input = indexComponents.openBlockingInput(indexComponents.postingLists);
        input.seek(fp);
        return new PostingsReader(input, fp, listener);
    }

    private PostingsReader.BlocksSummary assertBlockSummary(int blockSize, PostingList expected, IndexInput input) throws IOException
    {
        final PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(input, input.getFilePointer());
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
