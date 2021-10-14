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

import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.BlockPackedReader;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

public class PointIDFilterPostingListTest extends SaiRandomizedTest
{
    @Test
    public void testRandom() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);

        BlockPackedWriter writer = new BlockPackedWriter(output, 128);

        long[] array = new long[nextInt(1, 10_000)];
        for (int x = 0; x < array.length; x++)
        {
            array[x] = nextInt(1, 10_000);
            writer.add(array[x]);
        }

        long count = writer.ord();
        writer.finish();
        output.close();

        IntArrayList matchingRowIds = new IntArrayList();

        long min = nextInt(0, array.length);
        long max = nextInt((int)min + 1, array.length);

        for (int rowid = 0; rowid < array.length; rowid++)
        {
            if (array[rowid] >= min && array[rowid] <= max)
            {
                matchingRowIds.add(rowid);
            }
        }

        ArrayPostingList postings = new ArrayPostingList(matchingRowIds.toArray());

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);

        BlockPackedReader reader = new BlockPackedReader(input, PackedInts.VERSION_CURRENT, 128, count, true);
        PointIDFilterPostingList postings2 = new PointIDFilterPostingList(min, max, count, reader);

        int currentrowid = 0;
        while (true)
        {
            int nextrowid = nextInt(currentrowid, (int)count);
            long adv1 = postings.advance(nextrowid);
            long adv2 = postings2.advance(nextrowid);

            assertEquals(adv1, adv2);

            currentrowid = (int)adv1 + 1;

            if (adv1 == PostingList.END_OF_STREAM) break;
        }

        input.close();
    }

    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);

        long[] rowIDPointID = new long[] {55, 100, 1005, 2006, 4560, 6000};

        BlockPackedWriter writer = new BlockPackedWriter(output, 128);
        for (long pointID : rowIDPointID)
        {
            writer.add(pointID);
        }

        long count = writer.ord();
        writer.finish();
        output.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);

        BlockPackedReader reader = new BlockPackedReader(input, PackedInts.VERSION_CURRENT, 128, count, true);
        PointIDFilterPostingList postings = new PointIDFilterPostingList(150, 2007, count, reader);
        assertArrayEquals(new long[] {2, 3}, toArray(postings));

        PointIDFilterPostingList postings2 = new PointIDFilterPostingList(2003, 6006, count, reader);
        assertArrayEquals(new long[] {3, 4, 5}, toArray(postings2));

        PointIDFilterPostingList postings3 = new PointIDFilterPostingList(2003, 6006, count, reader);
        long advResult3 = postings3.advance(2);
        assertEquals(3, advResult3);

        PointIDFilterPostingList postings4 = new PointIDFilterPostingList(55, 1005, count, reader);
        long advResult4 = postings4.advance(1);
        assertEquals(1, advResult4);
        assertEquals(2, postings4.nextPosting());

        PointIDFilterPostingList postings5 = new PointIDFilterPostingList(40, 110, count, reader);
        assertArrayEquals(new long[] {0, 1}, toArray(postings5));

        input.close();
    }

    @Test
    public void test2() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);

        long[] rowIDPointID = new long[] {55, 50, 60, 100, 600, 101};

        BlockPackedWriter writer = new BlockPackedWriter(output, 128);
        for (long pointID : rowIDPointID)
        {
            writer.add(pointID);
        }

        long count = writer.ord();
        writer.finish();
        output.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);

        BlockPackedReader reader = new BlockPackedReader(input, PackedInts.VERSION_CURRENT, 128, count, true);
        PointIDFilterPostingList postings = new PointIDFilterPostingList(61, 102, count, reader);
        assertArrayEquals(new long[] {3, 5}, toArray(postings));
        input.close();
    }

    public static long[] toArray(PostingList postings) throws IOException
    {
        LongArrayList list = new LongArrayList();
        while (true)
        {
            final long rowid = postings.nextPosting();
            if (rowid == PostingList.END_OF_STREAM)
            {
                break;
            }
            list.add(rowid);
        }
        return list.toArray();
    }
}
