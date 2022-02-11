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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.kdtree.OneDimBKDPostingsWriterTest;
import org.apache.cassandra.index.sai.disk.v1.postings.PackedLongsPostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.packed.PackedLongValues;

import static org.apache.cassandra.index.sai.disk.v1.kdtree.OneDimBKDPostingsWriterTest.pathToRoot;
import static org.apache.cassandra.index.sai.disk.v1.kdtree.OneDimBKDPostingsWriterTest.postings;

/**
 * Test based on {@link org.apache.cassandra.index.sai.disk.v1.kdtree.OneDimBKDPostingsWriterTest}
 */
public class BinaryTreePostingsWriterTest extends SaiRandomizedTest
{
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
    public void shouldSkipPostingListWhenTooFewLeaves() throws IOException
    {
        List<PackedLongValues> leaves = Collections.singletonList(postings(1, 2, 3));
        final BinaryTreePostingsWriter writer = new BinaryTreePostingsWriter(new IndexWriterConfig("test", 2, 2));;

        final List<Long> leafPostingFPs = new ArrayList<>();

        try (IndexOutputWriter leafPostingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_POSTINGS, indexContext);
             PostingsWriter postingsWriter = new PostingsWriter(leafPostingsOut))
        {
            for (PackedLongValues leafPostings : leaves)
            {
                long leafPostingsFP = postingsWriter.write(new PackedLongsPostingList(leafPostings));
                leafPostingFPs.add(leafPostingsFP);
            }
            postingsWriter.complete();
        }

        // The tree is too short to have any internal posting lists.
        writer.onLeaf(0, 16, pathToRoot(1, 2, 4, 8));

        BinaryTreePostingsWriter.Result result = null;

        try (FileHandle leafPostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.BLOCK_POSTINGS, indexContext);
             IndexOutputWriter output = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_UPPER_POSTINGS, indexContext))
        {
            result = writer.finish(leafPostingsHandle,
                                   new LongArray()
                                   {
                                       @Override
                                       public long get(long idx)
                                       {
                                           return leafPostingFPs.get((int) idx);
                                       }

                                       @Override
                                       public long length()
                                       {
                                           return leafPostingFPs.size();
                                       }

                                       @Override
                                       public long findTokenRowID(long targetToken)
                                       {
                                           throw new UnsupportedOperationException();
                                       }
                                   },
                                   output,
                                   indexContext);
        }

        try (IndexInput upperPostingsInput = indexDescriptor.openPerIndexInput(IndexComponent.BLOCK_UPPER_POSTINGS, indexContext))
        {
            BinaryTreePostingsReader postingsIndex = new BinaryTreePostingsReader(result.indexFilePointer, upperPostingsInput);

            assertEquals(0, postingsIndex.size());
        }
    }

    @Test
    public void shouldSkipPostingListWhenSamplingMisses() throws IOException
    {
        List<PackedLongValues> leaves = Collections.singletonList(postings(1, 2, 3));
        final List<Long> leafPostingFPs = new ArrayList<>();

        try (IndexOutputWriter leafPostingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_POSTINGS, indexContext);
             PostingsWriter postingsWriter = new PostingsWriter(leafPostingsOut))
        {
            for (PackedLongValues leafPostings : leaves)
            {
                long leafPostingsFP = postingsWriter.write(new PackedLongsPostingList(leafPostings));
                leafPostingFPs.add(leafPostingsFP);
            }
            postingsWriter.complete();
        }
        final BinaryTreePostingsWriter writer = new BinaryTreePostingsWriter(new IndexWriterConfig("test", 5, 1));;

        // The tree is too short to have any internal posting lists.
        writer.onLeaf(0, 16, pathToRoot(1, 2, 4, 8));

        BinaryTreePostingsWriter.Result result = null;

        try (FileHandle leafPostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.BLOCK_POSTINGS, indexContext);
             IndexOutputWriter output = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_UPPER_POSTINGS, indexContext))
        {
            result = writer.finish(leafPostingsHandle,
                                   new LongArray()
                                   {
                                       @Override
                                       public long get(long idx)
                                       {
                                           return leafPostingFPs.get((int) idx);
                                       }

                                       @Override
                                       public long length()
                                       {
                                           return leafPostingFPs.size();
                                       }

                                       @Override
                                       public long findTokenRowID(long targetToken)
                                       {
                                           throw new UnsupportedOperationException();
                                       }
                                   },
                                   output,
                                   indexContext);
        }

        try (IndexInput upperPostingsInput = indexDescriptor.openPerIndexInput(IndexComponent.BLOCK_UPPER_POSTINGS, indexContext))
        {
            BinaryTreePostingsReader postingsIndex = new BinaryTreePostingsReader(result.indexFilePointer, upperPostingsInput);

            assertEquals(0, postingsIndex.size());
        }
    }

    @Test
    public void shouldWritePostingsForEligibleNodes() throws IOException
    {
        List<PackedLongValues> leafPostingsList = Arrays.asList(postings(1, 5, 7), postings(3, 4, 6), postings(2, 8, 10), postings(11, 12, 13));

        final List<Long> leafPostingFPs = new ArrayList<>();

        try (IndexOutputWriter leafPostingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_POSTINGS, indexContext);
             PostingsWriter postingsWriter = new PostingsWriter(leafPostingsOut))
        {
            for (PackedLongValues leafPostings : leafPostingsList)
            {
                long leafPostingsFP = postingsWriter.write(new PackedLongsPostingList(leafPostings));
                leafPostingFPs.add(leafPostingsFP);
            }
            postingsWriter.complete();
        }

        final BinaryTreePostingsWriter writer = new BinaryTreePostingsWriter(new IndexWriterConfig("test", 2, 1));;

        // should build postings for nodes 2 & 3 (lvl 2) and 8, 10, 12, 14 (lvl 4)
        writer.onLeaf(0,64, pathToRoot(1, 2, 4, 8, 16));
        writer.onLeaf(1,80, pathToRoot(1, 2, 5, 10, 20));
        writer.onLeaf(2,96, pathToRoot(1, 3, 6, 12, 24));
        writer.onLeaf(3,112, pathToRoot(1, 3, 7, 14, 28));

        BinaryTreePostingsWriter.Result result = null;

        try (FileHandle leafPostingsHandle = indexDescriptor.createPerIndexFileHandle(IndexComponent.BLOCK_POSTINGS, indexContext);
             IndexOutputWriter output = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_UPPER_POSTINGS, indexContext))
        {
            result = writer.finish(leafPostingsHandle,
                                   new LongArray()
                                   {
                                       @Override
                                       public long get(long idx)
                                       {
                                           return leafPostingFPs.get((int) idx);
                                       }

                                       @Override
                                       public long length()
                                       {
                                           return leafPostingFPs.size();
                                       }

                                       @Override
                                       public long findTokenRowID(long targetToken)
                                       {
                                           throw new UnsupportedOperationException();
                                       }
                                   },
                                   output,
                                   indexContext);
        }

        try (IndexInput upperPostingsInput = indexDescriptor.openPerIndexInput(IndexComponent.BLOCK_UPPER_POSTINGS, indexContext))
        {
            BinaryTreePostingsReader postingsIndex = new BinaryTreePostingsReader(result.indexFilePointer, upperPostingsInput);

            assertEquals(6, postingsIndex.size());

            // Internal postings...
            assertTrue(postingsIndex.exists(2));
            assertTrue(postingsIndex.exists(3));
            assertTrue(postingsIndex.exists(8));
            assertTrue(postingsIndex.exists(10));
            assertTrue(postingsIndex.exists(12));
            assertTrue(postingsIndex.exists(14));

            assertPostingReaderEquals(postingsIndex, 2, new int[]{ 1, 3, 4, 5, 6, 7 });
            assertPostingReaderEquals(postingsIndex, 3, new int[]{ 2, 8, 10, 11, 12, 13 });
            assertPostingReaderEquals(postingsIndex, 8, new int[]{ 1, 5, 7 });
            assertPostingReaderEquals(postingsIndex, 10, new int[]{ 3, 4, 6 });
            assertPostingReaderEquals(postingsIndex, 12, new int[]{ 2, 8, 10 });
            assertPostingReaderEquals(postingsIndex, 14, new int[]{ 11, 12, 13 });
        }
    }

    private void assertPostingReaderEquals(BinaryTreePostingsReader postingsIndex,
                                           int nodeID,
                                           int[] postings) throws IOException
    {
        OneDimBKDPostingsWriterTest.assertPostingReaderEquals(indexDescriptor.openPerIndexInput(IndexComponent.BLOCK_UPPER_POSTINGS, indexContext),
                                                              postingsIndex.getPostingsFilePointer(nodeID),
                                                              new ArrayPostingList(postings));
    }
}
