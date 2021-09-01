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

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.postings.PForDeltaPostingsReader;
import org.apache.cassandra.index.sai.disk.v2.postings.PForDeltaPostingsWriter;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;

public class PFoRPostingsTest extends NdiRandomizedTest
{
    @Test
    public void test() throws Exception
    {
        IndexDescriptor indexDescriptor = newIndexDescriptor();
        int blockSize = 128;
        String index = "index";
        IndexComponent postingLists = IndexComponent.create(IndexComponent.Type.POSTING_LISTS, index);

        // new int[]{ 10, 20, 30, 40, 50, 60 }

        int[] arr = new int[1000];
        for (int x=0; x < arr.length; x++)
        {
            arr[x] = x;
        }

        final ArrayPostingList expectedPostingList = new ArrayPostingList(arr);

        long postingPointer;
        try (PForDeltaPostingsWriter writer = new PForDeltaPostingsWriter(indexDescriptor, index, blockSize, false))
        {
            postingPointer = writer.write(expectedPostingList);
            writer.complete();
        }

        SharedIndexInput sharedInput = new SharedIndexInput(indexDescriptor.openInput(postingLists));

        expectedPostingList.reset();

        PostingsTest.CountingPostingListEventListener listener = new PostingsTest.CountingPostingListEventListener();
        try (PForDeltaPostingsReader reader = new PForDeltaPostingsReader(sharedInput, postingPointer, listener))
        {
            long actualRowID;
            while ((actualRowID = reader.nextPosting()) != PostingList.END_OF_STREAM)
            {
                long expectedPosting = expectedPostingList.nextPosting();
                assertEquals(expectedPosting, actualRowID);
            }
            assertEquals(PostingList.END_OF_STREAM, expectedPostingList.nextPosting());
        }
    }
}
