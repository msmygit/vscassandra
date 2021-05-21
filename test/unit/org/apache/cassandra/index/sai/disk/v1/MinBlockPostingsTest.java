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

import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.lucene.store.IndexInput;

public class MinBlockPostingsTest extends NdiRandomizedTest
{
    @Test
    public void test() throws Exception
    {
        final IndexComponents indexComponents = newIndexComponents();
        final int[] array = new int[]{ 10, 20, 30, 40, 50, 60 };
        ArrayPostingList expectedPostingList = new ArrayPostingList(array);

        long postingPointer;
        try (MinBlockPostingsWriter writer = new MinBlockPostingsWriter(indexComponents, 3, false))
        {
            postingPointer = writer.write(expectedPostingList);
            writer.complete();
        }

        IndexInput input = indexComponents.openBlockingInput(indexComponents.postingLists);
        MinBlockPostingsReader reader = new MinBlockPostingsReader(input, postingPointer, QueryEventListener.PostingListEventListener.NO_OP);

        expectedPostingList = new ArrayPostingList(array);

//        ReversePostingList reversePostings = reader.reversePostingList();
//        long rowID = reversePostings.advance(10);
//        System.out.println("advance reverse rowID=" + rowID);
//
//        rowID = reversePostings.nextPosting();
//        System.out.println("reverse rowID=" + rowID);

//        long rowID;
//        while ((rowID = reversePostings.nextPosting()) != ReversePostingList.REVERSE_END_OF_STREAM)
//        {
//            System.out.println("reverse rowID=" + rowID);
//        }

        //reversePostings.close();
    }

    @Test
    public void testDuplicates() throws Exception
    {
        final IndexComponents indexComponents = newIndexComponents();
        final int[] array = new int[]{ 10, 10, 10, 10, 10, 10 };
        ArrayPostingList expectedPostingList = new ArrayPostingList(array);

        long postingPointer;
        try (MinBlockPostingsWriter writer = new MinBlockPostingsWriter(indexComponents, 2,  false))
        {
            postingPointer = writer.write(expectedPostingList);
            writer.complete();
        }

        IndexInput input = indexComponents.openBlockingInput(indexComponents.postingLists);
        MinBlockPostingsReader reader = new MinBlockPostingsReader(input, postingPointer, QueryEventListener.PostingListEventListener.NO_OP);

        expectedPostingList = new ArrayPostingList(array);

        long advancedRowID = reader.advance(5);
        assertEquals(10, advancedRowID);

        long rowID = reader.nextPosting();
        assertEquals(10, rowID);

        reader.close();
    }
}
