/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.io.util.FileUtils;

public class DisjunctionDISI implements PostingList
{
    final DisiPriorityQueue subIterators;
    final long cost;
    final Closeable onClose;

    private DisjunctionDISI(DisiPriorityQueue subIterators, Closeable onClose)
    {
        this.subIterators = subIterators;
        this.onClose = onClose;
        long cost = 0;
        for (DisiWrapper w : subIterators)
        {
            cost += w.cost;
        }
        this.cost = cost;
    }

    @Override
    public void close() throws IOException
    {
        if (onClose != null)
        {
            FileUtils.close(onClose);
        }
    }

    public static PostingList create(PriorityQueue<PeekablePostingList> postings)
    {
        return create(postings, null);
    }

    public static PostingList create(PriorityQueue<PeekablePostingList> postings, Closeable onClose)
    {
        DisiPriorityQueue queue = new DisiPriorityQueue(postings.size());
        for (PostingList list : postings)
        {
            queue.add(new DisiWrapper(list));
        }
        return new DisjunctionDISI(queue, onClose);
    }

    public static PostingList create(List<PostingList> postings, Closeable onClose)
    {
        DisiPriorityQueue queue = new DisiPriorityQueue(postings.size());
        for (PostingList list : postings)
        {
            queue.add(new DisiWrapper(list));
        }
        return new DisjunctionDISI(queue, onClose);
    }

    @Override
    public long size()
    {
        return cost;
    }

//    @Override
//    public long currentPosting()
//    {
//        return subIterators.top().doc;
//    }

    @Override
    public long nextPosting() throws IOException
    {
        DisiWrapper top = subIterators.top();
        final long doc = top.doc;
        do
        {
            top.doc = top.iterator.nextPosting();
            top = subIterators.updateTop();
        } while (top.doc == doc);

        return top.doc;
    }

    @Override
    public long advance(long target) throws IOException
    {
        DisiWrapper top = subIterators.top();
        do
        {
            top.doc = top.iterator.advance(target);
            top = subIterators.updateTop();
        } while (top.doc < target);

        return top.doc;
    }
}


