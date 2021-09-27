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
package org.apache.cassandra.index.sai.disk;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Merges multiple {@link PostingList} which individually contain unique items into a single list.
 */
@NotThreadSafe
public class MergePostingListV2 implements PostingList
{
    final List<PeekablePostingList> postingLists;
    final List<PeekablePostingList> candidates;
    final Closeable onClose;
    final long size;
    private long lastRowId = -1;

    private MergePostingListV2(List<PeekablePostingList> postingLists, Closeable onClose)
    {
        this.candidates = new ArrayList<>(postingLists.size());
        this.onClose = onClose;
        this.postingLists = postingLists;
        long size = 0;
        for (PostingList postingList : postingLists)
        {
            size += postingList.size();
        }
        this.size = size;
    }

    public static PostingList merge(List<PeekablePostingList> postings, Closeable onClose)
    {
        checkArgument(!postings.isEmpty());
        return postings.size() > 1 ? new MergePostingListV2(postings, onClose) : postings.get(0);
    }

    public static PostingList merge(List<PeekablePostingList> postings)
    {
        return merge(postings, () -> postings.forEach(posting -> FileUtils.closeQuietly(posting)));
    }

    @SuppressWarnings("resource")
    @Override
    public long nextPosting() throws IOException
    {
        candidates.clear();
        Iterator<PeekablePostingList> iterator = postingLists.iterator();
        long candidate = -1;
        while(iterator.hasNext())
        {
            PeekablePostingList postingList = iterator.next();
            while (postingList.peek() == lastRowId)
                postingList.nextPosting();
            long rowId = postingList.peek();
            if (rowId == END_OF_STREAM)
            {
                iterator.remove();
                continue;
            }
            if (candidate == -1)
            {
                candidate = postingList.peek();
                candidates.add(postingList);
            }
            else
            {
                if (candidate == postingList.peek())
                    candidates.add(postingList);
                else if (candidate > postingList.peek())
                {
                    candidates.clear();
                    candidate = postingList.peek();
                    candidates.add(postingList);
                }
            }
        }
        if (candidates.isEmpty())
            return END_OF_STREAM;

        for (PeekablePostingList postingList : candidates)
            postingList.nextPosting();

        lastRowId = candidate;

        return candidate;
    }

    @SuppressWarnings("resource")
    @Override
    public long advance(PrimaryKey key) throws IOException
    {
        for (PeekablePostingList postingList : postingLists)
            postingList.advanceWithoutConsuming(key);
        return nextPosting();
    }

    @Override
    public long advance(long targetRowId) throws IOException
    {
        for (PeekablePostingList postingList : postingLists)
            postingList.advanceWithoutConsuming(targetRowId);
        return nextPosting();
    }

    @Override
    public PrimaryKey mapRowId(long rowId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public void close() throws IOException
    {
        onClose.close();
    }
}
