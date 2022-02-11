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
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;

/**
 * Merge streams of points iterators.
 */
@NotThreadSafe
public class MergePointsIterators implements AutoCloseable
{
    private final MergeQueue queue;
    private final BytesRefBuilder spare = new BytesRefBuilder();
    private final List<BlockTermsReader.PointsIterator> toClose;
    private long rowId = -1;

    public MergePointsIterators(List<BlockTermsReader.PointsIterator> iterators) throws IOException
    {
        toClose = new ArrayList<>(iterators);
        queue = new MergeQueue(iterators.size());
        for (BlockTermsReader.PointsIterator iterator : iterators)
        {
            // initialize each iterator
            if (iterator.next())
            {
                queue.add(iterator);
            }
        }
    }

    public BytesRef term()
    {
        return spare.get();
    }

    public long rowId()
    {
        return rowId;
    }

    @Override
    public void close() throws Exception
    {
        FileUtils.closeQuietly(toClose);
    }

    public boolean next() throws IOException
    {
        if (queue.size() != 0)
        {
            final BlockTermsReader.PointsIterator iterator = queue.top();

            rowId = iterator.rowId();
            spare.copyBytes(iterator.term());

            // iterator is exhausted, remove it
            if (!iterator.next())
                queue.pop();

            queue.updateTop();

            return true;
        }
        // when there are no more iterators
        return false;
    }

    private static class MergeQueue extends PriorityQueue<BlockTermsReader.PointsIterator>
    {
        public MergeQueue(int maxSize)
        {
            super(maxSize);
        }

        @Override
        public boolean lessThan(BlockTermsReader.PointsIterator a, BlockTermsReader.PointsIterator b)
        {
            assert a != b;

            final int cmp = a.compareTo(b);

            return cmp < 0;
        }
    }
}
