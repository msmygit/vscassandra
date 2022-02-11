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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.PriorityQueue;

public class MergePointsIterators implements AutoCloseable
{
    private final MergeQueue queue;
    private final BytesRefBuilder spare = new BytesRefBuilder();
    private final List<BlockTerms.Reader.PointsIterator> toClose;
    private long rowId = -1;

    public MergePointsIterators(List<BlockTerms.Reader.PointsIterator> iterators)
    {
        toClose = new ArrayList<>(iterators);
        queue = new MergeQueue(iterators.size());
        for (BlockTerms.Reader.PointsIterator iterator : iterators)
        {
            queue.add(iterator);
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
        for (BlockTerms.Reader.PointsIterator iterator : toClose)
        {
            iterator.close();
        }
    }

    public boolean next() throws IOException
    {
        while (queue.size() != 0)
        {
            final BlockTerms.Reader.PointsIterator iterator = queue.top();
            if (iterator.next())
            {
                rowId = iterator.rowId();
                spare.copyBytes(iterator.term());
                queue.updateTop();
                return true;
            }
            else
            {
                // iterator is exhausted
                queue.pop();
            }
        }
        return false;
    }

    private static class MergeQueue extends PriorityQueue<BlockTerms.Reader.PointsIterator>
    {
        public MergeQueue(int maxSize)
        {
            super(maxSize);
        }

        @Override
        public boolean lessThan(BlockTerms.Reader.PointsIterator a, BlockTerms.Reader.PointsIterator b)
        {
            assert a != b;

            int cmp = a.compareTo(b);

            if (cmp < 0)
            {
                return true;
            }
            else return false;
        }
    }
}
