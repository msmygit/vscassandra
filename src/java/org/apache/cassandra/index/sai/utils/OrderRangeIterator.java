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

package org.apache.cassandra.index.sai.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.io.util.FileUtils;

// This is essentially a filter on top of a range iterator where we order chunks of primary keys,
// take the top ones and then put them back in primary key order
@NotThreadSafe
public class OrderRangeIterator extends RangeIterator
{
    private final RangeIterator input;
    private final int chunkSize;
    private final Function<List<PrimaryKey>, RangeIterator> nextRangeFunction;
    private RangeIterator nextIterator;
    private ArrayList<PrimaryKey> nextKeys;

    public OrderRangeIterator(RangeIterator input, int chunkSize, Function<List<PrimaryKey>, RangeIterator> nextRangeFunction)
    {
        super(input);
        this.input = input;
        this.chunkSize = chunkSize;
        this.nextRangeFunction = nextRangeFunction;
        this.nextKeys = new ArrayList<>(chunkSize);
    }

    @Override
    public PrimaryKey computeNext()
    {
        if (nextIterator == null || !nextIterator.hasNext())
        {
            do
            {
                if (!input.hasNext())
                    return endOfData();
                nextKeys.clear();
                do
                {
                    nextKeys.add(input.next());
                }
                while (nextKeys.size() < chunkSize && input.hasNext());
                // Get the next iterator before closing this one to prevent releasing the resource.
                var previousIterator = nextIterator;
                // If this results in an exception, previousIterator is closed in close() method.
                nextIterator = nextRangeFunction.apply(nextKeys);
                if (previousIterator != null)
                    FileUtils.closeQuietly(previousIterator);
                // nextIterator might not have any rows due to shadowed primary keys
            } while (!nextIterator.hasNext());
        }
        return nextIterator.next();
    }

    @Override
    protected void performSkipTo(PrimaryKey nextToken)
    {
        input.skipTo(nextToken);
        nextIterator.skipTo(nextToken);
    }

    public void close() {
        FileUtils.closeQuietly(input);
        FileUtils.closeQuietly(nextIterator);
    }
}
