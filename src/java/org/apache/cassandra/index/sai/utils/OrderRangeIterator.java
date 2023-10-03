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

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
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

    public OrderRangeIterator(RangeIterator input, int chunkSize, Function<List<PrimaryKey>, RangeIterator> nextRangeFunction)
    {
        super(input);
        this.input = input;
        this.chunkSize = chunkSize;
        this.nextRangeFunction = nextRangeFunction;
    }

    @Override
    public PrimaryKey computeNext()
    {
        if (nextIterator == null || !nextIterator.hasNext())
            nextIterator = computeNextIterator();
        if (nextIterator == null)
            return endOfData();
        return nextIterator.next();
    }

    private RangeIterator computeNextIterator()
    {
        if (!input.hasNext())
            return null;
        List<PrimaryKey> nextKeys = new ArrayList<>(chunkSize);
        do
        {
            nextKeys.add(input.next());
            // todo that was the wrong order for hasNext... wow, very subtle
            // we're still failing for chunk size 1 though
        }
        while (nextKeys.size() < chunkSize && input.hasNext());
        // each call here gets new leases...
        var previousIterator = nextIterator;
        // TODO how do we handle errors out of this handle errors?
        nextIterator = nextRangeFunction.apply(nextKeys);
        // Close afterward to make sure we keep the references counted correctly.
        if (previousIterator != null)
            FileUtils.closeQuietly(previousIterator);
        if (!nextIterator.hasNext())
            return computeNextIterator();
        return nextIterator;
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
