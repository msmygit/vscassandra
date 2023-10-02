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

package org.apache.cassandra.index.sai.plan;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;

// This is essentially a filter on top of a range iterator where we order chunks of primary keys,
// take the top ones and then put them back in primary key order
public class OrderRangeIterator extends RangeIterator
{
    private final RangeIterator wrapped;
    private final int chunkSize;
    private final RowFilter.Expression expression;
    private final QueryController controller;
    private RangeIterator nextIterator;

    public OrderRangeIterator(RangeIterator wrapped, int chunkSize, RowFilter.Expression expression, QueryController controller)
    {
        super(wrapped);
        System.out.println("Creating orderer");
        this.wrapped = wrapped;
        this.chunkSize = chunkSize;
        this.expression = expression;
        this.controller = controller;
    }

    // TODO figure out what needs to be overridden in this class.
//    @Override
//    public boolean tryToComputeNext()
//    {
//        return (nextIterator != null && nextIterator.hasNext()) || wrapped.hasNext();
//    }

    protected boolean tryToComputeNext()
    {
        super.tryToComputeNext();
        // todo why is this necessary
        return next != null;
    }

    @Override
    public PrimaryKey computeNext()
    {
        if (nextIterator == null || !nextIterator.hasNext())
        {
            System.out.println("Creating nextIterator");
            if (!wrapped.hasNext())
                return endOfData();
            List<PrimaryKey> nextKeys = new ArrayList<>(chunkSize);
            do
            {
                nextKeys.add(wrapped.next());
            }
            while (wrapped.hasNext() && nextKeys.size() < chunkSize);
            // each call here gets new leases...
            var previousIterator = nextIterator;
            nextIterator = controller.getTopKRows(nextKeys, expression);
            // Close afterward to make sure we keep the references counted correctly.
            if (previousIterator != null)
                FileUtils.closeQuietly(previousIterator);
            if (!nextIterator.hasNext())
                return endOfData();
        }
        // todo getting no such element here... seems crazy to me!
        return nextIterator.next();
    }

    // -Dcassandra.test.random.seed=61262512248244 failure
    // there was leak detected during testUpdateNonVectorColumnWhereNoSingleSSTableRowMatchesAllPredicates, but
    // only on first run...

    @Override
    protected void performSkipTo(PrimaryKey nextToken)
    {
        // TODO is this necessary
        System.out.println("Skipping to " + nextToken);
        wrapped.skipTo(nextToken);
        if (nextIterator != null && nextToken.compareTo(nextIterator.getMaximum()) > 0)
        {
            FileUtils.closeQuietly(nextIterator);
            nextIterator = null;
        }
    }

    public void close() {
        FileUtils.closeQuietly(wrapped);
        FileUtils.closeQuietly(nextIterator);
    }


}
