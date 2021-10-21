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
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

public class TermIterator extends RangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(TermIterator.class);

    private final QueryContext context;

    private final RangeIterator union;
    private final Set<SSTableIndex> referencedIndexes;

    public TermIterator(RangeIterator union, Set<SSTableIndex> referencedIndexes, QueryContext queryContext)
    {
        super(union.getMinimum(), union.getMaximum(), union.getCount());

        this.union = union;
        this.referencedIndexes = referencedIndexes;
        this.context = queryContext;
    }

    protected PrimaryKey computeNext()
    {
        try
        {
            return union.hasNext() ? union.next() : endOfData();
        }
        finally
        {
            context.checkpoint();
        }
    }

    protected void performSkipTo(PrimaryKey nextToken)
    {
        try
        {
            union.skipTo(nextToken);
        }
        finally
        {
            context.checkpoint();
        }
    }

    public void close()
    {
        FileUtils.closeQuietly(union);
        referencedIndexes.forEach(TermIterator::releaseQuietly);
        referencedIndexes.clear();
    }

    private static void releaseQuietly(SSTableIndex index)
    {
        try
        {
            index.release();
        }
        catch (Throwable e)
        {
            logger.error(String.format("Failed to release index %s", index.getSSTable()), e);
        }
    }
}
