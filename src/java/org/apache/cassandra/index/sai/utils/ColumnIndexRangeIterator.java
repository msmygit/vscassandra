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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.MergePostingList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

public class ColumnIndexRangeIterator extends RangeIterator
{
    private static final Logger logger = LoggerFactory.getLogger(ColumnIndexRangeIterator.class);

    private final QueryContext context;

    public final RangeIterator union;
    private final Set<SSTableIndex> referencedIndexes;

    private ColumnIndexRangeIterator(RangeIterator union, Set<SSTableIndex> referencedIndexes, QueryContext queryContext)
    {
        super(union.getMinimum(), union.getMaximum(), union.getCount());

        this.union = union;
        this.referencedIndexes = referencedIndexes;
        this.context = queryContext;
    }

    public static class SSTablePostings
    {
        public final List<PostingListRangeIterator> sstablePostingsRangeIterators;
        public final SSTableIndex ssTableIndex;

        public SSTablePostings(List<PostingListRangeIterator> sstablePostingsRangeIterators, SSTableIndex ssTableIndex)
        {
            this.sstablePostingsRangeIterators = sstablePostingsRangeIterators;
            this.ssTableIndex = ssTableIndex;
        }
    }

    @SuppressWarnings("resource")
    public static ColumnIndexRangeIterator build(final Expression expression,
                                                 Set<SSTableIndex> perSSTableIndexes,
                                                 AbstractBounds<PartitionPosition> keyRange,
                                                 QueryContext queryContext,
                                                 final Map<SSTableReader.UniqueIdentifier, Map<Expression,SSTablePostings>> map)
    {
        final List<RangeIterator> columnIndexRangeIterators = new ArrayList<>(1 + perSSTableIndexes.size());;

        RangeIterator memtableIterator = expression.context.searchMemtable(expression, keyRange);
        if (memtableIterator != null)
            columnIndexRangeIterators.add(memtableIterator);

        for (final SSTableIndex index : perSSTableIndexes)
        {
            try
            {
                queryContext.checkpoint();
                queryContext.incSstablesHit();
                assert !index.isReleased();

                SSTableQueryContext context = queryContext.getSSTableQueryContext(index.getSSTable());

                List<RangeIterator> sstableRangeIterators = index.search(expression, keyRange, context);

                List<PostingListRangeIterator> sstablePostingsRangeIterators = new ArrayList<>();
                for (RangeIterator rangeIterator : sstableRangeIterators)
                {
                    if (rangeIterator instanceof PostingListRangeIterator)
                    {
                        sstablePostingsRangeIterators.add((PostingListRangeIterator) rangeIterator);
                    }
                    else
                    {
                        assert rangeIterator == RangeIterator.empty();
                    }
                }

                if (sstableRangeIterators == null || sstableRangeIterators.isEmpty())
                    continue;

                Map<Expression,SSTablePostings> expressionMultimap = map.computeIfAbsent(index.getSSTable().instanceId, (id) -> new HashMap());
                expressionMultimap.put(expression, new SSTablePostings(sstablePostingsRangeIterators, index));

                columnIndexRangeIterators.addAll(sstablePostingsRangeIterators);
            }
            catch (Throwable e1)
            {
                if (logger.isDebugEnabled() && !(e1 instanceof AbortedOperationException))
                    logger.debug(String.format("Failed search an index %s, skipping.", index.getSSTable()), e1);

                throw Throwables.cleaned(e1);
            }
        }

        RangeIterator ranges = RangeUnionIterator.build(columnIndexRangeIterators);
        return new ColumnIndexRangeIterator(ranges, perSSTableIndexes, queryContext);
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
        referencedIndexes.forEach(ColumnIndexRangeIterator::releaseQuietly);
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
