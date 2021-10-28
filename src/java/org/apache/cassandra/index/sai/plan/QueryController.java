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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.MultiRangeReadCommand;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.IndexSearcherContext;
import org.apache.cassandra.index.sai.disk.MergePostingList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v2.postings.Copyable;
import org.apache.cassandra.index.sai.metrics.TableQueryMetrics;
import org.apache.cassandra.index.sai.utils.ColumnIndexRangeIterator;
import org.apache.cassandra.index.sai.utils.ConjunctionPostingList;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.RangeUnionIterator;
import org.apache.cassandra.index.sai.utils.TermIterator;
import org.apache.cassandra.index.sai.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Ref;

public class QueryController
{
    private static final Logger logger = LoggerFactory.getLogger(QueryController.class);

    private final ColumnFamilyStore cfs;
    private final ReadCommand command;
    private final QueryContext queryContext;
    private final TableQueryMetrics tableQueryMetrics;
    private final RowFilter.FilterElement filterOperation;
    private final IndexFeatureSet indexFeatureSet;
    private final List<DataRange> ranges;
    private final AbstractBounds<PartitionPosition> mergeRange;

    public QueryController(ColumnFamilyStore cfs,
                           ReadCommand command,
                           RowFilter.FilterElement filterOperation,
                           IndexFeatureSet indexFeatureSet,
                           QueryContext queryContext,
                           TableQueryMetrics tableQueryMetrics)
    {
        this.cfs = cfs;
        this.command = command;
        this.queryContext = queryContext;
        this.tableQueryMetrics = tableQueryMetrics;
        this.filterOperation = filterOperation;
        this.indexFeatureSet = indexFeatureSet;
        this.ranges = dataRanges(command);
        DataRange first = ranges.get(0);
        DataRange last = ranges.get(ranges.size() - 1);
        this.mergeRange = ranges.size() == 1 ? first.keyRange() : first.keyRange().withNewRight(last.keyRange().right);
    }

    public TableMetadata metadata()
    {
        return command.metadata();
    }

    RowFilter.FilterElement filterOperation()
    {
        return this.filterOperation;
    }

    /**
     * @return token ranges used in the read command
     */
    List<DataRange> dataRanges()
    {
        return ranges;
    }

    /**
     * Note: merged range may contain subrange that no longer belongs to the local node after range movement.
     * It should only be used as an optimization to reduce search space. Use {@link #dataRanges()} instead to filter data.
     *
     * @return merged token range
     */
    AbstractBounds<PartitionPosition> mergeRange()
    {
        return mergeRange;
    }

    /**
     * @return indexed {@code ColumnContext} if index is found; otherwise return non-indexed {@code ColumnContext}.
     */
    public IndexContext getContext(RowFilter.Expression expression)
    {
        StorageAttachedIndex index = getBestIndexFor(expression);

        return index != null ? index.getIndexContext() : new IndexContext(cfs.metadata(), expression.column());
    }

    public StorageAttachedIndex getBestIndexFor(RowFilter.Expression expression)
    {
        return cfs.indexManager.getBestIndexFor(expression, StorageAttachedIndex.class).orElse(null);
    }

    public boolean needsRow(PrimaryKey key)
    {
        return key.hasEmptyClustering() || command.clusteringIndexFilter(key.partitionKey()).selects(key.clustering());
    }

    public UnfilteredRowIterator getPartition(PrimaryKey key, ReadExecutionController executionController)
    {
        if (key == null)
            throw new IllegalArgumentException("non-null key required");

        try
        {
            SinglePartitionReadCommand partition = SinglePartitionReadCommand.create(cfs.metadata(),
                                                                                     command.nowInSec(),
                                                                                     command.columnFilter(),
                                                                                     RowFilter.NONE,
                                                                                     DataLimits.NONE,
                                                                                     key.partitionKey(),
                                                                                     makeFilter(key));

            return partition.queryMemtableAndDisk(cfs, executionController);
        }
        finally
        {
            queryContext.checkpoint();
        }
    }

    /**
     * Build a {@link RangeIterator.Builder} from the given list of expressions by applying given operation (OR/AND).
     * Building of such builder involves index search, results of which are persisted in the internal resources list
     *
     * @param op The operation type to coalesce expressions with.
     * @param expressions The expressions to build range iterator from (expressions with not results are ignored).
     *
     * @return range iterator builder based on given expressions and operation type.
     */
//    public RangeIterator.Builder getIndexes(Operation.OperationType op, Collection<Expression> expressions)
//    {
//        final Multimap<SSTableReader.UniqueIdentifier, ColumnIndexRangeIterator.ExpressionSSTablePostings> sstablePostingsMap = HashMultimap.create();
//
//        RangeIterator.Builder builder = op == Operation.OperationType.OR
//                                        ? RangeUnionIterator.builder()
//                                        : RangeIntersectionIterator.selectiveBuilder();
//
//        Set<Map.Entry<Expression, NavigableSet<SSTableIndex>>> view = referenceAndGetView(op, expressions).entrySet();
//
//        final RangeIterator.Builder columnRangeBuilder = op == Operation.OperationType.OR
//                                                         ? RangeUnionIterator.builder()
//                                                         : RangeIntersectionIterator.selectiveBuilder();
//
//        try
//        {
//            for (Map.Entry<Expression, NavigableSet<SSTableIndex>> e : view)
//            {
//                ColumnIndexRangeIterator columnIndexRangeIterator = ColumnIndexRangeIterator.build(e.getKey(),
//                                                                                                   e.getValue(),
//                                                                                                   mergeRange,
//                                                                                                   queryContext,
//                                                                                                   sstablePostingsMap);
//                columnRangeBuilder.add(columnIndexRangeIterator);
//
//                @SuppressWarnings("resource") // RangeIterators are closed by releaseIndexes
//                RangeIterator index = TermIterator.build(e.getKey(), e.getValue(), mergeRange, queryContext);
//
//                builder.add(index);
//            }
//        }
//        catch (Throwable t)
//        {
//            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
//            FileUtils.closeQuietly(builder.ranges());
//            view.forEach(e -> e.getValue().forEach(SSTableIndex::release));
//            throw t;
//        }
//        return builder;
//    }
    public RangeIterator.Builder getIndexes(Operation.OperationType op, Collection<Expression> expressions)
    {
        final Map<SSTableReader.UniqueIdentifier, Map<Expression, ColumnIndexRangeIterator.SSTableIndexPostings>> map = new HashMap();

        Set<Map.Entry<Expression, NavigableSet<SSTableIndex>>> view = referenceAndGetView(op, expressions).entrySet();

        final RangeIterator.Builder columnRangeBuilder = op == Operation.OperationType.OR
                                                         ? RangeUnionIterator.builder()
                                                         : RangeIntersectionIterator.selectiveBuilder();

        try
        {
            for (Map.Entry<Expression, NavigableSet<SSTableIndex>> e : view)
            {
                ColumnIndexRangeIterator columnIndexRangeIterator = ColumnIndexRangeIterator.build(e.getKey(),
                                                                                                   e.getValue(),
                                                                                                   mergeRange,
                                                                                                   queryContext,
                                                                                                   map);
                columnRangeBuilder.add(columnIndexRangeIterator);
            }



            RangeIterator.Builder perSSTableRangeIterators = getIndexesPostings(op, expressions, map);
            return perSSTableRangeIterators;
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            FileUtils.closeQuietly(columnRangeBuilder.ranges());
            view.forEach(e -> e.getValue().forEach(SSTableIndex::release));
            throw t;
        }
        //return columnRangeBuilder;
    }

    public RangeIterator.Builder getIndexesPostings(Operation.OperationType op,
                                                    Collection<Expression> expressions,
                                                    final Map<SSTableReader.UniqueIdentifier, Map<Expression, ColumnIndexRangeIterator.SSTableIndexPostings>> map)
    {
        final RangeIterator.Builder builder = RangeUnionIterator.builder();

        final List<RangeIterator> memoryRangeIterators = new ArrayList<>();
        for (final Expression expression : expressions)
        {
            final RangeIterator memtableIterator = expression.context.searchMemtable(expression, mergeRange);
            memoryRangeIterators.add(memtableIterator);
        }

        final RangeIterator primaryMemoryRangeIterator;

        if (op == Operation.OperationType.AND)
        {
            RangeIntersectionIterator.Builder andBuilder = RangeIntersectionIterator.builder();
            for (RangeIterator it : memoryRangeIterators)
            {
                andBuilder.add(it);
            }
            primaryMemoryRangeIterator = andBuilder.build();
        }
        else
        {
            assert op == Operation.OperationType.OR;

            RangeUnionIterator.Builder orBuilder = RangeUnionIterator.builder();
            orBuilder.add(memoryRangeIterators);
            primaryMemoryRangeIterator = orBuilder.build();
        }

        builder.add(primaryMemoryRangeIterator);

        final List<PostingList> toClose = new ArrayList<>();

        try
        {
            // iterate over each sstable
            for (Map.Entry<SSTableReader.UniqueIdentifier, Map<Expression, ColumnIndexRangeIterator.SSTableIndexPostings>> entry : map.entrySet())
            {
                final PriorityQueue<PostingList.PeekablePostingList> sstablePostingLists = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));

                final List<PostingList.PeekablePostingList> sstableMissingValuePostingLists = new ArrayList<>();

                final Map<Expression, ColumnIndexRangeIterator.SSTableIndexPostings> expMap = entry.getValue();

                IndexContext indexContext = null;
                PrimaryKeyMap primaryKeyMap = null;
                IndexSearcherContext context = null;

                PrimaryKey maxKey = null;
                PrimaryKey minKey = null;

                List<PostingList> acrossSSTableMissingValuePostings = new ArrayList<>();
                for (ColumnIndexRangeIterator.SSTableIndexPostings postings : expMap.values())
                {
                    SearchableIndex searchableIndex = postings.ssTableIndex.getSearchableIndex();
                    PostingList missingValuePostings = searchableIndex.missingValuesPostings();
                    if (missingValuePostings != null)
                    {
                        acrossSSTableMissingValuePostings.add(missingValuePostings);
                    }
                }

                // iterate over the postings of each expression/column
                for (ColumnIndexRangeIterator.SSTableIndexPostings postings : expMap.values())
                {
                    List<PostingList.PeekablePostingList> missingValuesPostingsList = new ArrayList<>();
                    if (acrossSSTableMissingValuePostings.size() > 0)
                    {
                        for (PostingList list : acrossSSTableMissingValuePostings)
                        {
                            missingValuesPostingsList.add(((Copyable) list).copy().peekable());
                        }
                    }

                    List<PostingList.PeekablePostingList> postingsCopies = new ArrayList<>();
                    for (PostingListRangeIterator iterator : postings.sstablePostingsRangeIterators)
                    {
                        PostingList postingsCopy = iterator.context.copyablePostings.copy();
                        postingsCopies.add(postingsCopy.peekable());
                    }

                    // AND the sstable postings with the missing values postings
                    // to get the green light postings that are OR'd into the
                    // sstable postings range iterator

                    if (missingValuesPostingsList.size() > 0)
                    {
                        PostingList missingValuesPostings = MergePostingList.merge(missingValuesPostingsList);
                        PostingList singleCopyPostings = MergePostingList.merge(postingsCopies);

                        ConjunctionPostingList missingValuePostings = new ConjunctionPostingList(missingValuesPostings, singleCopyPostings);

                        sstableMissingValuePostingLists.add(missingValuePostings.peekable());
                    }

                    final PriorityQueue<PostingList.PeekablePostingList> expPostings = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));

                    for (PostingListRangeIterator iterator : postings.sstablePostingsRangeIterators)
                    {
                        expPostings.add(iterator.postingList.peekable());

                        if (minKey == null)
                        {
                            minKey = iterator.getMinimum();
                        }
                        else
                        {
                            minKey = iterator.getMinimum().compareTo(minKey) < 0 ? iterator.getMinimum() : minKey;
                        }

                        if (maxKey == null)
                        {
                            maxKey = iterator.getMaximum();
                        }
                        else
                        {
                            maxKey = iterator.getMaximum().compareTo(maxKey) > 0 ? iterator.getMaximum() : maxKey;
                        }

                        if (indexContext == null)
                        {
                            indexContext = iterator.indexContext;
                            primaryKeyMap = iterator.primaryKeyMap;
                            context = iterator.context;
                        }
                    }

                    sstablePostingLists.add(MergePostingList.merge(expPostings).peekable());
                }

                if (sstablePostingLists.size() == 0)
                {
                    continue;
                }

                final PostingList sstablePostings;

                if (op == Operation.OperationType.OR)
                {
                    sstablePostings = MergePostingList.merge(sstablePostingLists);
                }
                else
                {
                    assert op == Operation.OperationType.AND;

                    if (sstablePostingLists.size() < 2)
                    {
                        sstablePostings = null;
                    }
                    else
                    {
                        sstablePostings = new ConjunctionPostingList(new ArrayList(sstablePostingLists));
                    }
                }

                // OR missingValuesPostings with the direct postings
                List<PostingList.PeekablePostingList> temp = new ArrayList<>();
                if (sstablePostings != null)
                {
                    temp.add(sstablePostings.peekable());
                }
                temp.addAll(sstableMissingValuePostingLists);
                if (temp.size() > 0)
                {
                    System.out.println("sstableMissingValuePostingLists=" + sstableMissingValuePostingLists);
                    PostingList finalSSTablePostingList = MergePostingList.merge(temp);

                    PostingList.PeekablePostingList sstablePostingsPeekable = finalSSTablePostingList.peekable();

                    if (sstablePostingsPeekable.peek() != PostingList.END_OF_STREAM)
                    {
                        IndexSearcherContext context2 = new IndexSearcherContext(minKey,
                                                                                 maxKey,
                                                                                 context.context,
                                                                                 sstablePostingsPeekable,
                                                                                 null);

                        builder.add(new PostingListRangeIterator(indexContext, primaryKeyMap, context2));
                    }
                    else
                    {
                        // TODO: close up resources
                    }
                }
            }
            return builder;
        }
        catch (Throwable t)
        {
            // all sstable indexes in view have been referenced, need to clean up when exception is thrown
            FileUtils.closeQuietly(toClose);
            //view.forEach(e -> e.getValue().forEach(SSTableIndex::release));
            throw new RuntimeException(t);
        }
    }

    public IndexFeatureSet getIndexFeatureSet()
    {
        return indexFeatureSet;
    }

    private ClusteringIndexFilter makeFilter(PrimaryKey key)
    {
        if (!indexFeatureSet.isRowAware() || key.hasEmptyClustering())
            return command.clusteringIndexFilter(key.partitionKey());
        else
            return new ClusteringIndexNamesFilter(FBUtilities.singleton(key.clustering(), key.clusteringComparator()), false);

    }

    private static void releaseQuietly(SSTableIndex index)
    {
        try
        {
            index.release();
        }
        catch (Throwable e)
        {
            logger.error(index.getIndexContext().logMessage("Failed to release index on SSTable {}"), index.getSSTable().descriptor, e);
        }
    }

    /**
     * Used to release all resources and record metrics when query finishes.
     */
    public void finish()
    {
        if (tableQueryMetrics != null) tableQueryMetrics.record(queryContext);
    }

    /**
     * Try to reference all SSTableIndexes before querying on disk indexes.
     *
     * If we attempt to proceed into {@link TermIterator#build(Expression, Set, AbstractBounds, QueryContext)}
     * without first referencing all indexes, a concurrent compaction may decrement one or more of their backing
     * SSTable {@link Ref} instances. This will allow the {@link SSTableIndex} itself to be released and will fail the query.
     */
    private Map<Expression, NavigableSet<SSTableIndex>> referenceAndGetView(Operation.OperationType op, Collection<Expression> expressions)
    {
        SortedSet<String> indexNames = new TreeSet<>();
        try
        {
            while (true)
            {
                List<SSTableIndex> referencedIndexes = new ArrayList<>();
                boolean failed = false;

                Map<Expression, NavigableSet<SSTableIndex>> view = getView(op, expressions);

                for (SSTableIndex index : view.values().stream().flatMap(Collection::stream).collect(Collectors.toList()))
                {
                    indexNames.add(index.getIndexContext().getIndexName());

                    if (index.reference())
                    {
                        referencedIndexes.add(index);
                    }
                    else
                    {
                        failed = true;
                        break;
                    }
                }

                if (failed)
                {
                    // TODO: This might be a good candidate for a table/index group metric in the future...
                    referencedIndexes.forEach(QueryController::releaseQuietly);
                }
                else
                {
                    return view;
                }
            }
        }
        finally
        {
            Tracing.trace("Querying storage-attached indexes {}", indexNames);
        }
    }

    private Map<Expression, NavigableSet<SSTableIndex>> getView(Operation.OperationType op, Collection<Expression> expressions)
    {
        // first let's determine the primary expression if op is AND
        Pair<Expression, NavigableSet<SSTableIndex>> primary = (op == Operation.OperationType.AND) ? calculatePrimary(expressions) : null;

        Map<Expression, NavigableSet<SSTableIndex>> indexes = new HashMap<>();
        for (Expression e : expressions)
        {
            // NO_EQ and non-index column query should only act as FILTER BY for satisfiedBy(Row) method
            // because otherwise it likely to go through the whole index.
            if (!e.context.isIndexed() || e.getOp() == Expression.Op.NOT_EQ)
            {
                continue;
            }

            // primary expression, we'll have to add as is
            if (primary != null && e.equals(primary.left))
            {
                indexes.put(primary.left, primary.right);

                continue;
            }

            View view = e.context.getView();

            NavigableSet<SSTableIndex> readers = new TreeSet<>(SSTableIndex.COMPARATOR);
            if (primary != null && primary.right.size() > 0)
            {
                for (SSTableIndex index : primary.right)
                    readers.addAll(view.match(index.minKey(), index.maxKey()));
            }
            else
            {
                readers.addAll(applyScope(view.match(e)));
            }

            indexes.put(e, readers);
        }

        return indexes;
    }

    private Pair<Expression, NavigableSet<SSTableIndex>> calculatePrimary(Collection<Expression> expressions)
    {
        Expression expression = null;
        NavigableSet<SSTableIndex> primaryIndexes = null;

        for (Expression e : expressions)
        {
            if (!e.context.isIndexed())
                continue;

            View view = e.context.getView();

            NavigableSet<SSTableIndex> indexes = new TreeSet<>(SSTableIndex.COMPARATOR);
            indexes.addAll(applyScope(view.match(e)));

            if (expression == null || primaryIndexes.size() > indexes.size())
            {
                primaryIndexes = indexes;
                expression = e;
            }
        }

        return expression == null ? null : Pair.create(expression, primaryIndexes);
    }

    private Set<SSTableIndex> applyScope(Set<SSTableIndex> indexes)
    {
        return Sets.filter(indexes, index -> {
            SSTableReader sstable = index.getSSTable();

            return mergeRange.left.compareTo(sstable.last) <= 0 && (mergeRange.right.isMinimum() || sstable.first.compareTo(mergeRange.right) <= 0);
        });
    }

    /**
     * Returns the {@link DataRange} list covered by the specified {@link ReadCommand}.
     *
     * @param command a read command
     * @return the data ranges covered by {@code command}
     */
    private static List<DataRange> dataRanges(ReadCommand command)
    {
        if (command instanceof SinglePartitionReadCommand)
        {
            SinglePartitionReadCommand cmd = (SinglePartitionReadCommand) command;
            DecoratedKey key = cmd.partitionKey();
            return Lists.newArrayList(new DataRange(new Range<>(key, key), cmd.clusteringIndexFilter()));
        }
        else if (command instanceof PartitionRangeReadCommand)
        {
            PartitionRangeReadCommand cmd = (PartitionRangeReadCommand) command;
            return Lists.newArrayList(cmd.dataRange());
        }
        else if (command instanceof MultiRangeReadCommand)
        {
            MultiRangeReadCommand cmd = (MultiRangeReadCommand) command;
            return cmd.ranges();
        }
        else
        {
            throw new AssertionError("Unsupported read command type: " + command.getClass().getName());
        }
    }
}
