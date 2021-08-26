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

package org.apache.cassandra.db.partitions;

import java.util.Iterator;
import java.util.NavigableSet;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.AbstractUnfilteredRowIterator;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowAndDeletionMergeIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.SearchIterator;

/**
 * Abstract class to help creating {@link Partition} implementations.
 * <p>
 * In practice, this is relatively shallow wrapper over a {@link PartitionData} implementation (it mostly adds the
 * partition key and access to the table metadata).
 * <p>
 * Subclasses of {@link AbstractPartition} will usually be immutable if they use a single final {@link PartitionData}
 * (that is, if {@link #data()} always return the same object).
 *
 * @param <DATA> the implementation of {@link PartitionData} used.
 */
abstract class AbstractPartition<DATA extends PartitionData> implements Partition, Iterable<Row>
{
    protected final DecoratedKey partitionKey;

    AbstractPartition(DecoratedKey partitionKey)
    {
        this.partitionKey = partitionKey;
    }

    /**
     * The data for the partition this represent.
     */
    protected abstract DATA data();

    /**
     * Must return {@code true} if the data of {@code #data()} can contain shadowed data (for instance, rows whose
     * content is entirely deleted by a range tombstone, cells deleted by their own row deletion, ...).
     * <p>
     * By definition, iterator or flows like the ones returned by {@link #unfilteredIterator}
     * or {@link #unfilteredPartition} should not return shadowed data, so if {@link #data()} contains some, it needs
     * to be filtered out by these methods, and that is what having this method returning {@code true} triggers.
     * <p>
     * This method can typically safely return {@code false} if the underlying {@link #data()} is built from a
     * {@link UnfilteredRowIterator}, {@link FlowableUnfilteredPartition} or similar as those are guaranteed to not
     * contain shadowed data. But otherwise, if no special work is done while creating the data to ensure there is
     * no shadowed data (like, for instance, is the case of a {@link PartitionUpdate}, or for partitions within
     * {@link Memtable} at the time of this writing), then this must return {@code true}.
     */
    protected abstract boolean canHaveShadowedData();

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionInfo deletionInfo()
    {
        return data().deletionInfo;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return deletionInfo().getPartitionDeletion();
    }

    public RegularAndStaticColumns columns()
    {
        return data().columns;
    }

    public EncodingStats stats()
    {
        return data().stats;
    }

    public Row staticRow()
    {
        return data().staticRow;
    }

    public boolean isEmpty()
    {
        DATA data = data();
        return data.deletionInfo.isLive() && !hasRows() && data.staticRow.isEmpty();
    }

    public boolean hasRows()
    {
        return data().hasRows();
    }

    public int rowCount()
    {
        return data().rowCount();
    }

    public Row getRow(Clustering clustering)
    {
        ColumnFilter columns = ColumnFilter.selection(columns());
        final DATA data = data();

        if (clustering == Clustering.STATIC_CLUSTERING)
        {
            Row staticRow = staticRow(data, columns, true);
            // Note that for statics, this will never return null, this will return an empty row. However,
            // it's more consistent for this method to return null if we don't really have a static row.
            return staticRow.isEmpty() ? null : staticRow;
        }

        final Row row = data.searchRows(metadata().comparator, false).next(clustering);
        final DeletionTime partitionDeletion = data.deletionInfo.getPartitionDeletion();

        RangeTombstone rt = data.deletionInfo.rangeCovering(clustering);

        // A search iterator only return a row, so it doesn't allow to directly account for deletion that should apply to row
        // (the partition deletion or the deletion of a range tombstone that covers it). So if needs be, reuse the row deletion
        // to carry the proper deletion on the row.
        DeletionTime activeDeletion = partitionDeletion;
        if (rt != null && rt.deletionTime().supersedes(activeDeletion))
            activeDeletion = rt.deletionTime();

        if (row == null)
        {
            return activeDeletion.isLive()
                   ? null
                   : BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(activeDeletion));
        }
        return row.filter(columns, activeDeletion, true, metadata());
    }

    public Row lastRow()
    {
        return data().lastRow();
    }

    public Iterator<Row> iterator()
    {
        return data().rowIterator();
    }

    private Row staticRow(DATA data, ColumnFilter columns, boolean setActiveDeletionToRow)
    {
        DeletionTime partitionDeletion = data.deletionInfo.getPartitionDeletion();
        if (columns.fetchedColumns().statics.isEmpty() || (data.staticRow.isEmpty() && partitionDeletion.isLive()))
            return Rows.EMPTY_STATIC_ROW;

        Row row = data.staticRow.filter(columns, partitionDeletion, setActiveDeletionToRow, metadata());
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

//    public FlowableUnfilteredPartition unfilteredPartition(ColumnFilter selection, Slices slices, boolean reversed)
//    {
//        //TODO - we should implement the content flow directly, without the iterator
//        return FlowablePartitions.fromIterator(unfilteredIterator(selection, slices, reversed));
//    }

    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, Slices slices, boolean reversed)
    {
        DATA data = data();
        Row staticRow = staticRow(data, selection, false);
        if (slices.size() == 0)
        {
            DeletionTime partitionDeletion = data.deletionInfo.getPartitionDeletion();
            return UnfilteredRowIterators.noRowsIterator(metadata(), partitionKey(), staticRow, partitionDeletion, reversed);
        }

        return slices.size() == 1
               ? sliceIterator(selection, slices.get(0), reversed, data, staticRow)
               : new SlicesIterator(data, staticRow, selection, slices, reversed);
    }

    private UnfilteredRowIterator sliceIterator(ColumnFilter selection,
                                                Slice slice,
                                                boolean reversed,
                                                DATA data,
                                                Row staticRow)
    {
        if (slice.isEmpty(metadata().comparator))
            return UnfilteredRowIterators.noRowsIterator(metadata(), partitionKey, staticRow, partitionLevelDeletion(), reversed);

        Iterator<Row> rowIter = data.sliceRows(metadata().comparator, slice, reversed);
        Iterator<RangeTombstone> deleteIter = data.deletionInfo.rangeIterator(slice, reversed);
        return merge(rowIter, deleteIter, selection, reversed, data, staticRow);
    }


    public UnfilteredRowIterator unfilteredIterator(ColumnFilter selection, NavigableSet<Clustering> clusteringsInQueryOrder, boolean reversed)
    {
        DATA data = data();
        Row staticRow = staticRow(data, selection, false);
        if (clusteringsInQueryOrder.isEmpty())
        {
            DeletionTime partitionDeletion = data.deletionInfo.getPartitionDeletion();
            return UnfilteredRowIterators.noRowsIterator(metadata(), partitionKey(), staticRow, partitionDeletion, reversed);
        }

        Iterator<Row> rowIter = new AbstractIterator<Row>() {

            Iterator<Clustering> clusterings = clusteringsInQueryOrder.iterator();
            SearchIterator<Clustering, Row> rowSearcher = data.searchRows(metadata().comparator, reversed);

            @Override
            protected Row computeNext()
            {
                while (clusterings.hasNext())
                {
                    Row row = rowSearcher.next(clusterings.next());
                    if (row != null)
                        return row;
                }
                return endOfData();
            }
        };

        // not using DeletionInfo.rangeCovering(Clustering), because it returns the original range tombstone,
        // but we need DeletionInfo.rangeIterator(Set<Clustering>) that generates tombstones based on given clustering bound.
        Iterator<RangeTombstone> deleteIter = data.deletionInfo.rangeIterator(clusteringsInQueryOrder, reversed);

        return merge(rowIter, deleteIter, selection, reversed, data, staticRow);
    }

    private RowAndDeletionMergeIterator merge(Iterator<Row> rowIter,
                                              Iterator<RangeTombstone> deleteIter,
                                              ColumnFilter selection,
                                              boolean reversed,
                                              DATA current,
                                              Row staticRow)
    {
        return new RowAndDeletionMergeIterator(metadata(), partitionKey(), current.deletionInfo.getPartitionDeletion(),
                                               selection, staticRow, reversed, current.stats,
                                               rowIter, deleteIter, canHaveShadowedData());
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(String.format("[%s] key=%s partition_deletion=%s columns=%s",
                                metadata(),
                                metadata().partitionKeyType.getString(partitionKey().getKey()),
                                partitionLevelDeletion(),
                                columns()));

        if (!staticRow().isEmpty())
            sb.append("\n    ").append(staticRow().toString(metadata(), true));

        try (UnfilteredRowIterator iter = unfilteredIterator())
        {
            while (iter.hasNext())
                sb.append("\n    ").append(iter.next().toString(metadata(), true));
        }

        return sb.toString();
    }

    private class SlicesIterator extends AbstractUnfilteredRowIterator
    {
        private final DATA data;
        private final ColumnFilter selection;
        private final Slices slices;

        private int idx;
        private Iterator<Unfiltered> currentSlice;

        private SlicesIterator(DATA data,
                               Row staticRow,
                               ColumnFilter selection,
                               Slices slices,
                               boolean isReversed)
        {
            super(AbstractPartition.this.metadata(),
                  AbstractPartition.this.partitionKey(),
                  data.deletionInfo.getPartitionDeletion(),
                  // Non-selected columns will be filtered in subclasses by RowAndDeletionMergeIterator. It would also
                  // be more precise to return the intersection of the selection and current.columns, but its probably
                  // not worth spending time on computing that.
                  selection.fetchedColumns(),
                  staticRow,
                  isReversed,
                  data.stats);
            this.data = data;
            this.selection = selection;
            this.slices = slices;
        }

        protected Unfiltered computeNext()
        {
            while (true)
            {
                if (currentSlice == null)
                {
                    if (idx >= slices.size())
                        return endOfData();

                    int sliceIdx = isReverseOrder ? slices.size() - idx - 1 : idx;
                    currentSlice = sliceIterator(selection, slices.get(sliceIdx), isReverseOrder, data, Rows.EMPTY_STATIC_ROW);
                    idx++;
                }

                if (currentSlice.hasNext())
                    return currentSlice.next();

                currentSlice = null;
            }
        }
    }
}
