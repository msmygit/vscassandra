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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;

/**
 * Abstract class used to represent data (in-memory) contained within a partition.
 * <p>
 * This class abstracts the actual data structures used for the collection of rows in the partition. It is primarily
 * used by {@link AbstractPartition}. It has 2 main subclasses:
 * <ul>
 *     <li>{@link BTreePartitionData}, which uses a {@link BTree} of rows.</li>
 *     <li>{@link ArrayBackedPartitionData}, which uses a simple array of rows.</li>
 * </ul>
 * <p>
 * Concrete implementations of {@link PartitionData} should be immutable.
 *
 * @see AbstractPartition
 */
abstract class PartitionData
{
    final RegularAndStaticColumns columns;
    final DeletionInfo deletionInfo;
    final Row staticRow;
    final EncodingStats stats;

    protected PartitionData(RegularAndStaticColumns columns,
                            Row staticRow,
                            DeletionInfo deletionInfo,
                            EncodingStats stats)
    {
        assert columns != null && staticRow != null && deletionInfo != null && stats != null;
        this.columns = columns;
        this.deletionInfo = deletionInfo;
        this.staticRow = staticRow;
        this.stats = stats;
    }

    /**
     * Whether the partition this is the data of has any non-static rows.
     */
    abstract boolean hasRows();

    /**
     * The number of non-static rows contained in this partition.
     */
    abstract int rowCount();

    /**
     * The last (in clustering order) non-static rows contained in this partition.
     */
    abstract Row lastRow();

    /**
     * Iterator over the non-static rows of the partition this is the data of.
     */
    abstract Iterator<Row> rowIterator();

   /**
     * Creates a new search iterator over the non-static rows of the partition this is a data of.
     * <p>
     * Note that the returned search iterator does not account for deletion information within this partition that may
     * cover the returned rows (see {@link AbstractPartition#getRow(Clustering)} for a practical consequence of this).
     *
     * @param comparator the clustering comparator for the table this is a partition of.
     * @param reversed whether the returned search iterator should search in reverse clustering order.
     * @return a search iterator for the non-static rows of this partition.
     */
    abstract SearchIterator<Clustering, Row> searchRows(ClusteringComparator comparator, boolean reversed);

    /**
     * Creates an iterator over the non-static rows of the partition this is a data of that are within the provided
     * slice.
     * <p>
     * Note that as for {@link #searchRows}, the returned iterator does not account for deletion information within
     * this partition that may cover the returned rows.
     *
     * @param comparator the clustering comparator for the table this is a partition of.
     * @param slice the slice for which to return rows.
     * @param reversed whether the returned iterator should produce rows in reverse clustering order.
     * @return an iterator returning all the row within this partition that fall within {@code slice}.
     */
    abstract Iterator<Row> sliceRows(ClusteringComparator comparator, Slice slice, boolean reversed);

    /**
     * Returns the heap size of the fields referenced by this object. This does not include the size of this object
     * itself, or the static row size.
     * For usage, see {@link BTreePartitionData#unsharedHeapSizeExcludingRows()}
     */
    protected long unsharedFieldsHeapSize()
    {
        return deletionInfo.unsharedHeapSize()
               + stats.unsharedHeapSize();
    }

    public String toDebugString(TableMetadata metadata)
    {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("(");
        sb.append("\n    columns=").append(columns);
        sb.append("\n    deletion=").append(deletionInfo);
        sb.append("\n    stats=").append(stats);
        if (!staticRow.isEmpty())
            sb.append("\n    static=").append(staticRow.toString(metadata, true));
        if (hasRows())
        {
            sb.append("\n    rows=[");
            for (Row row : (Iterable<Row>)this::rowIterator)
                sb.append("\n      ").append(row.toString(metadata, true));
            sb.append("\n    ]");
        }
        return sb.append(")").toString();
    }

    /**
     * A builder for {@link PartitionData}.
     * <p>
     * Note that as for {@link PartitionData}, this is primarily meant for {@link AbstractPartition} and his subclasses.
     *
     * @param <DATA> the concrete subclass of {@link PartitionData} built.
     */
    static abstract class Builder<DATA extends PartitionData>
    {
        protected boolean reversed;
        protected boolean rebuildEncodingStats;

        /** Adds the provided non-static row to the builder. */
        abstract Builder<DATA> add(Row row);

        /** Whether any non-static row has been added to the builder. */
        abstract boolean hasRows();

        /**
         * Sets the builder to reverse the rows passed to {@link #add(Row)} before building the final data.
         *
         * @param reversed if {@code true}, the rows added should be reversed (put in the reverse order of {@link #add}
         *                 calls.
         *
         * @return this builder.
         */
        Builder<DATA> reversed(boolean reversed)
        {
            this.reversed = reversed;
            return this;
        }

        /**
         * Sets the builder to rebuild the encoding stats based on the data added to the builder.
         *
         * @param rebuildEncodingStats if {@code true}, the builder will rebuild then {@link EncodingStats} used for
         *                             the built data. In that case, the stats passed to {@link #build} will be ignored.
         *
         * @return this builder.
         */
        Builder<DATA> rebuildEncodingStats(boolean rebuildEncodingStats)
        {
            this.rebuildEncodingStats = rebuildEncodingStats;
            return this;
        }

        /**
         * Builds a new {@link PartitionData} using the information provided as argument and rows added through
         * {@link #add}.
         *
         * @param columns columns used by the rows of the partition.
         * @param staticRow the static row of the partition (which can be {@link Rows#EMPTY_STATIC_ROW} but should not
         *                  be {@code null}).
         * @param deletionInfo the deletion info for the partition.
         * @param stats the encoding stats for the partition (which will be ignore if {@link #rebuildEncodingStats}) has
         *              be set to true.
         * @return the built {@link PartitionData}.
         */
        abstract DATA build(RegularAndStaticColumns columns, Row staticRow, DeletionInfo deletionInfo, EncodingStats stats);
    }

    /**
     * Build a {@link PartitionData} containing the data of the provided {@link UnfilteredRowIterator}.
     *
     * @param iterator the iterator containing the partition data to materialize as a {@link PartitionData} object.
     * @param builder the concrete builder to use to build the returned object.
     * @param <DATA> the concrete subclass of {@link PartitionData} built.
     * @return a new {@link PartitionData} containing the data from {@code iterator}.
     */
    static <DATA extends PartitionData> DATA build(UnfilteredRowIterator iterator, Builder<DATA> builder)
    {
        boolean reversed = iterator.isReverseOrder();
        builder.reversed(reversed);
        MutableDeletionInfo.Builder deletionBuilder = MutableDeletionInfo.builder(iterator.partitionLevelDeletion(),
                                                                                  iterator.metadata().comparator,
                                                                                  reversed);

        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            if (unfiltered.kind() == Unfiltered.Kind.ROW)
                builder.add((Row)unfiltered);
            else
                deletionBuilder.add((RangeTombstoneMarker)unfiltered);
        }


        return builder.build(iterator.columns(), iterator.staticRow(), deletionBuilder.build(), iterator.stats());
    }

    /**
     * Build a {@link PartitionData} containing the data of the provided {@link RowIterator}.
     * <p>
     * Please note that as {@link RowIterator} does not expose any {@link EncodingStats}, the returned data will
     * use {@link EncodingStats#NO_STATS} unless {@link Builder#rebuildEncodingStats} has been called on the
     * {@code builder} prior to this call.
     *
     * @param iterator the iterator containing the partition data to materialize as a {@link PartitionData} object.
     * @param builder the concrete builder to use to build the returned object.
     * @param <DATA> the concrete subclass of {@link PartitionData} built.
     * @return a new {@link PartitionData} containing the data from {@code iterator}.
     */
    static <DATA extends PartitionData> DATA build(RowIterator iterator, Builder<DATA> builder)
    {
        builder.reversed(iterator.isReverseOrder());

        Row staticRow = iterator.staticRow();
        while (iterator.hasNext())
            builder.add(iterator.next());

        return builder.build(iterator.columns(),
                             staticRow,
                             DeletionInfo.LIVE,
                             EncodingStats.NO_STATS);
    }
}
