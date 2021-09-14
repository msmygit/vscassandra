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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.utils.Iterators;
import org.apache.cassandra.utils.SearchIterator;

/**
 * {@link PartitionData} implementation backed by an array of rows.
 */
class ArrayBackedPartitionData extends PartitionData
{
    static final Row[] EMPTY_ROWS = new Row[0];

    static final ArrayBackedPartitionData EMPTY = new ArrayBackedPartitionData(RegularAndStaticColumns.NONE,
                                                                               Rows.EMPTY_STATIC_ROW,
                                                                               EMPTY_ROWS,
                                                                               0,
                                                                               DeletionInfo.LIVE,
                                                                               EncodingStats.NO_STATS);

    final Row[] rows;
    final int rowCount;

    ArrayBackedPartitionData(RegularAndStaticColumns columns,
                             Row staticRow,
                             Row[] rows,
                             int rowCount,
                             DeletionInfo deletionInfo,
                             EncodingStats stats)
    {
        super(columns, staticRow, deletionInfo, stats);
        assert rowCount >= 0;
        this.rows = rows;
        this.rowCount = rowCount;
    }

    /**
     * Builds a new {@link ArrayBackedPartitionData} from the data of an {@link UnfilteredRowIterator}.
     *
     * @param iterator the iterator to build the data from.
     * @param initialCapacity sizing hint to use when creating the array storing the data; this can save resizing.
     * @return the created data.
     */
    static ArrayBackedPartitionData build(UnfilteredRowIterator iterator, int initialCapacity)
    {
        return build(iterator, initialCapacity, false);
    }

    /**
     * Builds a new {@link ArrayBackedPartitionData} from the data of an {@link UnfilteredRowIterator}.
     *
     * @param iterator the iterator to build the data from.
     * @param initialCapacity sizing hint to use when creating the array storing the data; this can save resizing.
     * @param rebuildEncodingStats if {@code true}, the {@link EncodingStats} of the returned data will be rebuilt from
     *                             the actual data including in the partition, and will be "exact". Otherwise, the stats
     *                             of the returned data will be those from {@code iterator} (which may or may not be
     *                             completely exact, depending on where the data comes from).
     * @return the created data.
     */
    static ArrayBackedPartitionData build(UnfilteredRowIterator iterator,
                                          int initialCapacity,
                                          boolean rebuildEncodingStats)
    {
        return PartitionData.build(iterator,
                                   new ArrayDataBuilder(initialCapacity).rebuildEncodingStats(rebuildEncodingStats));
    }

    @Override
    boolean hasRows()
    {
        return rowCount != 0;
    }

    @Override
    int rowCount()
    {
        return rowCount;
    }

    @Override
    Row lastRow()
    {
        return rowCount == 0 ? null : rows[rowCount - 1];
    }

    @Override
    Iterator<Row> rowIterator()
    {
        return Iterators.forArray(rows, 0, rowCount);
    }

    @Override
    SearchIterator<Clustering<?>, Row> searchRows(ClusteringComparator comparator, boolean reversed)
    {
        return new ArrayBackedSearchIterator(comparator, reversed);
    }

    @Override
    Iterator<Row> sliceRows(ClusteringComparator comparator, Slice slice, boolean reversed)
    {
        int startAt = slice.start() == ClusteringBound.BOTTOM
                      ? 0
                      : Arrays.binarySearch(rows, 0, rowCount, slice.start(), comparator);
        if (startAt < 0)
            startAt = -startAt - 1;

        int endAt = slice.end() == ClusteringBound.TOP
                    ? rowCount
                    : Arrays.binarySearch(rows, startAt, rowCount, slice.end(), comparator);
        if (endAt < 0)
            endAt = -endAt - 1;

        if (endAt == 0 || endAt < startAt)
            return Collections.emptyIterator();

        return Iterators.forArray(rows, startAt, endAt, reversed);
    }

    private class ArrayBackedSearchIterator implements SearchIterator<Clustering<?>, Row>
    {
        private final ClusteringComparator comparator;
        private final boolean reversed;
        private int index;

        ArrayBackedSearchIterator(ClusteringComparator comparator, boolean reversed)
        {
            this.comparator = comparator;
            this.reversed = reversed;
            this.index = reversed ? rowCount() : 0;
        }

        // Perform a binary search of the data
        public Row next(Clustering key)
        {
            int start = reversed ? 0 : index;
            int end = reversed ? index : rowCount;

            index = Arrays.binarySearch(rows, start, end, key, comparator);
            if (index < 0)
            {
                // If the key is not found, we still set the index to the first element greater than the one searched,
                // as there will be no point searching before that in followup calls (that by definition of
                // SearchIterator should only request clustering greater than the current key)
                index = -index - 1;
                return null;
            }

            return rows[index++];
        }
    }

    protected static class ArrayDataBuilder extends PartitionData.Builder<ArrayBackedPartitionData>
    {
        protected Row[] rows;
        protected int rowCount;

        ArrayDataBuilder(int initialCapacity)
        {
            this.rows = new Row[initialCapacity];
        }

        @Override
        public ArrayDataBuilder add(Row row)
        {
            if (rowCount == rows.length)
                rows = Arrays.copyOf(rows, Math.max(8, rowCount * 2));
            rows[rowCount++] = row;
            return this;
        }

        @Override
        public boolean hasRows()
        {
            return rowCount > 0;
        }

        @Override
        public ArrayBackedPartitionData build(RegularAndStaticColumns columns, Row staticRow, DeletionInfo deletionInfo, EncodingStats stats)
        {
            if (reversed)
                reverseRows();

            if (rebuildEncodingStats)
                stats = EncodingStats.Collector.collect(staticRow, Iterators.forArray(rows, 0, rowCount), deletionInfo);

            return new ArrayBackedPartitionData(columns, staticRow, rows, rowCount, deletionInfo, stats);
        }

        private void reverseRows()
        {
            if (rowCount == 0)
                return;

            Row tmp;
            for (int i = 0; i < rowCount / 2; i++)
            {
                tmp = rows[i];
                int swapIdx = rowCount - 1 - i;
                rows[i] = rows[swapIdx];
                rows[swapIdx] = tmp;
            }
        }
    }
}
