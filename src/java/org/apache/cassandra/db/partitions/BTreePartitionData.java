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
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.cassandra.db.Clusterable;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;

import static org.apache.cassandra.utils.btree.BTree.Dir.desc;

/**
 * Holder of the content of a partition, see AbstractBTreePartition.
 * When updating a partition one holder is swapped for another atomically.
 */
public final class BTreePartitionData extends PartitionData
{
    public static final BTreePartitionData EMPTY = new BTreePartitionData(RegularAndStaticColumns.NONE,
                                                                          Rows.EMPTY_STATIC_ROW,
                                                                          BTree.empty(),
                                                                          DeletionInfo.LIVE,
                                                                          EncodingStats.NO_STATS);
    public static final long UNSHARED_HEAP_SIZE = ObjectSizes.measure(EMPTY);

    private final Object[] rows;

    BTreePartitionData(RegularAndStaticColumns columns,
                       Row staticRow,
                       Object[] rows,
                       DeletionInfo deletionInfo,
                       EncodingStats stats)
    {
        super(columns, staticRow, deletionInfo, stats);
        this.rows = rows;
    }

    /**
     * Builds a new {@link BTreePartitionData} from the data of an {@link UnfilteredRowIterator}.
     *
     * @param iterator the iterator to build the data from.
     * @param ordered whether the data coming from {@code iterator} is sorted (NOTE: by definition,
     *                {@link UnfilteredRowIterator} should always be sorted/in clustering order so it kind of a "bug"
     *                when this is not the case. However, this method is used by scrub, whose job it is in a way to
     *                detect that kind of issue and correct it).
     * @return the created data.
     */
    static BTreePartitionData build(UnfilteredRowIterator iterator, boolean ordered)
    {
        return PartitionData.build(iterator, new BTreeDataBuilder(iterator.metadata(), ordered));
    }

    /**
     * Builds a new {@link BTreePartitionData} from the data of a {@link RowIterator}.
     *
     * @param iterator the iterator to build the data from.
     * @return the created data.
     */
    static BTreePartitionData build(RowIterator iterator)
    {
        return PartitionData.build(iterator, new BTreeDataBuilder(iterator.metadata()));
    }

    @Override
    boolean hasRows()
    {
        return !BTree.isEmpty(rows);
    }

    @Override
    int rowCount()
    {
        return BTree.size(rows);
    }

    @Override
    Row lastRow()
    {
        return BTree.isEmpty(rows)
               ? null
               : BTree.findByIndex(rows, BTree.size(rows) - 1);
    }

    @Override
    Iterator<Row> rowIterator()
    {
        return BTree.iterator(rows);
    }

    @Override
    SearchIterator<Clustering<?>, Row> searchRows(ClusteringComparator comparator, boolean reversed)
    {
        return BTree.slice(rows, comparator, desc(reversed));
    }

    @Override
    Iterator<Row> sliceRows(ClusteringComparator comparator, Slice slice, boolean reversed)
    {
        ClusteringBound start = slice.start() == ClusteringBound.BOTTOM ? null : slice.start();
        ClusteringBound end = slice.end() == ClusteringBound.TOP ? null : slice.end();
        return BTree.slice(rows, comparator, start, true, end, true, desc(reversed));
    }

    public EncodingStats stats()
    {
        return stats;
    }

    /**
     * Creates a new {@link BTreePartitionData} composed of this data plus the added data provided to this method call.
     *
     * @param update the updates to add to this object.
     * @param deletionFct function called on the previous and existing deletion info (in that order) to compute the
     *                    final deletion returned.
     * @param rowFct update function to use when applying the newly added rows to the existing ones.
     * @param allocatedFct function called to update the heap allocation
     * @return a newly created {@link BTreePartitionData} object containing both the data of this object, and data
     * passed as argument.
     */
    BTreePartitionData add(PartitionUpdate update,
                           BiFunction<DeletionInfo, DeletionInfo, DeletionInfo> deletionFct,
                           UpdateFunction<Row, Row> rowFct,
                           Consumer<Long> allocatedFct)
    {
        RegularAndStaticColumns newColumns = update.columns().mergeTo(columns);
        allocatedFct.accept(newColumns.unsharedHeapSize() - columns.unsharedHeapSize());
        Row newStaticRow = staticRow;
        if (!update.staticRow().isEmpty())
        {
            newStaticRow = staticRow.isEmpty()
            ? rowFct.apply(update.staticRow())
                           : rowFct.apply(staticRow, update.staticRow());
        }

        Object[] newRows = BTree.update(rows, update.metadata().comparator, update, update.rowCount(), rowFct);
        DeletionInfo newDeletionInfo = deletionFct.apply(deletionInfo, update.deletionInfo());
        EncodingStats newStats = stats.mergeWith(update.stats());
        allocatedFct.accept(newStats.unsharedHeapSize() - stats.unsharedHeapSize());
        return new BTreePartitionData(newColumns, newStaticRow, newRows, newDeletionInfo, newStats);
    }

    private static class BTreeDataBuilder extends PartitionData.Builder<BTreePartitionData>
    {
        private final BTree.Builder<Clusterable> btreeBuilder;

        private BTreeDataBuilder(TableMetadata metadata)
        {
            this(metadata, true);
        }

        private BTreeDataBuilder(TableMetadata metadata, boolean ordered)
        {
            this.btreeBuilder = BTree.builder(metadata.comparator).auto(!ordered);
        }


        @Override
        public BTreeDataBuilder add(Row row)
        {
            btreeBuilder.add(row);
            return this;
        }

        @Override
        public boolean hasRows()
        {
            return !btreeBuilder.isEmpty();
        }

        @Override
        public BTreePartitionData build(RegularAndStaticColumns columns,
                                        Row staticRow,
                                        DeletionInfo deletionInfo,
                                        EncodingStats stats)
        {
            if (reversed)
                btreeBuilder.reverse();

            Object[] btree = btreeBuilder.build();
            if (rebuildEncodingStats)
                stats = EncodingStats.Collector.collect(staticRow, BTree.iterator(btree), deletionInfo);

            return new BTreePartitionData(columns, staticRow, btree, deletionInfo, stats);
        }
    }
}
