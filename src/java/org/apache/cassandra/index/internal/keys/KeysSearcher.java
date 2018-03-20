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
package org.apache.cassandra.index.internal.keys;

import java.nio.ByteBuffer;

import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowableUnfilteredPartition;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.index.internal.CassandraIndex;
import org.apache.cassandra.index.internal.CassandraIndexSearcher;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.flow.Flow;

public class KeysSearcher extends CassandraIndexSearcher
{
    public KeysSearcher(ReadCommand command,
                        RowFilter.Expression expression,
                        CassandraIndex indexer)
    {
        super(command, expression, indexer);
    }

    protected Flow<FlowableUnfilteredPartition> queryDataFromIndex(final DecoratedKey indexKey,
                                                                   final FlowablePartition indexHits,
                                                                   final ReadCommand command,
                                                                   final ReadExecutionController executionController)
    {
        assert indexHits.staticRow() == Rows.EMPTY_STATIC_ROW;
        return indexHits.content()
                        .flatMap(hit ->
               {
                   DecoratedKey key = index.baseCfs.decorateKey(hit.clustering().get(0));
                   if (!command.selectsKey(key))
                       return Flow.<FlowableUnfilteredPartition>empty();

                   ColumnFilter extendedFilter = getExtendedFilter(command.columnFilter());
                   SinglePartitionReadCommand dataCmd = SinglePartitionReadCommand.create(index.baseCfs.metadata(),
                                                                                          command.nowInSec(),
                                                                                          extendedFilter,
                                                                                          command.rowFilter(),
                                                                                          DataLimits.NONE,
                                                                                          key,
                                                                                          command.clusteringIndexFilter(key),
                                                                                          TPCTaskType.READ_SECONDARY_INDEX);

                   Flow<FlowableUnfilteredPartition> partition = dataCmd.queryStorage(index.baseCfs, executionController); // one or less
                   return partition.skippingMap(p -> filterIfStale(p,
                                                                   hit,
                                                                   indexKey.getKey(),
                                                                   executionController.writeOpOrderGroup(),
                                                                   command.nowInSec()));
               });
    }

    private ColumnFilter getExtendedFilter(ColumnFilter initialFilter)
    {
        if (command.columnFilter().fetches(index.getIndexedColumn()))
            return initialFilter;

        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        builder.addAll(initialFilter.fetchedColumns());
        builder.add(index.getIndexedColumn());
        return builder.build();
    }

    private FlowableUnfilteredPartition filterIfStale(FlowableUnfilteredPartition partition,
                                                      Row indexHit,
                                                      ByteBuffer indexedValue,
                                                      OpOrder.Group writeOp,
                                                      int nowInSec)
    throws Exception
    {
        Row data = partition.staticRow();
        if (!index.isStale(data, indexedValue, nowInSec))
            return partition;

        // Index is stale, remove the index entry and ignore
        index.deleteStaleEntry(index.getIndexCfs().decorateKey(indexedValue),
                               makeIndexClustering(partition.header().partitionKey.getKey(), Clustering.EMPTY),
                               new DeletionTime(indexHit.primaryKeyLivenessInfo().timestamp(), nowInSec),
                               writeOp)
             .subscribe();    // We don't need to wait for the deletion to complete, and we don't care too much if it fails.

        return null;
    }
}
