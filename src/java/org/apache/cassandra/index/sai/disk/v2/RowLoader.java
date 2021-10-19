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

package org.apache.cassandra.index.sai.disk.v2;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ClusteringIndexNamesFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class RowLoader
{
    public static UnfilteredRowIterator loadRow(ColumnFamilyStore cfs, PrimaryKey primaryKey)
    {
        return null;
//        SinglePartitionReadCommand partition = SinglePartitionReadCommand.create(cfs.metadata(),
//                                                                                 command.nowInSec(),
//                                                                                 command.columnFilter(),
//                                                                                 RowFilter.NONE,
//                                                                                 DataLimits.NONE,
//                                                                                 primaryKey.partitionKey(),
//                                                                                 makeFilter(primaryKey));
//        ReadExecutionController executionController = partition.executionController();
//        return partition.queryMemtableAndDisk(cfs, executionController);
    }

    private static ClusteringIndexFilter makeFilter(PrimaryKey key)
    {
        return new ClusteringIndexNamesFilter(FBUtilities.singleton(key.clustering(), key.clusteringComparator()), false);
    }
}
