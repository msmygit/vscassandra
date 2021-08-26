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

import java.util.NavigableSet;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;


/**
 * Immutable partition implementation backed by a simple array of rows.
 */
public class ImmutableArrayBackedPartition extends AbstractPartition<ArrayBackedPartitionData>
{
    private static final int INITIAL_ROW_CAPACITY = 16;

    private final TableMetadata metadata;
    private final ArrayBackedPartitionData data;

    protected ImmutableArrayBackedPartition(TableMetadata metadata,
                                            DecoratedKey partitionKey,
                                            ArrayBackedPartitionData data)
    {
        super(partitionKey);
        this.metadata = metadata;
        this.data = data;
    }

    public static ImmutableArrayBackedPartition create(UnfilteredRowIterator iterator)
    {
        return create(iterator, INITIAL_ROW_CAPACITY);
    }

    public static ImmutableArrayBackedPartition create(UnfilteredRowIterator iterator, int initialRowCapacity)
    {
        return new ImmutableArrayBackedPartition(iterator.metadata(),
                                                 iterator.partitionKey(),
                                                 ArrayBackedPartitionData.build(iterator, initialRowCapacity));
    }

//    public static Partition create(Flow<FlowableUnfilteredPartition> partitions)
//    {
//        return create(partitions, INITIAL_ROW_CAPACITY);
//    }

//    public static Flow<Partition> create(Flow<FlowableUnfilteredPartition> partitions, int initialRowCapacity)
//    {
//        return partitions.flatMap(partition -> create(partition, initialRowCapacity));
//    }
//
//    public static Flow<Partition> create(FlowableUnfilteredPartition partition, int initialRowCapacity)
//    {
//        return ArrayBackedPartitionData.build(partition, initialRowCapacity)
//                                       .map(data -> new ImmutableArrayBackedPartition(partition.metadata(),
//                                                                                      partition.partitionKey(),
//                                                                                      data));
//    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    // TODO mfleming
    @Override
    public UnfilteredRowIterator unfilteredIterator()
    {
        return null;
    }

    @Override
    protected ArrayBackedPartitionData data()
    {
        return data;
    }

    @Override
    protected boolean canHaveShadowedData()
    {
        return false;
    }
}
