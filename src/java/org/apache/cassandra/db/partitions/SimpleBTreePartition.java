/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

public abstract class SimpleBTreePartition extends AbstractPartition<BTreePartitionData>
{
    protected final TableMetadata metadata;
    protected final BTreePartitionData data;

    protected SimpleBTreePartition(TableMetadata metadata,
                                   DecoratedKey partitionKey,
                                   BTreePartitionData data)
    {
        super(partitionKey);
        this.metadata = metadata;
        this.data = data;
    }

    /**
     * Creates an {@code SimpleBTreePartition} holding all the data of the provided iterator.
     *
     * Warning: Note that this method does not close the provided iterator and it is
     * up to the caller to do so.
     *
     * @param iterator the iterator to gather in memory.
     * @param ordered {@code true} if the iterator will return the rows in order, {@code false} otherwise.
     * @return the created partition.
     */
    public static SimpleBTreePartition create(UnfilteredRowIterator iterator, boolean ordered)
    {
        return new SimpleBTreePartition(iterator.metadata(),
                                        iterator.partitionKey(),
                                        BTreePartitionData.build(iterator, ordered))
        {
            @Override
            protected boolean canHaveShadowedData()
            {
                // We build from an UnfilteredRowIterator, so there shouldn't be shadowed data. That said, that
                // method is a bit abused by scrub to provide possibly non-ordered (and thus broken) iterators,
                // and it's better to not assume anything in that case (it's always safe, if possibly inefficient, to
                // return true here; it's returning false while there is shadowed data that would be problematic).
                return !ordered;
            }
        };
    }

    public static SimpleBTreePartition create(TableMetadata metadata,
                                              DecoratedKey partitionKey,
                                              RegularAndStaticColumns columns,
                                              Row staticRow,
                                              Object[] tree,
                                              DeletionInfo deletionInfo,
                                              EncodingStats stats,
                                              boolean mayHaveShadowedData)
    {
        return new SimpleBTreePartition(metadata,
                                        partitionKey,
                                        new BTreePartitionData(columns, staticRow, tree, deletionInfo, stats))
        {
            @Override
            protected boolean canHaveShadowedData()
            {
                return mayHaveShadowedData;
            }
        };
    }

    @Override
    public TableMetadata metadata()
    {
        return metadata;
    }

    @Override
    protected BTreePartitionData data()
    {
        return data;
    }
}
