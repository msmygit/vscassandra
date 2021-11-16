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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import io.netty.handler.ipfilter.IpFilterRule;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifier;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * The primary key of a row, composed by the partition key and the clustering key.
 */
public interface PrimaryKey extends Comparable<PrimaryKey>
{
    final ClusteringComparator EMPTY_COMPARATOR = new ClusteringComparator();

    interface PrimaryKeyFactory
    {
        PrimaryKey createKey(Token token);

        default PrimaryKey createKey(DecoratedKey partitionKey)
        {
            return createKey(partitionKey, Clustering.EMPTY);
        }

        PrimaryKey createKey(DecoratedKey partitionKey, Clustering clustering);

        default PrimaryKey createKey(ByteSource.Peekable peekable)
        {
            throw new UnsupportedOperationException();
        }
    }

    static PrimaryKeyFactory factory(IPartitioner partitioner, ClusteringComparator clusteringComparator, IndexFeatureSet indexFeatureSet)
    {
        return indexFeatureSet.isRowAware() ? new RowAwarePrimaryKeyFactory(partitioner, clusteringComparator)
                                            : new TokenAwarePrimaryKeyFactory();
    }

    Token token();

    DecoratedKey partitionKey();

    Clustering clustering();

    default boolean hasEmptyClustering()
    {
        return clustering() == null || clustering().isEmpty();
    }

    ClusteringComparator clusteringComparator();

    PrimaryKey withSSTableRowId(long sstableRowId);

    PrimaryKey withPrimaryKeySupplier(Supplier<PrimaryKey> primaryKeySupplier);

    PrimaryKey withGeneration(SSTableUniqueIdentifier generation);

    PrimaryKey loadDeferred();

    long sstableRowId();

    /**
     * Returns the {@link PrimaryKey} as a {@link ByteSource} byte comparable representation.
     *
     * It is important that these representations are only ever used with byte comparables using
     * the same elements. This means that {@code asComparableBytes} responses can only be used
     * together from the same {@link PrimaryKey} implementation.
     *
     * @param version the {@link ByteComparable.Version} to use for the implementation
     * @return the {@code ByteSource} byte comparable.
     */
    ByteSource asComparableBytes(ByteComparable.Version version);
}
