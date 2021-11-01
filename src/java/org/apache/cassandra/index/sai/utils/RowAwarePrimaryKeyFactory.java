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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class RowAwarePrimaryKeyFactory implements PrimaryKey.PrimaryKeyFactory
{
    private final IPartitioner partitioner;
    private final ClusteringComparator clusteringComparator;

    public RowAwarePrimaryKeyFactory(IPartitioner partitioner, ClusteringComparator clusteringComparator)
    {
        this.partitioner = partitioner;
        this.clusteringComparator = clusteringComparator;
    }

    @Override
    public PrimaryKey createKey(Token token)
    {
        return new RowAwarePrimaryKey(token, null, null);
    }

    @Override
    public PrimaryKey createKey(DecoratedKey partitionKey, Clustering clustering)
    {
        return new RowAwarePrimaryKey(partitionKey.getToken(), partitionKey, clustering);
    }

    @Override
    public PrimaryKey createKey(ByteSource.Peekable peekable)
    {
        Token token = partitioner.getTokenFactory().fromComparableBytes(ByteSourceInverse.nextComponentSource(peekable), ByteComparable.Version.OSS41);
        byte[] keyBytes = ByteSourceInverse.getUnescapedBytes(ByteSourceInverse.nextComponentSource(peekable));

        DecoratedKey key =  new BufferDecoratedKey(token, ByteBuffer.wrap(keyBytes));

        Clustering clustering = clusteringComparator.size() == 0 ? Clustering.EMPTY
                                                                 : clusteringComparator.clusteringFromByteComparable(ByteBufferAccessor.instance, v -> peekable);

        return new RowAwarePrimaryKey(token, key, clustering);
    }

    private class RowAwarePrimaryKey implements PrimaryKey
    {
        private final Token token;
        private DecoratedKey partitionKey;
        private Clustering clustering;
        private long sstableRowId = -1;
        private Supplier<PrimaryKey> primaryKeySupplier;

        private RowAwarePrimaryKey(Token token, DecoratedKey partitionKey, Clustering clustering)
        {
            this.token = token;
            this.partitionKey = partitionKey;
            this.clustering = clustering;
        }

        @Override
        public Token token()
        {
            return token;
        }

        @Override
        public DecoratedKey partitionKey()
        {
            if (partitionKey == null && primaryKeySupplier != null)
            {
                PrimaryKey primaryKey = primaryKeySupplier.get();
                partitionKey = primaryKey.partitionKey();
                clustering = primaryKey.clustering();
            }
            return partitionKey;
        }

        @Override
        public Clustering clustering()
        {
            return clustering;
        }

        @Override
        public ClusteringComparator clusteringComparator()
        {
            return clusteringComparator;
        }

        @Override
        public PrimaryKey withSSTableRowId(long sstableRowId)
        {
            this.sstableRowId = sstableRowId;
            return this;
        }

        @Override
        public PrimaryKey withPrimaryKeySupplier(Supplier<PrimaryKey> primaryKeySupplier)
        {
            this.primaryKeySupplier = primaryKeySupplier;
            return this;
        }

        @Override
        public PrimaryKey loadDeferred()
        {
            return this;
        }

        @Override
        public long sstableRowId()
        {
            return sstableRowId;
        }

        @Override
        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            assert partitionKey != null && clustering != null;
            ByteSource tokenComparable = token.asComparableBytes(version);
            ByteSource keyComparable = ByteSource.of(partitionKey.getKey(), version);
            ByteSource clusteringComparable = clusteringComparator.size() == 0 ||
                                              clustering.isEmpty() ? null
                                                                   : clusteringComparator.asByteComparable(clustering)
                                                                                         .asComparableBytes(version);
            return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY
                                             ? ByteSource.END_OF_STREAM
                                             : ByteSource.TERMINATOR,
                                             tokenComparable,
                                             keyComparable,
                                             clusteringComparable);
        }

        @Override
        public int compareTo(PrimaryKey o)
        {
            int cmp = token().compareTo(o.token());
            if (cmp != 0 || partitionKey() == null || o.partitionKey() == null)
                return cmp;
            cmp = partitionKey().compareTo(o.partitionKey());
            if (cmp != 0 ||
                clusteringComparator().size() == 0 ||
                !clusteringComparator().equals(o.clusteringComparator()) ||
                clustering().isEmpty() ||
                o.clustering().isEmpty())
                return cmp;
            return clusteringComparator().compare(clustering(), o.clustering());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(token, partitionKey, clustering, clusteringComparator);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof PrimaryKey)
                return compareTo((PrimaryKey)obj) == 0;
            return false;
        }

        @Override
        public String toString()
        {
            return String.format("RowAwarePrimaryKey: { token : %s, partition : %s, clustering: %s:%s} ",
                                 token,
                                 partitionKey,
                                 clustering == null ? null : clustering.kind(),
                                 clustering == null ? null :String.join(",", Arrays.stream(clustering.getBufferArray())
                                                                                   .map(ByteBufferUtil::bytesToHex)
                                                                                   .collect(Collectors.toList())));
        }
    }
}
