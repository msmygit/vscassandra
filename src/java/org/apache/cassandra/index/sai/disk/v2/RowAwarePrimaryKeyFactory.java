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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.SSTableId;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * A row-aware {@link PrimaryKey.Factory}. This creates {@link PrimaryKey} instances that are
 * sortable by {@link DecoratedKey} and {@link Clustering}.
 */
public class RowAwarePrimaryKeyFactory implements PrimaryKey.Factory
{
    private final ClusteringComparator clusteringComparator;

    public RowAwarePrimaryKeyFactory(ClusteringComparator clusteringComparator)
    {
        this.clusteringComparator = clusteringComparator;
    }

    @Override
    public PrimaryKey createTokenOnly(Token token)
    {
        return new RowAwarePrimaryKey(token, null, null, null, null, 0);
    }

    @Override
    public PrimaryKey createDeferred(Token token, Supplier<PrimaryKey> primaryKeySupplier)
    {
        return new RowAwarePrimaryKey(token, null, null, primaryKeySupplier, null, 0);
    }

    @Override
    public PrimaryKey create(DecoratedKey partitionKey, Clustering clustering)
    {
        return new RowAwarePrimaryKey(partitionKey.getToken(), partitionKey, clustering, null, null, 0);
    }

    @Override
    public PrimaryKey create(DecoratedKey partitionKey, Clustering clustering, SSTableId sstableId, long sstableRowId)
    {
        return new RowAwarePrimaryKey(partitionKey.getToken(), partitionKey, clustering, null, sstableId, sstableRowId);
    }

    private class RowAwarePrimaryKey implements PrimaryKey
    {
        private Token token;
        private DecoratedKey partitionKey;
        private Clustering clustering;
        private Supplier<PrimaryKey> primaryKeySupplier;

        // Only meant for tracking the source of the PrimaryKey. Not meant for comparing keys.
        // A map from sstable id to sstable row id.
        private transient Map<SSTableId<?>, Long> sstableRows = null;

        private RowAwarePrimaryKey(Token token, DecoratedKey partitionKey, Clustering clustering, Supplier<PrimaryKey> primaryKeySupplier, SSTableId<?> sstableId, long sstableRowId)
        {
            this.token = token;
            this.partitionKey = partitionKey;
            this.clustering = clustering;
            this.primaryKeySupplier = primaryKeySupplier;
            if (sstableId != null)
            {
                this.sstableRows = new HashMap<>(1);
                this.sstableRows.put(sstableId, sstableRowId);
            }
        }

        public Map<SSTableId<?>, Long> getSstableRows()
        {
            return sstableRows;
        }

        @Override
        public long sstableRowId(SSTableId<?> ssTableId)
        {
            return sstableRows.getOrDefault(ssTableId, -1L);
        }

        @Override
        public void mergeSSTableMetadata(PrimaryKey other)
        {
            assert other instanceof RowAwarePrimaryKey;
            RowAwarePrimaryKey otherRowAwarePrimaryKey = (RowAwarePrimaryKey) other;
            var otherSstableRows = otherRowAwarePrimaryKey.getSstableRows();
            if (otherSstableRows == null)
                return;
            if (sstableRows == null)
            {
                sstableRows = otherSstableRows;
                return;
            }
            for (var entry : otherSstableRows.entrySet())
            {
                var sstableRowId = entry.getValue();
                Long previousValue = sstableRows.put(entry.getKey(), sstableRowId);
                assert previousValue == null || previousValue.equals(sstableRowId) : String.format("Cannot overwrite sstable row id %d with %d", previousValue, sstableRowId);
            }
        }

        @Override
        public Token token()
        {
            return token;
        }

        @Override
        public DecoratedKey partitionKey()
        {
            loadDeferred();
            return partitionKey;
        }

        @Override
        public Clustering clustering()
        {
            loadDeferred();
            return clustering;
        }

        @Override
        public PrimaryKey loadDeferred()
        {
            if (primaryKeySupplier != null && partitionKey == null)
            {
                PrimaryKey deferredPrimaryKey = primaryKeySupplier.get();
                this.partitionKey = deferredPrimaryKey.partitionKey();
                this.clustering = deferredPrimaryKey.clustering();
                this.mergeSSTableMetadata(deferredPrimaryKey);
                primaryKeySupplier = null;
            }
            return this;
        }

        @Override
        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            return asComparableBytes(version == ByteComparable.Version.LEGACY ? ByteSource.END_OF_STREAM : ByteSource.TERMINATOR, version, false);
        }

        @Override
        public ByteSource asComparableBytesMinPrefix(ByteComparable.Version version)
        {
            return asComparableBytes(ByteSource.LT_NEXT_COMPONENT, version, true);
        }

        @Override
        public ByteSource asComparableBytesMaxPrefix(ByteComparable.Version version)
        {
            return asComparableBytes(ByteSource.GT_NEXT_COMPONENT, version, true);
        }

        private ByteSource asComparableBytes(int terminator, ByteComparable.Version version, boolean isPrefix)
        {
            // We need to make sure that the key is loaded before returning a
            // byte comparable representation. If we don't we won't get a correct
            // comparison because we potentially won't be using the partition key
            // and clustering for the lookup
            loadDeferred();

            ByteSource tokenComparable = token.asComparableBytes(version);
            ByteSource keyComparable = partitionKey == null ? null
                                                            : ByteSource.of(partitionKey.getKey(), version);

            // It is important that the ClusteringComparator.asBytesComparable method is used
            // to maintain the correct clustering sort order
            ByteSource clusteringComparable = clusteringComparator.size() == 0 ||
                                              clustering == null ||
                                              clustering.isEmpty() ? null
                                                                   : clusteringComparator.asByteComparable(clustering)
                                                                                         .asComparableBytes(version);

            // prefix doesn't include null components
            if (isPrefix)
            {
                if (keyComparable == null)
                    return ByteSource.withTerminator(terminator, tokenComparable);
                else if (clusteringComparable == null)
                    return ByteSource.withTerminator(terminator, tokenComparable, keyComparable);
            }
            return ByteSource.withTerminator(terminator, tokenComparable, keyComparable, clusteringComparable);
        }

        @Override
        public int compareTo(PrimaryKey o)
        {
            int cmp = token().compareTo(o.token());

            // If the tokens don't match then we don't need to compare any more of the key.
            // Otherwise if this key has no deferred loader and it's partition key is null
            // or the other partition key is null then one or both of the keys
            // are token only so we can only compare tokens
            if ((cmp != 0) || (primaryKeySupplier == null && partitionKey == null) || o.partitionKey() == null)
                return cmp;

            // Next compare the partition keys. If they are not equal or
            // this is a single row partition key or there are no
            // clusterings then we can return the result of this without
            // needing to compare the clusterings
            cmp = partitionKey().compareTo(o.partitionKey());
            if (cmp != 0 || hasEmptyClustering() || o.hasEmptyClustering())
                return cmp;
            return clusteringComparator.compare(clustering(), o.clustering());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(token(), partitionKey(), clustering());
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
            return String.format("RowAwarePrimaryKey: { token: %s, partition: %s, clustering: %s:%s} ",
                                 token,
                                 partitionKey,
                                 clustering == null ? null : clustering.kind(),
                                 clustering == null ? null :String.join(",", Arrays.stream(clustering.getBufferArray())
                                                                                   .map(ByteBufferUtil::bytesToHex)
                                                                                   .collect(Collectors.toList())));
        }
    }

    public class SSTableRow {
        public final SSTableId ssTableId;
        public final long sstableRowId;
        SSTableRow(SSTableId ssTableId, long sstableRowId) {
            this.ssTableId = ssTableId;
            this.sstableRowId = sstableRowId;
        }
    }
}
