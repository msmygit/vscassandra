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

import java.util.Objects;
import java.util.function.Supplier;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class TokenAwarePrimaryKeyFactory implements PrimaryKey.PrimaryKeyFactory
{
    @Override
    public PrimaryKey createKey(Token token)
    {
        assert token != null;
        return new TokenAwarePrimaryKey(token, null);
    }

    @Override
    public PrimaryKey createKey(DecoratedKey partitionKey, Clustering clustering)
    {
        assert partitionKey != null;
        return new TokenAwarePrimaryKey(partitionKey.getToken(), partitionKey);
    }

    private class TokenAwarePrimaryKey implements PrimaryKey
    {
        private final Token token;
        private DecoratedKey partitionKey;
        private long sstableRowId = -1;
        private Supplier<PrimaryKey> primaryKeySupplier;

        private TokenAwarePrimaryKey(Token token, DecoratedKey partitionKey)
        {
            this.token = token;
            this.partitionKey = partitionKey;
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
            if (primaryKeySupplier != null && partitionKey == null)
                partitionKey = primaryKeySupplier.get().partitionKey();
            return this;
        }

        @Override
        public Token token()
        {
            return this.token;
        }

        @Override
        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        @Override
        public Clustering clustering()
        {
            return Clustering.EMPTY;
        }

        @Override
        public ClusteringComparator clusteringComparator()
        {
            return PrimaryKey.EMPTY_COMPARATOR;
        }

        @Override
        public long sstableRowId()
        {
            return sstableRowId;
        }

        @Override
        public ByteSource asComparableBytes(ByteComparable.Version version)
        {
            ByteSource tokenComparable = token.asComparableBytes(version);
            ByteSource keyComparable = ByteSource.of(partitionKey.getKey(), version);
            return ByteSource.withTerminator(version == ByteComparable.Version.LEGACY
                                             ? ByteSource.END_OF_STREAM
                                             : ByteSource.TERMINATOR,
                                             tokenComparable,
                                             keyComparable,
                                             null);

        }

        @Override
        public int compareTo(PrimaryKey o)
        {
            if (partitionKey == null || o.partitionKey() == null)
                return token().compareTo(o.token());
            return partitionKey.compareTo(o.partitionKey());
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(token);
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
            return String.format("TokenAwarePrimaryKey: { token : %s } ", token);
        }
    }
}
