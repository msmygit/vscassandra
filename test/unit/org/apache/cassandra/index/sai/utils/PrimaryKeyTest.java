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

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@Ignore
public class PrimaryKeyTest
{
    private static TableMetadata simplePartition = TableMetadata.builder("test", "test")
                                                                .partitioner(Murmur3Partitioner.instance)
                                                                .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                .build();

    private static TableMetadata compositePartition = TableMetadata.builder("test", "test")
                                                                   .partitioner(Murmur3Partitioner.instance)
                                                                   .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                   .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                   .build();

    private static TableMetadata simplePartitonSingleClusteringAsc = TableMetadata.builder("test", "test")
                                                                                  .partitioner(Murmur3Partitioner.instance)
                                                                                  .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                  .addClusteringColumn("ck1", UTF8Type.instance)
                                                                                  .build();

    private static TableMetadata simplePartitionMultipleClusteringAsc = TableMetadata.builder("test", "test")
                                                                                     .partitioner(Murmur3Partitioner.instance)
                                                                                     .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                     .addClusteringColumn("ck1", UTF8Type.instance)
                                                                                     .addClusteringColumn("ck2", UTF8Type.instance)
                                                                                     .build();

    private static TableMetadata simplePartitonSingleClusteringDesc = TableMetadata.builder("test", "test")
                                                                                   .partitioner(Murmur3Partitioner.instance)
                                                                                   .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                   .addClusteringColumn("ck1", ReversedType.getInstance(UTF8Type.instance))
                                                                                   .build();

    private static TableMetadata simplePartitionMultipleClusteringDesc = TableMetadata.builder("test", "test")
                                                                                      .partitioner(Murmur3Partitioner.instance)
                                                                                      .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                      .addClusteringColumn("ck1", ReversedType.getInstance(UTF8Type.instance))
                                                                                      .addClusteringColumn("ck2", ReversedType.getInstance(UTF8Type.instance))
                                                                                      .build();

    private static TableMetadata compositePartitonSingleClusteringAsc = TableMetadata.builder("test", "test")
                                                                                     .partitioner(Murmur3Partitioner.instance)
                                                                                     .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                     .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                                     .addClusteringColumn("ck1", UTF8Type.instance)
                                                                                     .build();

    private static TableMetadata compositePartitionMultipleClusteringAsc = TableMetadata.builder("test", "test")
                                                                                        .partitioner(Murmur3Partitioner.instance)
                                                                                        .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                        .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                                        .addClusteringColumn("ck1", UTF8Type.instance)
                                                                                        .addClusteringColumn("ck2", UTF8Type.instance)
                                                                                        .build();

    private static TableMetadata compositePartitonSingleClusteringDesc = TableMetadata.builder("test", "test")
                                                                                     .partitioner(Murmur3Partitioner.instance)
                                                                                     .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                     .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                                     .addClusteringColumn("ck1", ReversedType.getInstance(UTF8Type.instance))
                                                                                     .build();

    private static TableMetadata compositePartitionMultipleClusteringDesc = TableMetadata.builder("test", "test")
                                                                                        .partitioner(Murmur3Partitioner.instance)
                                                                                        .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                        .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                                        .addClusteringColumn("ck1", ReversedType.getInstance(UTF8Type.instance))
                                                                                        .addClusteringColumn("ck2", ReversedType.getInstance(UTF8Type.instance))
                                                                                        .build();

    private static TableMetadata simplePartitionMultipleClusteringMixed = TableMetadata.builder("test", "test")
                                                                                       .partitioner(Murmur3Partitioner.instance)
                                                                                       .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                       .addClusteringColumn("ck1", UTF8Type.instance)
                                                                                       .addClusteringColumn("ck2", ReversedType.getInstance(UTF8Type.instance))
                                                                                       .build();

    private static TableMetadata compositePartitionMultipleClusteringMixed = TableMetadata.builder("test", "test")
                                                                                         .partitioner(Murmur3Partitioner.instance)
                                                                                         .addPartitionKeyColumn("pk1", Int32Type.instance)
                                                                                         .addPartitionKeyColumn("pk2", Int32Type.instance)
                                                                                         .addClusteringColumn("ck1", UTF8Type.instance)
                                                                                         .addClusteringColumn("ck2", ReversedType.getInstance(UTF8Type.instance))
                                                                                         .build();


    @Test
    public void simplePartitionTokenAwareTest() throws Throwable
    {
        PrimaryKey.PrimaryKeyFactory factory = new TokenAwarePrimaryKeyFactory();

        PrimaryKey first = factory.createKey(makeKey(simplePartition, "1").getToken());
        PrimaryKey firstToken = factory.createKey(first.token());
        PrimaryKey second = factory.createKey(makeKey(simplePartition, "2").getToken());
        PrimaryKey secondToken = factory.createKey(second.token());

        assertByteComparable(first, factory);
        assertByteComparable(firstToken, factory);

        assertComparison(first, second, -1);
        assertComparison(second, first, 1);
        assertComparison(first, first, 0);
        assertComparison(first, firstToken, 0);
        assertComparison(firstToken, secondToken, -1);
    }

    @Test
    public void singlePartitionRowAwareTest() throws Throwable
    {
        PrimaryKey.PrimaryKeyFactory factory = new RowAwarePrimaryKeyFactory(simplePartition.partitioner, simplePartition.comparator);

        PrimaryKey first = factory.createKey(makeKey(simplePartition, "1"), Clustering.EMPTY);
        PrimaryKey firstToken = factory.createKey(first.token());
        PrimaryKey second = factory.createKey(makeKey(simplePartition, "2"), Clustering.EMPTY);
        PrimaryKey secondToken = factory.createKey(second.token());

        assertByteComparable(first, factory);
        assertByteComparable(firstToken, factory);

        assertComparison(first, second, -1);
        assertComparison(second, first, 1);
        assertComparison(first, first, 0);
        assertComparison(first, firstToken, 1);
        assertComparison(firstToken, secondToken, -1);
    }

    private void assertByteComparable(PrimaryKey key, PrimaryKey.PrimaryKeyFactory factory)
    {
        assertEquals(key, factory.createKey(ByteSource.peekable(key.asComparableBytes(ByteComparable.Version.OSS41))));
    }

    private void assertComparison(PrimaryKey a, PrimaryKey b, int expected)
    {
        assertEquals(expected, ByteComparable.compare(v -> a.asComparableBytes(v),
                                                      v -> b.asComparableBytes(v),
                                                      ByteComparable.Version.OSS41));

        assertEquals(expected, a.compareTo(b));

        if (expected == 0)
            assertEquals(a, b);
        else
            assertNotEquals(a, b);
    }

    private DecoratedKey makeKey(TableMetadata table, String...partitionKeys)
    {
        ByteBuffer key;
        if (TypeUtil.isComposite(table.partitionKeyType))
            key = ((CompositeType)table.partitionKeyType).decompose(partitionKeys);
        else
            key = table.partitionKeyType.fromString(partitionKeys[0]);
        return table.partitioner.decorateKey(key);
    }

    private Clustering makeClustering(TableMetadata table, String...clusteringKeys)
    {
        Clustering clustering;
        if (table.comparator.size() == 0)
            clustering = Clustering.EMPTY;
        else
        {
            ByteBuffer[] values = new ByteBuffer[clusteringKeys.length];
            for (int index = 0; index < table.comparator.size(); index++)
                values[index] = table.comparator.subtype(index).fromString(clusteringKeys[index]);
            clustering = Clustering.make(values);
        }
        return clustering;
    }
}
