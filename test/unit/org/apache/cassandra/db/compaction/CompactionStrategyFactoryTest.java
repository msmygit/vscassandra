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

package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class CompactionStrategyFactoryTest
{
    @Mock
    ColumnFamilyStore cfs;

    @BeforeClass
    public static void setupSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace("local", KeyspaceParams.local(), TableMetadata.builder("local", "table")
                                                                                  .addPartitionKeyColumn("pk", Int32Type.instance));
        SchemaLoader.createKeyspace("simple", KeyspaceParams.simple(1), TableMetadata.builder("simple", "table")
                                                                                     .addPartitionKeyColumn("pk", Int32Type.instance));
    }

    @Before
    public void setupMocks()
    {
        MockitoAnnotations.initMocks(this);
    }

    @After
    public void clearProperty()
    {
        System.clearProperty("cassandra.compaction.remote_data_supplier");
    }

    @Test
    public void realmIsNotWrappedIfPropertyNotSet()
    {
        System.clearProperty("cassandra.compaction.remote_data_supplier");

        when(cfs.metadata()).thenReturn(Schema.instance.getTableMetadata("simple", "table"));
        when(cfs.getKeyspaceName()).thenReturn("simple");

        CompactionStrategyFactory factory = new CompactionStrategyFactory(cfs);

        assertTrue(factory.getRealm() instanceof CompactionRealm);
    }

    @Test
    public void realmIsNotWrappedIfLocalKeyspaceName()
    {
        System.setProperty("cassandra.compaction.remote_data_supplier", TestCompactionRealm.class.getName());

        TableMetadata metadata = TableMetadata.builder("system", "table").addPartitionKeyColumn("pk", Int32Type.instance).build();

        when(cfs.metadata()).thenReturn(metadata);
        when(cfs.getKeyspaceName()).thenReturn("system");

        CompactionStrategyFactory factory = new CompactionStrategyFactory(cfs);

        assertTrue(factory.getRealm() instanceof CompactionRealm);
    }

    @Test
    public void realmIsNotWrappedIfKeyspaceUsingLocalStrategy()
    {
        System.setProperty("cassandra.compaction.remote_data_supplier", TestCompactionRealm.class.getName());

        when(cfs.metadata()).thenReturn(Schema.instance.getTableMetadata("local", "table"));
        when(cfs.getKeyspaceName()).thenReturn("local");

        CompactionStrategyFactory factory = new CompactionStrategyFactory(cfs);

        assertTrue(factory.getRealm() instanceof CompactionRealm);
    }

    @Test
    public void realmIsWrappedWithPropertySetAndKeyspaceNotLocal()
    {
        System.setProperty("cassandra.compaction.remote_data_supplier", TestCompactionRealm.class.getName());

        when(cfs.metadata()).thenReturn(Schema.instance.getTableMetadata("simple", "table"));
        when(cfs.getKeyspaceName()).thenReturn("simple");

        CompactionStrategyFactory factory = new CompactionStrategyFactory(cfs);

        assertTrue(factory.getRealm() instanceof TestCompactionRealm);
    }

    public static class TestCompactionRealm implements CompactionRealm
    {
        public TestCompactionRealm(ColumnFamilyStore wrapped)
        {
        }

        @Override
        public TableMetadataRef metadataRef()
        {
            return null;
        }

        @Override
        public int getKeyspaceReplicationFactor()
        {
            return 0;
        }

        @Override
        public Directories getDirectories()
        {
            return null;
        }

        @Override
        public DiskBoundaries getDiskBoundaries()
        {
            return null;
        }

        @Override
        public TableMetrics metrics()
        {
            return null;
        }

        @Override
        public SecondaryIndexManager getIndexManager()
        {
            return null;
        }

        @Override
        public boolean onlyPurgeRepairedTombstones()
        {
            return false;
        }

        @Override
        public Set<? extends CompactionSSTable> getOverlappingLiveSSTables(Iterable<? extends CompactionSSTable> sstables)
        {
            return null;
        }

        @Override
        public boolean isCompactionActive()
        {
            return false;
        }

        @Override
        public CompactionParams getCompactionParams()
        {
            return null;
        }

        @Override
        public boolean getNeverPurgeTombstones()
        {
            return false;
        }

        @Override
        public int getMinimumCompactionThreshold()
        {
            return 0;
        }

        @Override
        public int getMaximumCompactionThreshold()
        {
            return 0;
        }

        @Override
        public int getLevelFanoutSize()
        {
            return 0;
        }

        @Override
        public boolean supportsEarlyOpen()
        {
            return false;
        }

        @Override
        public long getExpectedCompactedFileSize(Iterable<SSTableReader> sstables, OperationType operationType)
        {
            return 0;
        }

        @Override
        public boolean isCompactionDiskSpaceCheckEnabled()
        {
            return false;
        }

        @Override
        public Iterable<Memtable> getAllMemtables()
        {
            return null;
        }

        @Override
        public Set<? extends CompactionSSTable> getLiveSSTables()
        {
            return null;
        }

        @Override
        public Set<? extends CompactionSSTable> getCompactingSSTables()
        {
            return null;
        }

        @Override
        public <S extends CompactionSSTable> Iterable<S> getNoncompactingSSTables(Iterable<S> sstables)
        {
            return null;
        }

        @Override
        public Iterable<? extends CompactionSSTable> getSSTables(SSTableSet set)
        {
            return null;
        }

        @Override
        public void invalidateCachedPartition(DecoratedKey key)
        {

        }

        @Override
        public Descriptor newSSTableDescriptor(File locationForDisk)
        {
            return null;
        }

        @Override
        public LifecycleTransaction tryModify(Iterable<? extends CompactionSSTable> sstables, OperationType operationType, UUID id)
        {
            return null;
        }

        @Override
        public OverlapTracker getOverlapTracker(Iterable<SSTableReader> sources)
        {
            return null;
        }

        @Override
        public void snapshotWithoutMemtable(String snapshotId)
        {

        }

        @Override
        public int mutateRepairedWithLock(Collection<SSTableReader> originals, long repairedAt, UUID pendingRepair, boolean isTransient) throws IOException
        {
            return 0;
        }

        @Override
        public void repairSessionCompleted(UUID sessionID)
        {

        }

        @Override
        public <V> V runWithCompactionsDisabled(Callable<V> callable, boolean interruptValidation, boolean interruptViews)
        {
            return null;
        }
    }
}
