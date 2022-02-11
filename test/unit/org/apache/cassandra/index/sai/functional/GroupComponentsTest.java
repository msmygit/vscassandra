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

package org.apache.cassandra.index.sai.functional;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.monitoring.runtime.instrumentation.common.collect.Sets;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static org.junit.Assert.assertEquals;

public class GroupComponentsTest extends SAITester
{
    @Test
    public void testInvalidateWithoutObsolete() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value int)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        execute("INSERT INTO %s (pk) VALUES (1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        StorageAttachedIndex index = (StorageAttachedIndex) group.getIndexes().iterator().next();
        SSTableReader sstable = Iterables.getOnlyElement(cfs.getLiveSSTables());

        Set<Component> components = group.getLiveComponents(sstable, getIndexesFromGroup(group));
        assertEquals(Version.LATEST.onDiskFormat().perSSTableComponents().size() + 1, components.size());

        // index files are released but not removed
        cfs.invalidate(true, false);
        Assert.assertTrue(index.getIndexContext().getView().getIndexes().isEmpty());
        for (Component component : components)
            Assert.assertTrue(sstable.descriptor.fileFor(component).exists());
    }

    @Test
    public void getLiveComponentsForEmptyIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value int)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();
        execute("INSERT INTO %s (pk) VALUES (1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        assertEquals(1, sstables.size());

        Set<Component> components = group.getLiveComponents(sstables.iterator().next(), getIndexesFromGroup(group));

        assertEquals(Version.LATEST.onDiskFormat().perSSTableComponents().size() + 1, components.size());
    }

    @Test
    public void getLiveComponentsForPopulatedIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int primary key, value int)");
        IndexContext indexContext = createIndexContext(createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'"), Int32Type.instance);
        waitForIndexQueryable();
        execute("INSERT INTO %s (pk, value) VALUES (1, 1)");
        flush();

        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        StorageAttachedIndexGroup group = StorageAttachedIndexGroup.getIndexGroup(cfs);
        Set<SSTableReader> sstables = cfs.getLiveSSTables();

        assertEquals(1, sstables.size());

        Set<Component> components = group.getLiveComponents(sstables.iterator().next(), getIndexesFromGroup(group));

        System.out.println("components="+components);

        Set<IndexComponent> comps = Version.LATEST.onDiskFormat().perIndexComponents(indexContext);
        Set<IndexComponent> comps2 = Version.LATEST.onDiskFormat().perSSTableComponents();

        Set<IndexComponent> total = new HashSet<>();
        total.addAll(comps);
        total.addAll(comps2);

        Set<String> reps = total.stream().map(comp -> comp.representation).collect(Collectors.toSet());

        for (String rep : reps)
        {
            boolean found = false;
            for (Component comp : components)
            {
                if (StringUtils.contains(comp.name, rep))
                {
                    found = true;
                    break;
                }
            }
            if (!found)
            {
                System.out.println("not found rep="+rep);
            }
        }

//        for (Component comp : components)
//        {
//            boolean found = false;
//
//                if (!StringUtils.contains(comp.name, rep))
//                {
//
//                }
//            }
//        }


        // Set<Object> diff = Sets.difference(total, components);

        //System.out.println("diff="+diff);

        assertEquals(components.toString(), Version.LATEST.onDiskFormat().perSSTableComponents().size() +
                     Version.LATEST.onDiskFormat().perIndexComponents(indexContext).size(),
                     components.size());
    }

    private Collection<StorageAttachedIndex> getIndexesFromGroup(StorageAttachedIndexGroup group)
    {
        return group.getIndexes().stream().map(index -> (StorageAttachedIndex)index).collect(Collectors.toList());
    }
}
