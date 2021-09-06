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

package org.apache.cassandra.index.sai.disk;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.Version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * This test checks that we can query from an index that has different on-disk formats
 * on different SSTables
 */
public class VersionUpgradeTest extends SAITester
{
    private static Version currentVersion;

    @BeforeClass
    public static void getCurrentVersion() throws Throwable
    {
        currentVersion = Version.LATEST;
    }

    @AfterClass
    public static void resetVersion() throws Throwable
    {
        setLatestVersion(currentVersion);
    }

    @Test
    public void canQueryAcrossDifferentVersionsTest() throws Throwable
    {
        setLatestVersion(Version.AA);

        createTable(CREATE_TABLE_TEMPLATE);
        IndexContext intContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v1")), Int32Type.instance);
        IndexContext textContext = createIndexContext(createIndex(String.format(CREATE_INDEX_TEMPLATE, "v2")), UTF8Type.instance);

        for (int row = 0; row < 10; row++)
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, ?, ?)", Integer.toString(row), row, Integer.toString(row));
        flush();

        setLatestVersion(Version.BA);

        for (int row = 10; row < 20; row++)
            execute("INSERT INTO %s (id1, v1, v2) VALUES (?, ?, ?)", Integer.toString(row), row, Integer.toString(row));
        flush();

        // Collect the index descriptors for all the live SSTables. There should be 2 of them and they should
        // have different versions
        List<IndexDescriptor> indexDescriptors = getCurrentColumnFamilyStore().getLiveSSTables()
                                                                              .stream()
                                                                              .map(s -> IndexDescriptor.create(s.descriptor, s.metadata()))
                                                                              .collect(Collectors.toList());

        assertEquals(2, indexDescriptors.size());
        assertNotEquals(indexDescriptors.get(0).version, indexDescriptors.get(1).version);

        // Now check that all the index components exist on disk for each of the index descriptors
        for (IndexDescriptor indexDescriptor : indexDescriptors)
        {
            for (IndexComponent indexComponent : indexDescriptor.version.onDiskFormat().perSSTableComponents())
                assertTrue(indexDescriptor.fileFor(indexComponent).exists());
            for (IndexComponent indexComponent : indexDescriptor.version.onDiskFormat().perIndexComponents(intContext))
                assertTrue(indexDescriptor.fileFor(indexComponent, intContext.getIndexName()).exists());
            for (IndexComponent indexComponent : indexDescriptor.version.onDiskFormat().perIndexComponents(textContext))
                assertTrue(indexDescriptor.fileFor(indexComponent, textContext.getIndexName()).exists());
        }

        // Finally check that we can do a query across the 2 SSTable indexes
        assertRowsIgnoringOrder(execute("SELECT id1 FROM %s WHERE v1 > 7 AND v1 < 13"),
                                row("8"),
                                row("9"),
                                row("10"),
                                row("11"),
                                row("12"));
    }

    private static void setLatestVersion(final Version version) throws Exception
    {
        Field latestVersion = Version.class.getDeclaredField("LATEST");
        latestVersion.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(latestVersion, latestVersion.getModifiers() & ~Modifier.FINAL);
        latestVersion.set(null, version);
    }
}
