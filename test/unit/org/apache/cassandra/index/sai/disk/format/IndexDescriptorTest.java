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

package org.apache.cassandra.index.sai.disk.format;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.PathUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexDescriptorTest
{
    private TemporaryFolder temporaryFolder = new TemporaryFolder();
    private Descriptor descriptor;
    private Version latest;

    @BeforeClass
    public static void initialise()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup() throws Throwable
    {
        temporaryFolder.create();
        descriptor = Descriptor.fromFilename(temporaryFolder.newFolder().getAbsolutePath() + "/nb-1-big-Data.db");
        latest = Version.LATEST;
    }

    @After
    public void teardown() throws Throwable
    {
        setLatestVersion(latest);
        temporaryFolder.delete();
    }

    @Test
    public void versionBAPerSSTableComponentIsParsedCorrectly() throws Throwable
    {
        setLatestVersion(Version.BA);

        createFileOnDisk("-SAI+ba+GroupComplete.db");

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);

        assertEquals(Version.BA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.GROUP_COMPLETION_MARKER));
    }

    @Test
    public void versionBAPerIndexComponentIsParsedCorrectly() throws Throwable
    {
        setLatestVersion(Version.BA);

        createFileOnDisk("-SAI+ba+test_index+ColumnComplete.db");

        IndexDescriptor indexDescriptor = IndexDescriptor.create(descriptor, Murmur3Partitioner.instance, SAITester.EMPTY_COMPARATOR);
        IndexContext indexContext = SAITester.createIndexContext("test_index", UTF8Type.instance);

        assertEquals(Version.BA, indexDescriptor.version);
        assertTrue(indexDescriptor.hasComponent(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext));
    }

    private void setLatestVersion(Version version) throws Throwable
    {
        Field latest = Version.class.getDeclaredField("LATEST");
        latest.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(latest, latest.getModifiers() & ~Modifier.FINAL);
        latest.set(null, version);
    }

    private void createFileOnDisk(String filename) throws Throwable
    {
        Path path;
        try
        {
            path = Paths.get(URI.create(descriptor.baseFilename() + filename));
        }
        catch (IllegalArgumentException ex)
        {
            path = Paths.get(descriptor.baseFilename() + filename);
        }

        Files.touch(new File(path).toJavaIOFile());
    }
}
