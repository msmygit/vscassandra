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

package org.apache.cassandra.index.sai.cql;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.FileUtils;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.cql3.validation.operations.TTLTest.copyFile;
import static org.apache.cassandra.cql3.validation.operations.TTLTest.getTableDir;
import static org.apache.cassandra.db.ColumnFamilyStore.loadNewSSTables;
import static org.apache.commons.io.FileUtils.copyDirectory;
import static org.junit.Assert.assertEquals;

public class MixedVersionTest extends SAITester
{
    @Test
    public void test() throws Throwable
    {
        //descriptor = Descriptor.fromFilename(temporaryFolder.newFolder().getAbsolutePath() + "/bb-1-bti-Data.db");
        //FileUtils.copySSTablesAndIndexes(descriptor, "aa");

        String table = createTable("CREATE TABLE %s (pk int PRIMARY KEY, int_value int, text_value text)");

        java.io.File srcDir = new java.io.File("test/data/legacy-sai/" + "aa");
        File destDir = Keyspace.open(keyspace()).getColumnFamilyStore(table).getDirectories().getCFDirectories().iterator().next();

        System.out.println("destDir="+destDir);

        System.out.println("srcDir="+srcDir.getAbsolutePath());
        for (java.io.File file : srcDir.listFiles())
        {
            System.out.println("srcfile="+file.getAbsolutePath());
        }

        copyDirectory(srcDir, destDir.toJavaIOFile());

        Keyspace keyspace = Keyspace.open(KEYSPACE);
        ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(currentTable());

        assertEquals(0, cfs.getLiveSSTables().size());

        loadNewSSTables(KEYSPACE, currentTable());
        cfs.loadNewSSTables();

        //createIndex("CREATE CUSTOM INDEX ON %s(int_value) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        UntypedResultSet results = executeFormattedQuery("SELECT * FROM " + KEYSPACE + '.' + currentTable());
        for (UntypedResultSet.Row row : results)
        {
            System.out.println("int_value=" + row.getInt("int_value"));
            System.out.println("text_value=" + row.getString("text_value"));
            System.out.println();
        }

        assertEquals(1, executeFormattedQuery("SELECT pk FROM " + KEYSPACE + '.' + currentTable() + " WHERE int_value = 34").size());


//        tableMetadata = TableMetadata.builder("test", "test")
//                                     .addPartitionKeyColumn("pk", Int32Type.instance)
//                                     .addRegularColumn("int_value", Int32Type.instance)
//                                     .addRegularColumn("text_value", UTF8Type.instance)
//                                     .build();


//        for (File file : srcDir.tryList())
//        {
//            copyFile(file, destDir);
//        }

//        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
//        waitForIndexQueryable();
//
//        execute("INSERT INTO %s (id, val) VALUES ('0', 'smith')");
//        execute("INSERT INTO %s (id, val) VALUES ('1', 'john')");
//        execute("INSERT INTO %s (id, val) VALUES ('2', 'peaches')");
//        execute("INSERT INTO %s (id, val) VALUES ('3', 'portray')");
//
//        // test memory index
//        assertEquals(0, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'ra%'").size());
//        assertEquals(1, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'smi%'").size());
//        assertEquals(2, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'p%'").size());
//
//        flush();
//
//        // test disk index
//        assertEquals(0, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'ra%'").size());
//        assertEquals(1, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'smi%'").size());
//        assertEquals(2, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'p%'").size());
    }

    private void copySSTablesToTableDir(String table, boolean simple, boolean clustering) throws IOException
    {
        File destDir = Keyspace.open(keyspace()).getColumnFamilyStore(table).getDirectories().getCFDirectories().iterator().next();
        File sourceDir = getTableDir(table, simple, clustering);
        for (File file : sourceDir.tryList())
        {
            copyFile(file, destDir);
        }
    }
}
