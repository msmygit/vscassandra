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

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

public class SAISimpleQueryTest extends SAITester
{
    @Test
    public void testSimple() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, value int, value2 int)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(value2) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (id, value, value2) VALUES(?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (id, value, value2) VALUES(?, ?, ?)", 2, 2, 2);

        UntypedResultSet result = execute("SELECT * FROM %s WHERE value = 1 AND value2 = 1");
        assertEquals(1, result.size());
    }

    public static Set<Integer> getInts(String column, UntypedResultSet result)
    {
        Set<Integer> set = new HashSet();
        for (UntypedResultSet.Row row : result)
        {
            set.add(row.getInt(column));
            //System.out.println("rowid="+);
        }
        return set;
    }

    @Test
    public void testUpdate() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, value int, value2 int)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(value2) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (id, value, value2) VALUES(?, ?, ?)", 1, 1, 1);
        execute("INSERT INTO %s (id, value, value2) VALUES(?, ?, ?)", 2, 2, 2);
        execute("INSERT INTO %s (id, value, value2) VALUES(?, ?, ?)", 3, 2, 2);
        execute("INSERT INTO %s (id, value, value2) VALUES(?, ?, ?)", 4, 1, 1);
        flush();
        execute("UPDATE %s SET value2 = ? WHERE id = ?", 2, 1);
        flush();

        compact();

        UntypedResultSet result6 = execute("SELECT * FROM %s WHERE value2 = 2");
        Set<Integer> ids6 = getInts("id", result6);
        System.out.println("ids6="+ids6);
        assertEquals(3, result6.size());
        assertEquals(Sets.newHashSet(1, 2, 3), ids6);

        UntypedResultSet result4 = execute("SELECT * FROM %s WHERE value2 >= 2");
        Set<Integer> ids4 = getInts("id", result4);
        System.out.println("ids4="+ids4);
        assertEquals(3, result4.size());
        assertEquals(Sets.newHashSet(1, 2, 3), ids4);

        UntypedResultSet result5 = execute("SELECT * FROM %s WHERE value2 < 2");
        assertEquals(1, result5.size());
        Set<Integer> ids5 = getInts("id", result5);
        System.out.println("ids5="+ids5);
        assertEquals(Sets.newHashSet(4), ids5);

        UntypedResultSet result = execute("SELECT * FROM %s WHERE value = 1 AND value2 = 2");
        assertEquals(1, result.size());

        UntypedResultSet result2 = execute("SELECT * FROM %s WHERE value = 1 AND value2 = 1");
        assertEquals(1, result2.size());
        assertEquals(4, result2.one().getInt("id"));

        UntypedResultSet result3 = execute("SELECT * FROM %s WHERE value = 2 AND value2 = 2");
        assertEquals(2, result3.size());
    }

//    @Test
//    public void test() throws Throwable
//    {
//        createTable("CREATE TABLE %s (id int primary key, value int, value2 int)");
//        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
//        createIndex("CREATE CUSTOM INDEX ON %s(value2) USING 'StorageAttachedIndex'");
//        execute("INSERT INTO %s (id, value, value2) VALUES(?, ?, ?)", 1, 1, 1);
//        execute("INSERT INTO %s (id, value, value2) VALUES(?, ?, ?)", 2, 2, 2);
//        flush();
//        execute("INSERT INTO %s (id, value, value2) VALUES(?, ?, ?)", 3, 2, 2);
//        execute("INSERT INTO %s (id, value, value2) VALUES(?, ?, ?)", 4, 1, 1);
//        execute("INSERT INTO %s (id, value2) VALUES(?, ?)", 1, 2);
//        //flush();
//        UntypedResultSet result = execute("SELECT * FROM %s WHERE value = 1 AND value2 = 2");
//        assertEquals(1, result.size());
//
//        UntypedResultSet result2 = execute("SELECT * FROM %s WHERE value = 1 AND value2 = 1");
//        assertEquals(1, result2.size());
//        assertEquals(4, result2.one().getInt("id"));
//
//        UntypedResultSet result3 = execute("SELECT * FROM %s WHERE value = 2 AND value2 = 2");
//        assertEquals(2, result3.size());
//    }
//
//    @Test
//    public void stringTest() throws Throwable
//    {
//        String string = "1";
//        ByteBuffer term = UTF8Type.instance.decompose(string);
//
//        final ByteComparable byteComparable = v -> UTF8Type.instance.asComparableBytes(term.duplicate(), v);
//
//        BytesRefBuilder builder = new BytesRefBuilder();
//
////        byte[] bytes = ByteSourceInverse.readBytes(byteSource);
////
////        gatherBytes(byteSource, builder);
//
//        BytesRef bytesRef = builder.toBytesRef();
//
//        System.out.println();
//    }
//
//    @Test
//    public void intTest() throws Throwable
//    {
//        int value = 10;
//        ByteBuffer term = Int32Type.instance.decompose(value);
//
//        final ByteSource byteSource = Int32Type.instance.asComparableBytes(term.duplicate(), ByteComparable.Version.OSS41);
//
//        BytesRefBuilder builder = new BytesRefBuilder();
//
//
//        byte[] bytes = ByteSourceInverse.readBytes(byteSource);
//
//        gatherBytes(byteSource, builder);
//
//        BytesRef bytesRef = builder.toBytesRef();
//
//        System.out.println();
//
//    }
//
//    @Test
//    public void rangeTest() throws Throwable
//    {
//        DataModel dataModel = new DataModel.BaseDataModel(DataModel.NORMAL_COLUMNS, DataModel.NORMAL_COLUMN_DATA);
//        DataModel.Executor executor = new SingleNodeExecutor(this, INDEX_QUERY_COUNTER);
//        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", DataModel.KEYSPACE));
//        dataModel.createTables(executor);
//        dataModel.disableCompaction(executor);
//        dataModel.createIndexes(executor);
//        dataModel.insertRows(executor);
//        dataModel.flush(executor);
//
////        UntypedResultSet result = execute("SELECT abbreviation FROM " + DataModel.KEYSPACE + "." + dataModel.indexedTable() + " WHERE murders_per_year <= 126 AND tiny_murders_per_year <= 9");
////
////        System.out.println(result);
//
//        List<Object> result2 = dataModel.executeIndexed(executor, "SELECT abbreviation FROM " +
//                                                                  DataModel.KEYSPACE + "." +
//                                                                  dataModel.indexedTable() +
//                                                                  " WHERE murders_per_year <= ? AND tiny_murders_per_year <= ?",
//                                                        4 ,
//                                                        (short)126,
//                                                        (byte)9);
//
//        System.out.println(result2);
//    }
}
