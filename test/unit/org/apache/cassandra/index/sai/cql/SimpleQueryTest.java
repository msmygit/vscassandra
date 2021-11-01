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

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import static org.apache.cassandra.index.sai.cql.AbstractQueryTester.INDEX_QUERY_COUNTER;
import static org.junit.Assert.assertEquals;

@Ignore
public class SimpleQueryTest extends SAITester
{
    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, value int)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (id, value) VALUES(?, ?)", 1, 1);
        flush();
//        execute("INSERT INTO %s (id, value) VALUES(?, ?)", 10, 10);
//        flush();
//        compact();
        assertEquals(1, execute("SELECT * FROM %s WHERE value = 1 AND id = 1").size());
    }

    @Test
    public void compoundKey() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck1 int, ck2 int, value int, primary key((pk1, pk2), ck1, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, value) VALUES(?, ?, ?, ?, ?, ?)", 1, 1, 1, 1, 1);
        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, value) VALUES(?, ?, ?, ?, ?, ?)", 2, 1, 1, 1, 2);
        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, value) VALUES(?, ?, ?, ?, ?, ?)", 1, 1, 1, 1, 3);
//        flush();

        assertEquals(2, execute("SELECT * FROM %s WHERE value < 3").size());



    }

    @Test
    public void compoundKeyWithStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk1 int, pk2 int, ck1 int, ck2 int, staticvalue int static, value int, primary key((pk1, pk2), ck1, ck2))");
        createIndex("CREATE CUSTOM INDEX ON %s(staticvalue) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");

        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, staticvalue, value) VALUES(?, ?, ?, ?, ?, ?)", 1, 1, 1, 1, 1, 1);
        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, staticvalue, value) VALUES(?, ?, ?, ?, ?, ?)", 2, 1, 1, 1, 1, 2);
        execute("INSERT INTO %s (pk1, pk2, ck1, ck2, staticvalue, value) VALUES(?, ?, ?, ?, ?, ?)", 3, 1, 1, 1, 1, 3);
//        flush();

        assertEquals(2, execute("SELECT * FROM %s WHERE value < 3").size());



    }

    @Test
    public void literalTest() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, value text)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (id, value) VALUES(?, ?)", 1, "1");
        flush();
        execute("INSERT INTO %s (id, value) VALUES(?, ?)", 10, "10");
        flush();
        compact();
        assertEquals(1, execute("SELECT * FROM %s WHERE value = '1'").size());
    }

    @Test
    public void rangeTest() throws Throwable
    {
        DataModel dataModel = new DataModel.CompoundKeyWithStaticsDataModel(DataModel.STATIC_COLUMNS, DataModel.STATIC_COLUMN_DATA);
        DataModel.Executor executor = new SingleNodeExecutor(this, INDEX_QUERY_COUNTER);
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", DataModel.KEYSPACE));
        dataModel.createTables(executor);
        dataModel.disableCompaction(executor);
        dataModel.createIndexes(executor);
        dataModel.insertRows(executor);
        dataModel.flush(executor);

//        UntypedResultSet result = execute("SELECT abbreviation FROM " + DataModel.KEYSPACE + "." + dataModel.indexedTable() + " WHERE murders_per_year <= 126 AND tiny_murders_per_year <= 9");
//
//        System.out.println(result);

//        UntypedResultSet result = execute("SELECT abbreviation FROM " + DataModel.KEYSPACE + "." + dataModel.indexedTable() + " WHERE gdp < ?", 5000000000L);
//
//
//        System.out.println(makeRowStrings(result));

        List<Object> result2 = dataModel.executeIndexed(executor, "SELECT abbreviation FROM " +
                                                                  DataModel.KEYSPACE + "." +
                                                                  dataModel.indexedTable() +
                                                                  " WHERE gdp < ?",
                                                        5 ,
                                                        5000000000L);

        System.out.println(result2);
    }
}
