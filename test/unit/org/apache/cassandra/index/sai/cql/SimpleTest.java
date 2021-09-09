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
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.junit.Assert.assertEquals;

public class SimpleTest extends SAITester
{
    @Test
    public void test1() throws Throwable
    {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(0);
        buffer.rewind();
        ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
        byte[] bytes = ByteSourceInverse.readBytes(byteSource);

        System.out.println("bytes=" + Arrays.toString(bytes));
    }

    @Test
    public void testInt() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val int)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('0', 0)");
        execute("INSERT INTO %s (id, val) VALUES ('1', 1)");
        execute("INSERT INTO %s (id, val) VALUES ('2', 1)");

        flush();

        assertEquals(2, execute("SELECT id FROM %s WHERE val = 1").size());
        assertEquals(1, execute("SELECT id FROM %s WHERE val = 0").size());
    }

    @Test
    public void testLike() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('0', 'aaa')");
        execute("INSERT INTO %s (id, val) VALUES ('1', 'aaabbb')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'aabbbb')");
        execute("INSERT INTO %s (id, val) VALUES ('3', 'aacccc')");

        flush();

        assertEquals(0,  executeFormattedQuery("SELECT id FROM "+KEYSPACE + "." + currentTable()+" WHERE val LIKE 'acc%'").size());

        //assertEquals(1, execute("SELECT id FROM %s WHERE val LIKE 'aaa%'").size());
    }

    @Test
    public void testTextRange() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('0', '0')");
        execute("INSERT INTO %s (id, val) VALUES ('1', '1')");
        execute("INSERT INTO %s (id, val) VALUES ('2', '2')");

        flush();

        assertEquals(1, execute("SELECT id FROM %s WHERE val < '2' AND val > '0'").size());
    }
}
