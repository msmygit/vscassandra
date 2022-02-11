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

import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

public class BooleanTypeTest extends SAITester
{
    @Test
    public void testTinyint() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val tinyint)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('0', 0)");
        execute("INSERT INTO %s (id, val) VALUES ('1', 1)");
        execute("INSERT INTO %s (id, val) VALUES ('2', 2)");

        flush();

//        execute("INSERT INTO %s (id, val) VALUES ('10', 10)");
//        execute("INSERT INTO %s (id, val) VALUES ('11', 11)");
//        execute("INSERT INTO %s (id, val) VALUES ('12', 12)");
//
//        flush();

//        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
//
//        waitForIndexQueryable();

        //compact();

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 0").size());
        //assertEquals(5, execute("SELECT id FROM %s WHERE val >= 1").size());
        //assertEquals(1, execute("SELECT id FROM %s WHERE val = 'tomato'").size());
    }

    @Test
    public void testSmallint() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val smallint)");
        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('0', 0)");
        execute("INSERT INTO %s (id, val) VALUES ('1', 1)");
        execute("INSERT INTO %s (id, val) VALUES ('2', 2)");

        flush();

//        execute("INSERT INTO %s (id, val) VALUES ('10', 10)");
//        execute("INSERT INTO %s (id, val) VALUES ('11', 11)");
//        execute("INSERT INTO %s (id, val) VALUES ('12', 12)");
//
//        flush();

//        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
//
//        waitForIndexQueryable();

        //compact();

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 0").size());
        //assertEquals(5, execute("SELECT id FROM %s WHERE val >= 1").size());
        //assertEquals(1, execute("SELECT id FROM %s WHERE val = 'tomato'").size());
    }

    // smallint
    @Test
    public void testDate() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val date)");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('0', '2013-06-10')");
        execute("INSERT INTO %s (id, val) VALUES ('1', '2013-06-11')");
        execute("INSERT INTO %s (id, val) VALUES ('2', '2013-06-12')");

        flush();

        execute("INSERT INTO %s (id, val) VALUES ('10', '2014-06-10')");
        execute("INSERT INTO %s (id, val) VALUES ('11', '2014-06-10')");
        execute("INSERT INTO %s (id, val) VALUES ('12', '2014-06-10')");

        flush();

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        waitForIndexQueryable();

        compact();

        assertEquals(1, execute("SELECT id FROM %s WHERE val = '2013-06-10'").size());
        assertEquals(6, execute("SELECT id FROM %s WHERE val >= '2013-06-10'").size());
        //assertEquals(1, execute("SELECT id FROM %s WHERE val = 'tomato'").size());
    }

    @Test
    public void testString() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('0', 'apple')");
        execute("INSERT INTO %s (id, val) VALUES ('1', 'sauce')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'yuck')");

        flush();

        execute("INSERT INTO %s (id, val) VALUES ('10', 'tomato')");
        execute("INSERT INTO %s (id, val) VALUES ('11', 'garbage')");
        execute("INSERT INTO %s (id, val) VALUES ('12', 'dump')");

        flush();

        compact();

        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'apple'").size());
        assertEquals(1, execute("SELECT id FROM %s WHERE val = 'tomato'").size());
    }

    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val boolean, val2 int)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(val2) USING 'StorageAttachedIndex'");

        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val, val2) VALUES ('0', false, 0)");
        execute("INSERT INTO %s (id, val, val2) VALUES ('1', true, 1)");
        execute("INSERT INTO %s (id, val, val2) VALUES ('2', true, 2)");

        flush();

        execute("INSERT INTO %s (id, val, val2) VALUES ('10', false, 10)");
        execute("INSERT INTO %s (id, val, val2) VALUES ('11', true, 11)");
        execute("INSERT INTO %s (id, val, val2) VALUES ('12', true, 12)");

        flush();

        compact();

        assertEquals(1, execute("SELECT id FROM %s WHERE val2 = 0").size());

        assertEquals(4, execute("SELECT id FROM %s WHERE val = true").size());
        assertEquals(2, execute("SELECT id FROM %s WHERE val = false").size());
    }
}
