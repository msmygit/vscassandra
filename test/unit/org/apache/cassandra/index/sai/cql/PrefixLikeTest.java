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

public class PrefixLikeTest extends SAITester
{
    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('0', 'smith')");
        execute("INSERT INTO %s (id, val) VALUES ('1', 'john')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'peaches')");
        execute("INSERT INTO %s (id, val) VALUES ('3', 'portray')");

        // test memory index
        assertEquals(0, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'ra%'").size());
        assertEquals(1, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'smi%'").size());
        assertEquals(2, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'p%'").size());

        flush();

        // test disk index
        assertEquals(0, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'ra%'").size());
        assertEquals(1, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'smi%'").size());
        assertEquals(2, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val LIKE 'p%'").size());
    }

    @Test
    public void testStringRangeQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (id text PRIMARY KEY, val text)");

        createIndex("CREATE CUSTOM INDEX ON %s(val) USING 'StorageAttachedIndex'");
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, val) VALUES ('0', 'apple')");
        execute("INSERT INTO %s (id, val) VALUES ('1', 'application')");
        execute("INSERT INTO %s (id, val) VALUES ('2', 'applier')");

        // test memory index
        assertEquals(3, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val >= 'app'").size());
        assertEquals(2, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val >= 'app' AND val <= 'application'").size());
        assertEquals(2, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val > 'app' AND val < 'applier'").size());
        assertEquals(2, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val >= 'appli'").size());
        assertEquals(0, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val >= 'bob'").size());

        flush();

        // test disk index
        assertEquals(3, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val >= 'app'").size());
        assertEquals(2, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val >= 'app' AND val <= 'application'").size());
        assertEquals(2, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val > 'app' AND val < 'applier'").size());
        assertEquals(2, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val >= 'appli'").size());
        assertEquals(0, executeFormattedQuery("SELECT id FROM " + KEYSPACE + '.' + currentTable() + " WHERE val >= 'bob'").size());
    }
}
