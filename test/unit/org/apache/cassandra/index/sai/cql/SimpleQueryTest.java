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

public class SimpleQueryTest extends SAITester
{
    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (id int primary key, value text, value2 text)");
        createIndex("CREATE CUSTOM INDEX ON %s(value) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(value2) USING 'StorageAttachedIndex'");
        execute("INSERT INTO %s (id, value, value2) VALUES (?, ?, ?)", 1, "1", "1");
        execute("INSERT INTO %s (id, value2) VALUES (?, ?)", 3, "3");
        flush();
        execute("INSERT INTO %s (id, value, value2) VALUES (?, ?, ?)", 2, "2", "2");
        execute("INSERT INTO %s (id, value) VALUES (?, ?)", 3, "3");
        flush();
        assertEquals(1, execute("SELECT * FROM %s WHERE value = '1' AND value2 = '1'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE value = '2' AND value2 = '2'").size());
        assertEquals(1, execute("SELECT * FROM %s WHERE value = '3' AND value2 = '3'").size());
    }
}
