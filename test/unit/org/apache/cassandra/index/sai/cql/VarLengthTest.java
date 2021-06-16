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

import java.util.List;

import com.datastax.driver.core.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.index.sai.SAITester;

import static org.junit.Assert.assertEquals;

public class VarLengthTest extends SAITester
{
    @BeforeClass
    public static void setupCluster()
    {
        requireNetwork();
    }

    @Test
    public void test() throws Throwable
    {
        createTable("CREATE TABLE %s (id TEXT PRIMARY KEY, age INT, description TEXT)");
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "age"));
        createIndex(String.format(CREATE_INDEX_TEMPLATE, "description"));
        waitForIndexQueryable();

        execute("INSERT INTO %s (id, age, description) VALUES (?, ?, ?)", Integer.toString(0), 0, "apple");
        flush();
        execute("INSERT INTO %s (id, age, description) VALUES (?, ?, ?)", Integer.toString(1), 1, "appleipad");
        flush();

        //compact();

        List<Row> rows = executeNet("SELECT * FROM %s WHERE age = 0").all();
        assertEquals(1, rows.size());

        rows = executeNet("SELECT * FROM %s WHERE description = 'appleipad'").all();
        assertEquals(1, rows.size());
    }
}
