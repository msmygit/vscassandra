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

package org.apache.cassandra.index.sai.utils;

import org.junit.Test;

public class GreenLightRangeIntersectionIteratorTest extends SaiRandomizedTest
{
    @Test
    public void test() throws Exception
    {
        LongIterator it1 = new LongIterator(new long[]{ 10, 20, 30 }, new boolean[]{ true, false, true}, t -> t);
        LongIterator it2 = new LongIterator(new long[]{ 10, 25, 32 }, new boolean[]{ true, true, true}, t -> t);

        GreenLightRangeIntersectionIterator.Builder builder = new GreenLightRangeIntersectionIterator.Builder();
        builder.add(it1);
        builder.add(it2);

        RangeIterator results = builder.buildIterator();

        results.hasNext();
        PrimaryKey key1 = results.next();
        assertEquals(10, key1.sstableRowId());

        results.hasNext();
        PrimaryKey key2 = results.next();
        assertEquals(20, key2.sstableRowId());

//        while (results.hasNext())
//        {
//            PrimaryKey key = results.next();
//            System.out.println("key="+key);
//        }
    }
}
