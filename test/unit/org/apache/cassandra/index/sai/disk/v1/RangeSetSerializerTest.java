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

package org.apache.cassandra.index.sai.disk.v1;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.junit.Test;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static org.apache.cassandra.index.sai.disk.v2.blockindex.RangeSetSerializer.deserialize;
import static org.apache.cassandra.index.sai.disk.v2.blockindex.RangeSetSerializer.serialize;
import static org.junit.Assert.assertEquals;

public class RangeSetSerializerTest
{
    @Test
    public void test() throws Exception
    {
        RangeSet<Integer> rangeSet = TreeRangeSet.create();
        rangeSet.add(Range.closed(50, 100));
        rangeSet.add(Range.closed(900, 1000));
        rangeSet.add(Range.closed(2060, 3400));

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput out = dir.createOutput("file", IOContext.DEFAULT);
        serialize(rangeSet, out);
        out.close();

        IndexInput input = dir.openInput("file", IOContext.DEFAULT);
        RangeSet<Integer> rangeSet2 = deserialize(input);
        System.out.println("rangeSet2="+rangeSet2);

        assertEquals(rangeSet, rangeSet2);
    }
}
