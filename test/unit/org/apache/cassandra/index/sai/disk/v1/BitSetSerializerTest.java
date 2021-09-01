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

import java.util.BitSet;

import com.google.common.collect.RangeSet;
import org.junit.Test;

import org.apache.cassandra.index.sai.disk.v2.blockindex.BitSetSerializer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.FixedBitSet;

import static org.apache.cassandra.index.sai.disk.v2.blockindex.BitSetSerializer.deserialize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BitSetSerializerTest
{
    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput out = dir.createOutput("file", IOContext.DEFAULT);
        BitSet bitSet = new BitSet(10);
        bitSet.set(4);
        bitSet.set(8);
        long fp = BitSetSerializer.serialize(bitSet, out);
        out.close();

        IndexInput input = dir.openInput("file", IOContext.DEFAULT);
        FixedBitSet bitSet2 = deserialize(fp, input);
        assertTrue(bitSet2.get(4));
        assertTrue(bitSet2.get(8));
        assertFalse(bitSet2.get(2));
        assertFalse(bitSet2.get(9));
        assertFalse(bitSet2.get(1));
        input.close();
    }
}
