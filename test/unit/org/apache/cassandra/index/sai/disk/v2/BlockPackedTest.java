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

package org.apache.cassandra.index.sai.disk.v2;

import org.junit.Test;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.packed.BlockPackedReader;
import org.apache.lucene.util.packed.BlockPackedWriter;
import org.apache.lucene.util.packed.PackedInts;

import static org.junit.Assert.assertEquals;

public class BlockPackedTest
{
    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);

        long[] array = new long[] {55, 100, 1005};

        BlockPackedWriter writer = new BlockPackedWriter(output, 128);
        for (long value : array)
        {
            writer.add(value);
        }

        long count = writer.ord();
        writer.finish();
        output.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);

        BlockPackedReader reader = new BlockPackedReader(input, PackedInts.VERSION_CURRENT, 128, count, true);
        for (int x = 0; x < count; x++)
        {
            long value = reader.get(x);
            assertEquals(array[x], value);
        }
        input.close();
    }
}
