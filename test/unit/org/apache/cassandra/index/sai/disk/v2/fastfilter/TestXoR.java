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

package org.apache.cassandra.index.sai.disk.v2.fastfilter;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.v2.fastfilter.xor.Xor16;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestXoR
{
    @Test
    public void test() throws Exception
    {
        long[] array = new long[] {1000, 6000, 9000};
        Xor16 xor = new Xor16(array);

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        try (IndexOutput out = dir.createOutput("test", IOContext.DEFAULT))
        {
            xor.write(out);
        }
        try (IndexInput input = dir.openInput("test", IOContext.DEFAULT))
        {
            Xor16 xor2 = new Xor16(input);

            assertTrue(xor2.mayContain(1000));
            assertTrue(xor2.mayContain(6000));
            assertTrue(xor2.mayContain(9000));

            assertFalse(xor2.mayContain(3000));
        }
    }
}
