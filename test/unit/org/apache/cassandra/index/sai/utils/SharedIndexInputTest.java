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

import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SharedIndexInputTest
{
    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("test", IOContext.DEFAULT);

        byte[] bytes = new byte[1000];
        ThreadLocalRandom.current().nextBytes(bytes);
        for (int x = 0; x < bytes.length; x++)
        {
            output.writeByte(bytes[x]);
        }
        output.close();

        IndexInput input = dir.openInput("test", IOContext.DEFAULT);
        SharedIndexInput2 input2 = new SharedIndexInput2(input);

        input2.seek(10);
        input.seek(10);

        byte[] loadBytes1 = new byte[10];
        byte[] loadBytes2 = new byte[10];

        input.readBytes(loadBytes1, 0, loadBytes1.length);
        input2.readBytes(loadBytes2, 0, loadBytes2.length);

        assertArrayEquals(loadBytes1, loadBytes2);

        input.readBytes(loadBytes1, 0, loadBytes1.length);
        input2.readBytes(loadBytes2, 0, loadBytes2.length);

        assertArrayEquals(loadBytes1, loadBytes2);

        for (int x = 0; x < 1000 * 2; x++)
        {
            int fp = ThreadLocalRandom.current().nextInt(0, bytes.length);
            final byte bytea;
            if (x % 2 == 0)
            {
                input.seek(fp);
                bytea = input.readByte();
            }
            else
            {
                input2.seek(fp);
                bytea = input2.readByte();
            }
            assertEquals("x=" + x, bytes[fp], bytea);
        }

        input.close();
        input2.close();
    }
}
