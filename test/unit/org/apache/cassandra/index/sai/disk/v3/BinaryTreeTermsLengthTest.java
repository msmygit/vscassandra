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

package org.apache.cassandra.index.sai.disk.v3;

import org.junit.Test;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static org.junit.Assert.assertEquals;

public class BinaryTreeTermsLengthTest
{
    // test the encoding method of the bytes values
    @Test
    public void test() throws Exception
    {
        for (int length = 0; length < 2000; length++)
        {
            try (ByteBuffersDirectory dir = new ByteBuffersDirectory())
            {
                try (IndexOutput out = dir.createOutput("aa", IOContext.DEFAULT))
                {
                    if (length >= 255)
                    {
                        out.writeByte((byte) 255);
                        out.writeVInt(length - 255);
                    }
                    else
                    {
                        out.writeByte((byte) length);
                    }
                }

                try (IndexInput input = dir.openInput("aa", IOContext.DEFAULT))
                {
                    byte a = input.readByte();
                    int length2;
                    if (Byte.toUnsignedInt(a) == 255)
                    {
                        length2 = input.readVInt() + 255;
                    }
                    else
                    {
                        length2 = Byte.toUnsignedInt(a);
                    }
                    
                    assertEquals(length, length2);
                }
            }
        }
    }
}
