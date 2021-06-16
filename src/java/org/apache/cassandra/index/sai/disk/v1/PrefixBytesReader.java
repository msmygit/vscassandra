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

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;

public class PrefixBytesReader
{
    public PrefixBytesReader(final IndexInput input, final IndexInput input2, int index, BytesRefBuilder builder) throws IOException
    {
        builder.clear();

        final SeekingRandomAccessInput seekingInput = new SeekingRandomAccessInput(input2);
        final long filePointer = input.getFilePointer();
        final int lengthsBytesLen = input.readInt();
        final int prefixBytesLen = input.readInt();
        final byte lengthsBits = input.readByte();
        final byte prefixBits = input.readByte();

        final long arraysFilePointer = input.getFilePointer();

        System.out.println("arraysFilePointer="+arraysFilePointer+" lengthsBytesLen="+lengthsBytesLen+" prefixBytesLen="+prefixBytesLen+" lengthsBits="+lengthsBits+" prefixBits="+prefixBits);

        final DirectReaders.Reader lengthsReader = DirectReaders.getReaderForBitsPerValue(lengthsBits);
        final DirectReaders.Reader prefixesReader = DirectReaders.getReaderForBitsPerValue(prefixBits);

        input.seek(arraysFilePointer + lengthsBytesLen + prefixBytesLen);

        long bytesPosition = input.getFilePointer();
        int bytesLength = 0;
        int lastLen = 0;
        int lastPrefix = 0;

        byte[] firstTerm = null;

        for (int x=0; x <= index; x++)
        {
            int len = LeafOrderMap.getValue(seekingInput, arraysFilePointer, x, lengthsReader);
            System.out.println("x="+x+" len="+len);

            if (x == 0)
            {
                firstTerm = new byte[len];
                long filePointer44 = input.getFilePointer();
                input.readBytes(firstTerm, 0, len);
                System.out.println("bytes file pointer="+filePointer44);
                System.out.println("firstTerm="+new BytesRef(firstTerm).utf8ToString());
            }

            int prefix = LeafOrderMap.getValue(seekingInput, arraysFilePointer + lengthsBytesLen, x, prefixesReader);

            if (prefix > 0 && x > 0)
            {
                bytesLength = (len - prefix);
                lastLen = len;
                lastPrefix = prefix;
                System.out.println("x=" + x + " bytesLength=" + bytesLength + " len=" + len + " prefix=" + prefix);
                bytesPosition += bytesLength;
            }
        }

        input.seek(bytesPosition);
        System.out.println("lastlen="+lastLen+" bytesLength="+bytesLength);
        byte[] bytes = new byte[bytesLength];
        input.readBytes(bytes, 0, bytesLength);

        builder.append(firstTerm, 0, lastPrefix);
        builder.append(bytes, 0, bytes.length);

        System.out.println("str="+builder.get().utf8ToString());
    }
}
