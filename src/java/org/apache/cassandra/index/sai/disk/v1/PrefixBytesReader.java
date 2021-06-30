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
    final DirectReaders.Reader lengthsReader, prefixesReader;
    final int lengthsBytesLen;
    final int prefixBytesLen;
    final byte lengthsBits;
    final byte prefixBits;
    final long arraysFilePointer;
    final SeekingRandomAccessInput seekingInput;
    final IndexInput input;
    final BytesRefBuilder builder = new BytesRefBuilder();
    final int leafSize;

    private int index = 0;
    private long bytesPosition;
    private final long bytesPositionStart;
    int bytesLength = 0;
    int lastLen = 0;
    int lastPrefix = 0;
    byte[] firstTerm;

    public PrefixBytesReader(final IndexInput input, final IndexInput input2) throws IOException
    {
        this.input = input;

        seekingInput = new SeekingRandomAccessInput(input2);
        final long filePointer = input.getFilePointer();
        leafSize = input.readInt();
        lengthsBytesLen = input.readInt();
        prefixBytesLen = input.readInt();
        lengthsBits = input.readByte();
        prefixBits = input.readByte();

        arraysFilePointer = input.getFilePointer();

        System.out.println("arraysFilePointer="+arraysFilePointer+" lengthsBytesLen="+lengthsBytesLen+" prefixBytesLen="+prefixBytesLen+" lengthsBits="+lengthsBits+" prefixBits="+prefixBits);

        lengthsReader = DirectReaders.getReaderForBitsPerValue(lengthsBits);
        prefixesReader = DirectReaders.getReaderForBitsPerValue(prefixBits);

        input.seek(arraysFilePointer + lengthsBytesLen + prefixBytesLen);

        bytesPositionStart = bytesPosition = input.getFilePointer();
    }

    public BytesRef seek(int seekIndex) throws IOException
    {
        if (seekIndex < index) throw new IllegalArgumentException("seekIndex="+seekIndex+" must be greater than index="+index);
        if (seekIndex >= leafSize) throw new IllegalArgumentException("seekIndex="+seekIndex+" must be less than the leaf size="+leafSize);

        int len = 0;
        int prefix = 0;

        for (int x = index; x <= seekIndex; x++)
        {
            len = LeafOrderMap.getValue(seekingInput, arraysFilePointer, x, lengthsReader);
            prefix = LeafOrderMap.getValue(seekingInput, arraysFilePointer + lengthsBytesLen, x, prefixesReader);

            System.out.println("x="+x+" len="+len+" prefix="+prefix);

            if (x == 0)
            {
                firstTerm = new byte[len];
                input.readBytes(firstTerm, 0, len);
                lastPrefix = len;
                System.out.println("firstTerm="+new BytesRef(firstTerm).utf8ToString());
                bytesLength = 0;
                bytesPosition += len;
            }

            if (len > 0 && x > 0)
            {
                bytesLength = len - prefix;
                lastLen = len;
                lastPrefix = prefix;
                System.out.println("x=" + x + " bytesLength=" + bytesLength + " len=" + len + " prefix=" + prefix);
            }
            else
            {
                bytesLength = 0;
            }
        }

        this.index = seekIndex + 1;

        if (len == 0 && prefix == 0)
        {

        }
        else
        {
            builder.clear();

            System.out.println("bytesPosition=" + bytesPosition + " bytesPositionStart=" + bytesPositionStart + " total=" + (bytesPosition - bytesPositionStart));

            System.out.println("lastlen=" + lastLen + " lastPrefix=" + lastPrefix + " bytesLength=" + bytesLength);
            final byte[] bytes = new byte[bytesLength];
            input.seek(bytesPosition);
            input.readBytes(bytes, 0, bytesLength);

            bytesPosition += bytesLength;

            System.out.println("bytes read=" + new BytesRef(bytes).utf8ToString());

            builder.append(firstTerm, 0, lastPrefix);
            builder.append(bytes, 0, bytes.length);
        }

        System.out.println("str="+builder.get().utf8ToString());
        return builder.get();
    }
}
