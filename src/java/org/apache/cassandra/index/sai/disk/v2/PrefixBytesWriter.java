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

import java.io.IOException;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

public class PrefixBytesWriter
{
    private final RAMIndexOutput suffixBuffer = new RAMIndexOutput("");
    private final RAMIndexOutput prefixLengthsBuffer = new RAMIndexOutput("");
    private final RAMIndexOutput suffixLengthsBuffer = new RAMIndexOutput("");
    private final IntArrayList suffixLengths = new IntArrayList();
    private final IntArrayList prefixLengths = new IntArrayList();
    private final BytesRefBuilder lastTerm = new BytesRefBuilder();
    private int count = 0;

    public void add(BytesRef bytes)
    {
        if (lastTerm.length() > 0)
        {
            final int prefix = BytesUtil.bytesDifference(lastTerm.get(), bytes);
            int len = bytes.length - prefix;
            suffixBuffer.writeBytes(bytes.bytes, prefix, len);
            System.out.println("writeBytes=" + BytesUtil.toString(bytes.bytes, prefix, len));
            prefixLengths.add(prefix);
            suffixLengths.add(len);
        }
        else
        {
            suffixBuffer.writeBytes(bytes.bytes, 0, bytes.length);
            System.out.println("writeBytes=" + BytesUtil.toString(bytes.bytes, 0, bytes.length));
            prefixLengths.add(0);
            suffixLengths.add(bytes.length);
        }

        System.out.println("add prefixLength="+prefixLengths.get(prefixLengths.size()-1)+" suffixLength="+suffixLengths.get(suffixLengths.size()-1));

        lastTerm.copyBytes(bytes);
        count++;
    }

    public int count()
    {
        return count;
    }

    // return total size written
    public long finish(IndexOutput output) throws IOException
    {
        // write the lengths to the buffers
        SimpleFoR.write(prefixLengths.toIntArray(), prefixLengthsBuffer);
        SimpleFoR.write(suffixLengths.toIntArray(), suffixLengthsBuffer);

        final long fp = output.getFilePointer();

        // TODO: write unsigned short, becomes the largest sub block size

        assert count <= 128;

        output.writeByte((byte)count);
        output.writeShort((short)prefixLengthsBuffer.getFilePointer());
        output.writeShort((short)suffixLengthsBuffer.getFilePointer());

        prefixLengthsBuffer.writeTo(output);
        suffixLengthsBuffer.writeTo(output);

        final long suffixBytesFP = output.getFilePointer();
        System.out.println("finish suffixBytesFP="+suffixBytesFP);
        suffixBuffer.writeTo(output);

        long size = output.getFilePointer() - fp;

        return size;
    }

    public void reset()
    {
        count = 0;
        suffixBuffer.reset();
        prefixLengthsBuffer.reset();
        suffixLengthsBuffer.reset();
        suffixLengths.clear();
        prefixLengths.clear();
        lastTerm.clear();
    }
}
