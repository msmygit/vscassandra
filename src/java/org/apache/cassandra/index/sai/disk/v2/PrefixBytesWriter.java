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
            int prefix = BytesUtil.bytesDifference(lastTerm.get(), bytes);
            if (prefix < 0)
            {
                prefix = 0;
            }
            int len = bytes.length - prefix;
            suffixBuffer.writeBytes(bytes.bytes, prefix, len);
            prefixLengths.add(prefix);
            suffixLengths.add(len);
        }
        else
        {
            suffixBuffer.writeBytes(bytes.bytes, 0, bytes.length);
            prefixLengths.add(0);
            suffixLengths.add(bytes.length);
        }

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
        final long fp = output.getFilePointer();

        // write the lengths to the buffers
        SimpleFoR.write(prefixLengths.toIntArray(), prefixLengthsBuffer);
        SimpleFoR.write(suffixLengths.toIntArray(), suffixLengthsBuffer);

        assert count <= 127;

        output.writeByte((byte)count);

        // TODO: maybe use unsigned short
        output.writeShort((short)prefixLengthsBuffer.getFilePointer());
        output.writeShort((short)suffixLengthsBuffer.getFilePointer());

        prefixLengthsBuffer.writeTo(output);
        suffixLengthsBuffer.writeTo(output);
        suffixBuffer.writeTo(output);

        return output.getFilePointer() - fp;
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
