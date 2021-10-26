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

package org.apache.cassandra.index.sai.disk.v2.primarykey;

import java.io.IOException;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.disk.v2.SimpleFoR;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Writes a block of 32 first term prefix encoded lexicographic ordered bytes.
 */
public class PrefixBytesWriter
{
    private final RAMIndexOutput suffixBuffer = new RAMIndexOutput("");
    private final RAMIndexOutput prefixLengthsBuffer = new RAMIndexOutput("");
    private final RAMIndexOutput suffixPositionsBuffer = new RAMIndexOutput("");
    private final IntArrayList suffixPositions = new IntArrayList();
    private final IntArrayList suffixLengths = new IntArrayList();
    private final IntArrayList prefixLengths = new IntArrayList();
    private final BytesRefBuilder firstTerm = new BytesRefBuilder();
    private int count = 0;

    public void add(BytesRef bytes)
    {
        // record the suffix position
        final int suffixPosition = (int)suffixBuffer.getFilePointer();
        suffixPositions.add(suffixPosition);

        if (firstTerm.length() == 0)
        {
            suffixBuffer.writeBytes(bytes.bytes, 0, bytes.length);

            prefixLengths.add(0);

            assert firstTerm.length() == 0;

            firstTerm.copyBytes(bytes);

            suffixLengths.add(firstTerm.length());
        }
        else
        {
            int prefix = BytesUtil.bytesDifference(firstTerm.get(), bytes);
            if (prefix < 0)
            {
                prefix = bytes.length;
            }
            int suffixLen = bytes.length - prefix;

            if (suffixLen > 0)
            {
                suffixBuffer.writeBytes(bytes.bytes, prefix + bytes.offset, suffixLen);
            }
            prefixLengths.add(prefix);

            suffixLengths.add(suffixLen);
        }
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

        int lastSuffixPosition = suffixPositions.get(suffixPositions.size() - 1) + suffixLengths.get(suffixLengths.size() - 1);
        suffixPositions.add(lastSuffixPosition);

        // write the lengths to the buffers
        SimpleFoR.write(prefixLengths.toIntArray(), prefixLengthsBuffer);
        SimpleFoR.write(suffixPositions.toIntArray(), suffixPositionsBuffer);

        assert count <= 127;

        output.writeByte((byte)count);

        // TODO: maybe use unsigned short
        output.writeShort((short)prefixLengthsBuffer.getFilePointer());
        output.writeShort((short)suffixPositionsBuffer.getFilePointer());

        prefixLengthsBuffer.writeTo(output);
        suffixPositionsBuffer.writeTo(output);
        suffixBuffer.writeTo(output);

        return output.getFilePointer() - fp;
    }

    public void reset()
    {
        count = 0;
        suffixBuffer.reset();
        prefixLengthsBuffer.reset();
        suffixPositionsBuffer.reset();
        suffixPositions.clear();
        prefixLengths.clear();
        firstTerm.clear();
    }
}
