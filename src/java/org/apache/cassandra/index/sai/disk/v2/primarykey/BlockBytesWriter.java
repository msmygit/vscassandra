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
import java.util.Arrays;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Writes blocks of 32 first term prefix encoded bytes.
 */
public class BlockBytesWriter
{
    public static final int BLOCK_SIZE = 1024;
    public static final int SUB_BLOCK_SIZE = 32;
    private final RAMIndexOutput blocksBuffer = new RAMIndexOutput("");
    private final RAMIndexOutput blockPositionsBuffer = new RAMIndexOutput("");
    private final IntArrayList blockPositions = new IntArrayList();
    private final PrefixBytesWriter subWriter = new PrefixBytesWriter();
    private int count = 0;

    public void add(BytesRef term) throws IOException
    {
        if (count % SUB_BLOCK_SIZE == 0 && count > 0)
        {
            flushSubBlock();
        }
        subWriter.add(term);
        count++;
    }

    public void finish(IndexOutput output) throws IOException
    {
        // write the last block if necessary
        if (subWriter.count() > 0)
        {
            flushSubBlock();
        }

        // write to the block positions buffer
        final int[] blockPositionsArray = blockPositions.toIntArray();
        final int max = Arrays.stream(blockPositionsArray).max().getAsInt();
        final int bits = DirectWriter.unsignedBitsRequired(max);
        LeafOrderMap.write(blockPositionsArray, blockPositionsArray.length, max, blockPositionsBuffer);

        output.writeShort((short) blockPositionsBuffer.getFilePointer());
        output.writeByte((byte) bits);

        // write the block positions buffer
        blockPositionsBuffer.writeTo(output);
        // write the blocks buffer
        blocksBuffer.writeTo(output);

        // reset for the next block
        blocksBuffer.reset();
        blockPositionsBuffer.reset();
        count = 0;
    }

    private void flushSubBlock() throws IOException
    {
        int blockPosition = (int)blocksBuffer.getFilePointer();
        subWriter.finish(blocksBuffer);
        blockPositions.add(blockPosition);
        subWriter.reset();
    }
}
