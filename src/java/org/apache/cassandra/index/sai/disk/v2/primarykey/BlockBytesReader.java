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

import java.io.Closeable;
import java.io.IOException;

import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput2;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.util.BytesRef;

import static org.apache.cassandra.index.sai.disk.v2.primarykey.BlockBytesWriter.SUB_BLOCK_SIZE;

/**
 * Reads blocks of lexicographic ordered bytes of size 1024 values.
 */
public class BlockBytesReader implements Closeable
{
    private long blockLengthsFP;
    private long blockBytesFP;
    private PrefixBytesReader bytesReader;
    private final SharedIndexInput2 input;
    private int currentSubBlock = -1;
    private final SeekingRandomAccessInput seekInput;
    private DirectReaders.Reader blockPositionsBitsReader;

    public BlockBytesReader(SharedIndexInput2 input) throws IOException
    {
        this.input = input;
        seekInput = new SeekingRandomAccessInput(input);
        bytesReader = new PrefixBytesReader(input);
    }

    public BytesRef seek(int targetIndex) throws IOException
    {
        final int subBlock = targetIndex / SUB_BLOCK_SIZE;
        final int subBlockIndex = targetIndex % SUB_BLOCK_SIZE;
        final long subBlockFP = getSubBlockFP(subBlock);

        if (subBlock != currentSubBlock)
        {
            bytesReader.reset(subBlockFP);
            currentSubBlock = subBlock;
        }
        return bytesReader.seek(subBlockIndex);
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(seekInput, bytesReader);
    }

    public void reset(long fp) throws IOException
    {
        input.seek(fp);
        final short blockLengthsBytesLength = input.readShort();
        final byte blockPositionsBits = input.readByte();
        blockPositionsBitsReader = DirectReaders.getReaderForBitsPerValue(blockPositionsBits);
        blockLengthsFP = input.getFilePointer();
        blockBytesFP = blockLengthsFP + blockLengthsBytesLength;
        currentSubBlock = -1;
    }

    private long getSubBlockFP(int subBlock)
    {
        return blockBytesFP + LeafOrderMap.getValue(seekInput, blockLengthsFP, subBlock, blockPositionsBitsReader);
    }
}
