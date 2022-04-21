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

package org.apache.cassandra.index.sai.memory;

import java.nio.ByteBuffer;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Creates byte buffer objects to parts of direct/offheap byte buffer blocks.
 *
 * If the added bytes length exceeds the remaining of the current byte block,
 * the bytes are added to the next block.
 *
 * The byte block size is 64kb.
 */
public class OffheapBytesArray
{
    public final static int BYTE_BLOCK_SHIFT = 16;
    public final static int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT; // 64kb

    private int currentBlockIndex = 0;
    private int count = 0;
    private ByteBuffer[] byteBuffers = new ByteBuffer[10];
    private ByteBuffer currentBuffer = null;
    private int currentOffset = 0;

    public OffheapBytesArray()
    {
        allocateBuffer();
    }

    private void allocateBuffer()
    {
        currentBuffer = ByteBuffer.allocateDirect(BYTE_BLOCK_SIZE);
        byteBuffers = ArrayUtil.grow(byteBuffers, currentBlockIndex + 1);
        byteBuffers[currentBlockIndex] = currentBuffer;
        currentOffset = BYTE_BLOCK_SIZE * currentBlockIndex;
        currentBlockIndex++;
    }

    public OffheapBytes add(BytesRef bytes)
    {
        if (bytes.length > currentBuffer.remaining())
        {
            allocateBuffer();
            return add(bytes);
        }
        else
        {
            int position = currentBuffer.position();
            currentBuffer.put(bytes.bytes, bytes.offset, bytes.length);

            ByteBuffer slice = currentBuffer.duplicate();
            slice.position(position);
            slice.limit(position + bytes.length);

            count++;

            return new OffheapBytes(slice);
        }
    }
}
