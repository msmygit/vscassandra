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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

public class SinglePrefixBytesBlockSerializer implements BlockValuesSerializer
{
//    @Override
//    public void readBlock(long filePointer, BlockIndexReader.BlockIndexReaderContext context) throws IOException
//    {
//        context.bytesInput.seek(filePointer);
//        context.currentLeafFP = filePointer;
//        context.leafSize = context.bytesInput.readInt();
//        context.lengthsBytesLen = context.bytesInput.readInt();
//        context.prefixBytesLen = context.bytesInput.readInt();
//        context.lengthsBits = context.bytesInput.readByte();
//        context.prefixBits = context.bytesInput.readByte();
//
//        context.arraysFilePointer = context.bytesInput.getFilePointer();
//
//        context.lengthsReader = DirectReaders.getReaderForBitsPerValue(context.lengthsBits);
//        context.prefixesReader = DirectReaders.getReaderForBitsPerValue(context.prefixBits);
//
//        context.bytesInput.seek(context.arraysFilePointer + context.lengthsBytesLen + context.prefixBytesLen);
//
//        context.leafBytesStartFP = context.leafBytesFP = context.bytesInput.getFilePointer();
//
//        // TODO: alloc here probably not required?
//        context.seekingInput = new SeekingRandomAccessInput(context.bytesInput);
//    }
//
//    @Override
//    public BytesRef seekInBlock(int seekIndex,
//                                BlockIndexReader.BlockIndexReaderContext context,
//                                boolean incLeafIndex) throws IOException
//    {
//        if (seekIndex >= context.leafSize)
//        {
//            throw new IllegalArgumentException("seekIndex="+seekIndex+" must be less than the leaf size="+context.leafSize);
//        }
//
//        int len = 0;
//        int prefix = 0;
//
//        // TODO: this part can go back from the current
//        //       position rather than start from the beginning each time
//
//        int start = 0;
//
//        // start from where we left off
//        if (seekIndex >= context.leafIndex)
//        {
//            start = context.leafIndex;
//        }
//
//        for (int x = start; x <= seekIndex; x++)
//        {
//            len = LeafOrderMap.getValue(context.seekingInput, context.arraysFilePointer, x, context.lengthsReader);
//            prefix = LeafOrderMap.getValue(context.seekingInput, context.arraysFilePointer + context.lengthsBytesLen, x, context.prefixesReader);
//
//            if (x == 0)
//            {
//                if (context.firstTerm == null)
//                {
//                    context.firstTerm = new byte[len];
//                }
//                else
//                {
//                    context.firstTerm = ArrayUtil.grow(context.firstTerm, len);
//                }
//                context.firstTermLen = len;
//
//                context.leafBytesFP = context.leafBytesStartFP;
//                context.bytesInput.seek(context.leafBytesStartFP);
//                context.bytesInput.readBytes(context.firstTerm, 0, len);
//                context.lastPrefix = len;
//                context.lastLen = len;
//                //System.out.println("firstTerm="+new BytesRef(firstTerm).utf8ToString());
//                context.bytesLength = 0;
//                context.leafBytesFP += len;
//            }
//            else if (len > 0)
//            {
//                context.bytesLength = len - prefix;
////                context.leafBytesFP += context.lastLen - context.lastPrefix;
//                context.lastPrefix = prefix;
//                context.lastLen = len;
//                if (x < seekIndex)
//                    context.leafBytesFP += context.bytesLength;
//
//            }
//            else
//            {
//                context.bytesLength = 0;
//            }
//        }
//
//        // TODO: don't do this with an IndexIterator
//        if (incLeafIndex)
//        {
//            context.leafIndex = seekIndex + 1;
//        }
//
//        if (!(len == 0 && prefix == 0))
//        {
//            context.builder.clear();
//
//            if (context.bytesLength > 0)
//            {
//                if (context.bytes == null)
//                {
//                    context.bytes = new byte[context.bytesLength];
//                }
//                else
//                {
//                    context.bytes = ArrayUtil.grow(context.bytes, context.bytesLength);
//                }
//                context.bytesInput.seek(context.leafBytesFP);
//                context.bytesInput.readBytes(context.bytes, 0, context.bytesLength);
//                context.leafBytesFP += context.bytesLength;
//            }
//            if (context.lastPrefix > 0 && context.firstTerm != null)
//                context.builder.append(context.firstTerm, 0, context.lastPrefix);
//            if (context.bytesLength > 0 )
//            {
//                context.builder.append(context.bytes, 0, context.bytesLength);
//            }
//        }
//        context.lastLeafIndex = seekIndex;
//        return context.builder.get();
//    }

    @Override
    public void write(BlockIndexWriter.BlockBuffer buffer, IndexOutput output) throws IOException
    {
//        final int maxLength = Arrays.stream(buffer.lengths).max().getAsInt();
//        LeafOrderMap.write(buffer.lengths, buffer.leafOrdinal, maxLength, buffer.lengthsScratchOut);
//        final int maxPrefix = Arrays.stream(buffer.prefixes).max().getAsInt();
//        LeafOrderMap.write(buffer.prefixes, buffer.leafOrdinal, maxPrefix, buffer.prefixScratchOut);
//
//        output.writeInt(buffer.leafOrdinal); // value count
//        output.writeInt(buffer.lengthsScratchOut.getPosition());
//        output.writeInt(buffer.prefixScratchOut.getPosition());
//        output.writeByte((byte) DirectWriter.unsignedBitsRequired(maxLength));
//        output.writeByte((byte) DirectWriter.unsignedBitsRequired(maxPrefix));
//        output.writeBytes(buffer.lengthsScratchOut.getBytes(), 0, buffer.lengthsScratchOut.getPosition());
//        output.writeBytes(buffer.prefixScratchOut.getBytes(), 0, buffer.prefixScratchOut.getPosition());
//        output.writeBytes(buffer.scratchOut.getBytes(), 0, buffer.scratchOut.getPosition());
    }
}
