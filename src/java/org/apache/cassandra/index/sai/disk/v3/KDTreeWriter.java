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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;

public class KDTreeWriter
{
    private final int bytesPerDim = 4;
    private final int numIndexDims = 1;
    final BytesRef scratchBytesRef1 = new BytesRef();
    final List<byte[]> leafBlockStartValues = new ArrayList<>();

    private byte[] packIndex(BKDTreeLeafNodes leafNodes) throws IOException {
        /** Reused while packing the index */
        ByteBuffersDataOutput writeBuffer = ByteBuffersDataOutput.newResettableInstance();

        // This is the "file" we append the byte[] to:
        List<byte[]> blocks = new ArrayList<>();
        byte[] lastSplitValues = new byte[bytesPerDim];
        //System.out.println("\npack index");
        int totalSize = recursePackIndex(writeBuffer, leafNodes, 0l, blocks, lastSplitValues, new boolean[numIndexDims], false,
                                         0, leafNodes.numLeaves());

        // Compact the byte[] blocks into single byte index:
        byte[] index = new byte[totalSize];
        int upto = 0;
        for(byte[] block : blocks) {
            System.arraycopy(block, 0, index, upto, block.length);
            upto += block.length;
        }
        assert upto == totalSize;

        return index;
    }

    private int recursePackIndex(ByteBuffersDataOutput writeBuffer,
                                 BKDTreeLeafNodes leafNodes,
                                 long minBlockFP,
                                 List<byte[]> blocks,
                                 byte[] lastSplitValues,
                                 boolean[] negativeDeltas,
                                 boolean isLeft,
                                 int leavesOffset,
                                 int numLeaves) throws IOException
    {
        if (numLeaves == 1)
        {
            if (isLeft)
            {
                assert leafNodes.getLeafLP(leavesOffset) - minBlockFP == 0;
                return 0;
            }
            else
            {
                long delta = leafNodes.getLeafLP(leavesOffset) - minBlockFP;
                assert leafNodes.numLeaves() == numLeaves || delta > 0 : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
                writeBuffer.writeVLong(delta);
                return appendBlock(writeBuffer, blocks);
            }
        }
        else
        {
            long leftBlockFP;
            if (isLeft)
            {
                // The left tree's left most leaf block FP is always the minimal FP:
                assert leafNodes.getLeafLP(leavesOffset) == minBlockFP;
                leftBlockFP = minBlockFP;
            }
            else
            {
                leftBlockFP = leafNodes.getLeafLP(leavesOffset);
                long delta = leftBlockFP - minBlockFP;
                assert leafNodes.numLeaves() == numLeaves || delta > 0 : "expected delta > 0; got numLeaves =" + numLeaves + " and delta=" + delta;
                writeBuffer.writeVLong(delta);
            }

            int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
            final int rightOffset = leavesOffset + numLeftLeafNodes;
            final int splitOffset = rightOffset - 1;

            int splitDim = leafNodes.getSplitDimension(splitOffset);
            BytesRef splitValue = leafNodes.getSplitValue(splitOffset);
            int address = splitValue.offset;

            //System.out.println("recursePack inner nodeID=" + nodeID + " splitDim=" + splitDim + " splitValue=" + new BytesRef(splitPackedValues, address, config.bytesPerDim));

            // find common prefix with last split value in this dim:
            int prefix = FutureArrays.mismatch(splitValue.bytes, address, address + bytesPerDim, lastSplitValues,
                                               splitDim * bytesPerDim, splitDim * bytesPerDim + bytesPerDim);
            if (prefix == -1)
            {
                prefix = bytesPerDim;
            }

            //System.out.println("writeNodeData nodeID=" + nodeID + " splitDim=" + splitDim + " numDims=" + numDims + " config.bytesPerDim=" + config.bytesPerDim + " prefix=" + prefix);

            int firstDiffByteDelta;
            if (prefix < bytesPerDim)
            {
                //System.out.println("  delta byte cur=" + Integer.toHexString(splitPackedValues[address+prefix]&0xFF) + " prev=" + Integer.toHexString(lastSplitValues[splitDim * config.bytesPerDim + prefix]&0xFF) + " negated?=" + negativeDeltas[splitDim]);
                firstDiffByteDelta = (splitValue.bytes[address + prefix] & 0xFF) - (lastSplitValues[splitDim * bytesPerDim + prefix] & 0xFF);
                if (negativeDeltas[splitDim])
                {
                    firstDiffByteDelta = -firstDiffByteDelta;
                }
                //System.out.println("  delta=" + firstDiffByteDelta);
                assert firstDiffByteDelta > 0;
            }
            else
            {
                firstDiffByteDelta = 0;
            }

            // pack the prefix, splitDim and delta first diff byte into a single vInt:
            int code = (firstDiffByteDelta * (1 + bytesPerDim) + prefix) * numIndexDims + splitDim;

            //System.out.println("  code=" + code);
            //System.out.println("  splitValue=" + new BytesRef(splitPackedValues, address, config.bytesPerDim));

            writeBuffer.writeVInt(code);

            // write the split value, prefix coded vs. our parent's split value:
            int suffix = bytesPerDim - prefix;
            byte[] savSplitValue = new byte[suffix];
            if (suffix > 1)
            {
                writeBuffer.writeBytes(splitValue.bytes, address + prefix + 1, suffix - 1);
            }

            byte[] cmp = lastSplitValues.clone();

            System.arraycopy(lastSplitValues, splitDim * bytesPerDim + prefix, savSplitValue, 0, suffix);

            // copy our split value into lastSplitValues for our children to prefix-code against
            System.arraycopy(splitValue.bytes, address + prefix, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

            int numBytes = appendBlock(writeBuffer, blocks);

            // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into the right sub-tree we can
            // quickly seek to its starting point
            int idxSav = blocks.size();
            blocks.add(null);

            boolean savNegativeDelta = negativeDeltas[splitDim];
            negativeDeltas[splitDim] = true;


            int leftNumBytes = recursePackIndex(writeBuffer, leafNodes, leftBlockFP, blocks, lastSplitValues, negativeDeltas, true,
                                                leavesOffset, numLeftLeafNodes);

            if (numLeftLeafNodes != 1)
            {
                writeBuffer.writeVInt(leftNumBytes);
            }
            else
            {
                assert leftNumBytes == 0 : "leftNumBytes=" + leftNumBytes;
            }

            byte[] bytes2 = writeBuffer.toArrayCopy();
            writeBuffer.reset();
            // replace our placeholder:
            blocks.set(idxSav, bytes2);

            negativeDeltas[splitDim] = false;
            int rightNumBytes = recursePackIndex(writeBuffer, leafNodes, leftBlockFP, blocks, lastSplitValues, negativeDeltas, false,
                                                 rightOffset, numLeaves - numLeftLeafNodes);

            negativeDeltas[splitDim] = savNegativeDelta;

            // restore lastSplitValues to what caller originally passed us:
            System.arraycopy(savSplitValue, 0, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

            assert Arrays.equals(lastSplitValues, cmp);

            return numBytes + bytes2.length + leftNumBytes + rightNumBytes;
        }
    }

    private int getNumLeftLeafNodes(int numLeaves) {
        assert numLeaves > 1: "getNumLeftLeaveNodes() called with " + numLeaves;
        // return the level that can be filled with this number of leaves
        int lastFullLevel = 31 - Integer.numberOfLeadingZeros(numLeaves);
        // how many leaf nodes are in the full level
        int leavesFullLevel = 1 << lastFullLevel;
        // half of the leaf nodes from the full level goes to the left
        int numLeftLeafNodes = leavesFullLevel / 2;
        // leaf nodes that do not fit in the full level
        int unbalancedLeafNodes = numLeaves - leavesFullLevel;
        // distribute unbalanced leaf nodes
        numLeftLeafNodes += Math.min(unbalancedLeafNodes, numLeftLeafNodes);
        // we should always place unbalanced leaf nodes on the left
        assert numLeftLeafNodes >= numLeaves - numLeftLeafNodes && numLeftLeafNodes <= 2L * (numLeaves - numLeftLeafNodes);
        return numLeftLeafNodes;
    }

    public void write(BlockTerms.Reader reader)
    {
        final LongArray leafPostingFPs = reader.blockPostingOffsetsReader.open();

        BKDTreeLeafNodes leafNodes = new BKDTreeLeafNodes() {
            @Override
            public long getLeafLP(int index) {
                return leafPostingFPs.get(index);
            }

            @Override
            public BytesRef getSplitValue(int index) {
                scratchBytesRef1.bytes = leafBlockStartValues.get(index);
                return scratchBytesRef1;
            }

            @Override
            public int getSplitDimension(int index) {
                return 0;
            }

            @Override
            public int numLeaves() {
                return reader.meta.numPostingBlocks;
            }
        };
    }

    private interface BKDTreeLeafNodes {
        /** number of leaf nodes */
        int numLeaves();
        /**
         * pointer to the leaf node previously written. Leaves are order from left to right, so leaf at
         * {@code index} 0 is the leftmost leaf and the leaf at {@code numleaves()} -1 is the rightmost
         * leaf
         */
        long getLeafLP(int index);
        /**
         * split value between two leaves. The split value at position n corresponds to the leaves at (n
         * -1) and n.
         */
        BytesRef getSplitValue(int index);
        /**
         * split dimension between two leaves. The split dimension at position n corresponds to the
         * leaves at (n -1) and n.
         */
        int getSplitDimension(int index);
    }

    /** Appends the current contents of writeBuffer as another block on the growing in-memory file */
    private int appendBlock(ByteBuffersDataOutput writeBuffer, List<byte[]> blocks) {
        byte[] block = writeBuffer.toArrayCopy();
        blocks.add(block);
        writeBuffer.reset();
        return block.length;
    }
}
