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
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;

public class KDTreeIndexWriter
{
    final IndexOutput output;
    final List<byte[]> leafBlockStartValues;
    public byte[][] bytes = new byte[10][];

    public KDTreeIndexWriter(IndexOutput output, List<byte[]> leafBlockStartValues)
    {
        this.output = output;
        this.leafBlockStartValues = leafBlockStartValues;
    }

    public long finish() throws IOException
    {
        long indexFP = output.getFilePointer();

        int numInnerNodes = leafBlockStartValues.size();

        //System.out.println("BKDW: now rotate numInnerNodes=" + numInnerNodes + " leafBlockStarts=" + leafBlockStartValues.size());

        //byte[] index = new byte[(1 + numInnerNodes) * (1 + bytesPerDim)];
        rotateToTree(1, 0, numInnerNodes, bytes, leafBlockStartValues);

        for (int x=0; x < bytes.length; x++)
        {
            if (bytes[x] != null)
            {
                int val = NumericUtils.sortableBytesToInt(bytes[x], 0);
                System.out.println(x+"="+val);
            }
        }
//        long[] arr = new long[leafBlockFPs.size()];
//        for (int i = 0; i < leafBlockFPs.size(); i++)
//        {
//            arr[i] = leafBlockFPs.get(i);
//        }
        //writeIndex(out, maxPointsInLeafNode, arr, index);
        return indexFP;
    }

    public void rotateToTree(int nodeID, int offset, int count, byte[][] bytes, List<byte[]> leafBlockStartValues)
    {
        if (count == 1)
        {
            // Leaf index node
            System.out.println("  leaf index node");
            System.out.println("  index[" + nodeID + "] = blockStartValues[" + offset + "]");

            if (bytes.length <= nodeID)
            {
                bytes = ArrayUtil.grow(bytes, nodeID + 1);
            }
            bytes[nodeID] = leafBlockStartValues.get(offset);

            //System.arraycopy(leafBlockStartValues.get(offset), 0, index, nodeID * (1 + bytesPerDim) + 1, bytesPerDim);
        }
        else if (count > 1)
        {
            // Internal index node: binary partition of count
            int countAtLevel = 1;
            int totalCount = 0;
            while (true)
            {
                int countLeft = count - totalCount;

                System.out.println("    cycle countLeft=" + countLeft + " coutAtLevel=" + countAtLevel);

                if (countLeft <= countAtLevel)
                {
                    // This is the last level, possibly partially filled:
                    int lastLeftCount = Math.min(countAtLevel / 2, countLeft);
                    assert lastLeftCount >= 0;
                    int leftHalf = (totalCount - 1) / 2 + lastLeftCount;

                    int rootOffset = offset + leftHalf;

                    System.out.println("  last left count " + lastLeftCount);
                    System.out.println("  leftHalf " + leftHalf + " rightHalf=" + (count - leftHalf - 1));
                    System.out.println("  rootOffset=" + rootOffset);

                    bytes[nodeID] = leafBlockStartValues.get(rootOffset);

                    //System.arraycopy(leafBlockStartValues.get(rootOffset), 0, index, nodeID * (1 + bytesPerDim) + 1, bytesPerDim);

                    System.out.println("  index[" + nodeID + "] = blockStartValues[" + rootOffset + "]");

                    // Recurse left
                    rotateToTree(2 * nodeID, offset, leftHalf, bytes, leafBlockStartValues);

                    // Recurse right
                    rotateToTree(2 * nodeID + 1, rootOffset + 1, count - leftHalf - 1, bytes, leafBlockStartValues);
                    return;
                }
                totalCount += countAtLevel;
                countAtLevel *= 2;
            }
        }
        else
        {
            assert count == 0;
        }
    }

    private byte[] packIndex(long[] leafBlockFPs, BytesRefBuilder splitPackedValue) throws IOException
    {

        int numLeaves = leafBlockFPs.length;

        // Possibly rotate the leaf block FPs, if the index not fully balanced binary tree (only happens
        // if it was created by OneDimensionBKDWriter).  In this case the leaf nodes may straddle the two bottom
        // levels of the binary tree:
        if (numLeaves > 1)
        {
            int levelCount = 2;
            while (true)
            {
                if (numLeaves >= levelCount && numLeaves <= 2 * levelCount)
                {
                    int lastLevel = 2 * (numLeaves - levelCount);
                    assert lastLevel >= 0;
                    if (lastLevel != 0)
                    {
                        // Last level is partially filled, so we must rotate the leaf FPs to match.  We do this here, after loading
                        // at read-time, so that we can still delta code them on disk at write:
                        long[] newLeafBlockFPs = new long[numLeaves];
                        System.arraycopy(leafBlockFPs, lastLevel, newLeafBlockFPs, 0, leafBlockFPs.length - lastLevel);
                        System.arraycopy(leafBlockFPs, 0, newLeafBlockFPs, leafBlockFPs.length - lastLevel, lastLevel);
                        leafBlockFPs = newLeafBlockFPs;
                    }
                    break;
                }

                levelCount *= 2;
            }
        }

        /** Reused while packing the index */
        // TODO: replace with RAMIndexOutput because RAMOutputStream has synchronized/monitor locks
        RAMOutputStream writeBuffer = new RAMOutputStream();

        // This is the "file" we append the byte[] to:
        List<byte[]> blocks = new ArrayList<>();
        //byte[] lastSplitValues = new byte[bytesPerDim * numDims];
        BytesRefBuilder lastSplitValue = new BytesRefBuilder();
        //System.out.println("\npack index");
        int totalSize = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValue, 0l, blocks, 1, lastSplitValue, false);

        // Compact the byte[] blocks into single byte index:
        byte[] index = new byte[totalSize];
        int upto = 0;
        for (byte[] block : blocks)
        {
            System.arraycopy(block, 0, index, upto, block.length);
            upto += block.length;
        }
        assert upto == totalSize;

        return index;
    }

    private int recursePackIndex(RAMOutputStream writeBuffer,
                                 long[] leafBlockFPs,
                                 BytesRefBuilder splitPackedValue,
                                 long minBlockFP,
                                 List<byte[]> blocks,
                                 int nodeID,
                                 BytesRefBuilder lastSplitValue,
                                 boolean isLeft) throws IOException
    {
        if (nodeID >= leafBlockFPs.length)
        {
            int leafID = nodeID - leafBlockFPs.length;
            System.out.println("recursePack leaf nodeID=" + nodeID);

            // In the unbalanced case it's possible the left most node only has one child:
            if (leafID < leafBlockFPs.length)
            {
                long delta = leafBlockFPs[leafID] - minBlockFP;
                if (isLeft)
                {
                    assert delta == 0;
                    return 0;
                }
                else
                {
                    assert nodeID == 1 || delta > 0 : "nodeID=" + nodeID;
                    //writeBuffer.writeVLong(delta);
                    return appendBlock(writeBuffer, blocks);
                }
            }
            else
            {
                return 0;
            }
        }
        else
        {
            long leftBlockFP;
            if (isLeft == false)
            {
                leftBlockFP = getLeftMostLeafBlockFP(leafBlockFPs, nodeID);
                long delta = leftBlockFP - minBlockFP;

                assert nodeID == 1 || delta > 0;

                //writeBuffer.writeVLong(delta);
            }
            else
            {
                // The left tree's left most leaf block FP is always the minimal FP:
                leftBlockFP = minBlockFP;
            }

            //int address = nodeID * (1 + bytesPerDim);
            //int splitDim = splitPackedValues[address++] & 0xff;

            //System.out.println("recursePack inner nodeID=" + nodeID + " splitDim=" + splitDim + " splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

            // find common prefix with last split value
            int prefix = 0;
            int minLen = Math.min(splitPackedValue.length(), lastSplitValue.length());
            for (; prefix < minLen; prefix++)
            {
                if (splitPackedValue.byteAt(prefix) != lastSplitValue.byteAt(prefix))
                {
                    break;
                }
//                if (splitPackedValues[address + prefix] != lastSplitValues[splitDim * bytesPerDim + prefix])
//                {
//                    break;
//                }
            }
//            for (; prefix < bytesPerDim; prefix++)
//            {
//                if (splitPackedValues[address + prefix] != lastSplitValues[splitDim * bytesPerDim + prefix])
//                {
//                    break;
//                }
//            }

            System.out.println("writeNodeData nodeID=" + nodeID + " prefix=" + prefix);

            //int firstDiffByteDelta;
//            if (prefix < bytesPerDim)
//            {
//                //System.out.println("  delta byte cur=" + Integer.toHexString(splitPackedValues[address+prefix]&0xFF) + " prev=" + Integer.toHexString(lastSplitValues[splitDim * bytesPerDim + prefix]&0xFF) + " negated?=" + negativeDeltas[splitDim]);
//                firstDiffByteDelta = (splitPackedValues[address + prefix] & 0xFF) - (lastSplitValues[splitDim * bytesPerDim + prefix] & 0xFF);
//                if (negativeDeltas[splitDim])
//                {
//                    firstDiffByteDelta = -firstDiffByteDelta;
//                }
//                //System.out.println("  delta=" + firstDiffByteDelta);
//                assert firstDiffByteDelta > 0;
//            }
//            else
//            {
//                firstDiffByteDelta = 0;
//            }

            // pack the prefix, splitDim and delta first diff byte into a single vInt:
            // int code = (firstDiffByteDelta * (1 + bytesPerDim) + prefix) * numDims + splitDim;

            //System.out.println("  code=" + code);
            //System.out.println("  splitValue=" + new BytesRef(splitPackedValues, address, bytesPerDim));

            // writeBuffer.writeVInt(code);

            // write the split value, prefix coded vs. our parent's split value:
            //int suffix = bytesPerDim - prefix;
            //byte[] savSplitValue = new byte[suffix];

            int suffix = splitPackedValue.length() - prefix;
            if (suffix > 1)
            {
                writeBuffer.writeBytes(splitPackedValue.bytes(), 0, suffix);
            }

            // byte[] cmp = lastSplitValues.clone();

            // System.arraycopy(lastSplitValues, splitDim * bytesPerDim + prefix, savSplitValue, 0, suffix);

            // copy our split value into lastSplitValues for our children to prefix-code against
            // System.arraycopy(splitPackedValues, address + prefix, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

            int numBytes = appendBlock(writeBuffer, blocks);

            // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into the right sub-tree we can
            // quickly seek to its starting point
            int idxSav = blocks.size();
            blocks.add(null);

//            boolean savNegativeDelta = negativeDeltas[splitDim];
//            negativeDeltas[splitDim] = true;

            int leftNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValue, leftBlockFP, blocks, 2 * nodeID, lastSplitValue, true);

            if (nodeID * 2 < leafBlockFPs.length)
            {
                //writeBuffer.writeVInt(leftNumBytes);
            }
            else
            {
                assert leftNumBytes == 0 : "leftNumBytes=" + leftNumBytes;
            }
//            int numBytes2 = Math.toIntExact(writeBuffer.getFilePointer());
//            byte[] bytes2 = new byte[numBytes2];
//            writeBuffer.writeTo(bytes2, 0);
//            writeBuffer.reset();
//            // replace our placeholder:
//            blocks.set(idxSav, bytes2);

//            negativeDeltas[splitDim] = false;
            int rightNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValue, leftBlockFP, blocks, 2 * nodeID + 1, lastSplitValue, false);

            // negativeDeltas[splitDim] = savNegativeDelta;

            // restore lastSplitValues to what caller originally passed us:
            //System.arraycopy(savSplitValue, 0, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

            //assert Arrays.equals(lastSplitValues, cmp);

            //return numBytes + numBytes2 + leftNumBytes + rightNumBytes;
            return numBytes + leftNumBytes + rightNumBytes;
        }
    }

    private long getLeftMostLeafBlockFP(long[] leafBlockFPs, int nodeID)
    {
        // TODO: can we do this cheaper, e.g. a closed form solution instead of while loop?  Or
        // change the recursion while packing the index to return this left-most leaf block FP
        // from each recursion instead?
        //
        // Still, the overall cost here is minor: this method's cost is O(log(N)), and while writing
        // we call it O(N) times (N = number of leaf blocks)
        while (nodeID < leafBlockFPs.length)
        {
            nodeID *= 2;
        }
        int leafID = nodeID - leafBlockFPs.length;
        long result = leafBlockFPs[leafID];
        if (result < 0)
        {
            throw new AssertionError(result + " for leaf " + leafID);
        }
        return result;
    }

    private int appendBlock(RAMOutputStream writeBuffer, List<byte[]> blocks) throws IOException
    {
        int pos = Math.toIntExact(writeBuffer.getFilePointer());
        byte[] bytes = new byte[pos];
        writeBuffer.writeTo(bytes, 0);
        writeBuffer.reset();
        blocks.add(bytes);
        return pos;
    }
}
