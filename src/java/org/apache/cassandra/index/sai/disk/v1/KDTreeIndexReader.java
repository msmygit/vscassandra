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

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.packed.PackedLongValues;

public class KDTreeIndexReader
{
    final int leafNodeOffset;
    final int numLeaves;
    final IndexTree indexTree;
    //final byte[][] bytes;

    public KDTreeIndexReader(int numLeaves)
    {
        this.numLeaves = numLeaves;
        this.leafNodeOffset = numLeaves;
        indexTree = new IndexTree(null);
        //this.bytes = bytes;
    }

    public IndexTree indexTree()
    {
        return indexTree;
    }

    private int getTreeDepth() {
        // First +1 because all the non-leave nodes makes another power
        // of 2; e.g. to have a fully balanced tree with 4 leaves you
        // need a depth=3 tree:

        // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
        // with 5 leaves you need a depth=4 tree:
        return MathUtil.log(numLeaves, 2) + 2;
    }

    public class IndexTree implements Cloneable {
        private int nodeID;
        // level is 1-based so that we can do level-1 w/o checking each time:
        private int level;
        final IndexInput input;
//        private final byte[][] splitPackedValueStack;
//        // used to read the packed tree off-heap
//        private final IndexInput in;
//        // holds the minimum (left most) leaf block file pointer for each level we've recursed to:
//        private final long[] leafBlockFPStack;
//        // holds the address, in the off-heap index, of the right-node of each level:
//        private final int[] rightNodePositions;
//        // holds the splitDim for each level:
//        private final int[] splitDims;
//        // true if the per-dim delta we read for the node at this level is a negative offset vs. the last split on this dim; this is a packed
//        // 2D array, i.e. to access array[level][dim] you read from negativeDeltas[level*numDims+dim].  this will be true if the last time we
//        // split on this dimension, we next pushed to the left sub-tree:
//        private final boolean[] negativeDeltas;
//        // holds the packed per-level split values; the intersect method uses this to save the cell min/max as it recurses:
//        private final byte[][] splitValuesStack;
//        // scratch value to return from getPackedValue:
//        private final BytesRef scratch;

        public IndexTree(IndexInput input) {
            this(input, 1, 1);
            // read root node
            //readNodeData(false);
        }

        private IndexTree(IndexInput input, int nodeID, int level) {
            this.input = input;
            int treeDepth = getTreeDepth();
            this.nodeID = nodeID;
            this.level = level;
//            splitPackedValueStack = new byte[treeDepth+1][];
//            this.nodeID = nodeID;
//            this.level = level;
//            splitPackedValueStack[level] = new byte[packedIndexBytesLength];
//            leafBlockFPStack = new long[treeDepth+1];
//            rightNodePositions = new int[treeDepth+1];
//            splitValuesStack = new byte[treeDepth+1][];
//            splitDims = new int[treeDepth+1];
//            negativeDeltas = new boolean[numIndexDims*(treeDepth+1)];
//            this.in = in;
//            splitValuesStack[0] = new byte[packedIndexBytesLength];
//            scratch = new BytesRef();
//            scratch.length = bytesPerDim;
        }

        public void pushLeft() {
            nodeID *= 2;
            level++;
            //readNodeData(true);
        }

//        /** Clone, but you are not allowed to pop up past the point where the clone happened. */
//        @Override
//        public BKDReader.IndexTree clone() {
//            BKDReader.IndexTree index = new BKDReader.IndexTree(in.clone(), nodeID, level);
//            // copy node data
//            index.splitDim = splitDim;
//            index.leafBlockFPStack[level] = leafBlockFPStack[level];
//            index.rightNodePositions[level] = rightNodePositions[level];
//            index.splitValuesStack[index.level] = splitValuesStack[index.level].clone();
//            System.arraycopy(negativeDeltas, level*numIndexDims, index.negativeDeltas, level*numIndexDims, numIndexDims);
//            index.splitDims[level] = splitDims[level];
//            return index;
//        }

        public void pushRight() {
//            final int nodePosition = rightNodePositions[level];
//            assert nodePosition >= in.getFilePointer() : "nodePosition = " + nodePosition + " < currentPosition=" + in.getFilePointer();
            nodeID = nodeID * 2 + 1;
            level++;
//            try {
//                in.seek(nodePosition);
//            } catch (IOException e) {
//                throw new UncheckedIOException(e);
//            }
//            //readNodeData(false);
        }

        public void pop() {
            nodeID /= 2;
            level--;
//            splitDim = splitDims[level];
//            //System.out.println("  pop nodeID=" + nodeID);
        }

        public boolean isLeafNode() {
            return nodeID >= leafNodeOffset;
        }

        public boolean nodeExists() {
            return nodeID - leafNodeOffset < leafNodeOffset;
        }

        public int getNodeID() {
            return nodeID;
        }

//        public byte[] getSplitPackedValue() {
//            assert isLeafNode() == false;
//            assert splitPackedValueStack[level] != null: "level=" + level;
//            return splitPackedValueStack[level];
//        }

//        /** Only valid after pushLeft or pushRight, not pop! */
//        public int getSplitDim() {
//            assert isLeafNode() == false;
//            return splitDim;
//        }

//        /** Only valid after pushLeft or pushRight, not pop! */
//        public BytesRef getSplitDimValue() {
//            assert isLeafNode() == false;
//            scratch.bytes = splitValuesStack[level];
//            scratch.offset = splitDim * bytesPerDim;
//            return scratch;
//        }

//        /** Only valid after pushLeft or pushRight, not pop! */
//        public long getLeafBlockFP() {
//            assert isLeafNode(): "nodeID=" + nodeID + " is not a leaf";
//            return leafBlockFPStack[level];
//        }

        /** Return the number of leaves below the current node. */
        public int getNumLeaves() {
            int leftMostLeafNode = nodeID;
            while (leftMostLeafNode < leafNodeOffset) {
                leftMostLeafNode = leftMostLeafNode * 2;
            }
            int rightMostLeafNode = nodeID;
            while (rightMostLeafNode < leafNodeOffset) {
                rightMostLeafNode = rightMostLeafNode * 2 + 1;
            }
            final int numLeaves;
            if (rightMostLeafNode >= leftMostLeafNode) {
                // both are on the same level
                numLeaves = rightMostLeafNode - leftMostLeafNode + 1;
            } else {
                // left is one level deeper than right
                numLeaves = rightMostLeafNode - leftMostLeafNode + 1 + leafNodeOffset;
            }
            assert numLeaves == getNumLeavesSlow(nodeID) : numLeaves + " " + getNumLeavesSlow(nodeID);
            return numLeaves;
        }

        // for assertions
        private int getNumLeavesSlow(int node) {
            if (node >= 2 * leafNodeOffset) {
                return 0;
            } else if (node >= leafNodeOffset) {
                return 1;
            } else {
                final int leftCount = getNumLeavesSlow(node * 2);
                final int rightCount = getNumLeavesSlow(node * 2 + 1);
                return leftCount + rightCount;
            }
        }

//        private void readNodeData(boolean isLeft) {
//            if (splitPackedValueStack[level] == null) {
//                splitPackedValueStack[level] = new byte[packedIndexBytesLength];
//            }
//            System.arraycopy(negativeDeltas, (level-1)*numIndexDims, negativeDeltas, level*numIndexDims, numIndexDims);
//            assert splitDim != -1;
//            negativeDeltas[level*numIndexDims+splitDim] = isLeft;
//
//            try {
//                leafBlockFPStack[level] = leafBlockFPStack[level - 1];
//
//                // read leaf block FP delta
//                if (isLeft == false) {
//                    leafBlockFPStack[level] += in.readVLong();
//                }
//
//                if (isLeafNode()) {
//                    splitDim = -1;
//                } else {
//
//                    // read split dim, prefix, firstDiffByteDelta encoded as int:
//                    int code = in.readVInt();
//                    splitDim = code % numIndexDims;
//                    splitDims[level] = splitDim;
//                    code /= numIndexDims;
//                    int prefix = code % (1 + bytesPerDim);
//                    int suffix = bytesPerDim - prefix;
//
//                    if (splitValuesStack[level] == null) {
//                        splitValuesStack[level] = new byte[packedIndexBytesLength];
//                    }
//                    System.arraycopy(splitValuesStack[level - 1], 0, splitValuesStack[level], 0, packedIndexBytesLength);
//                    if (suffix > 0) {
//                        int firstDiffByteDelta = code / (1 + bytesPerDim);
//                        if (negativeDeltas[level * numIndexDims + splitDim]) {
//                            firstDiffByteDelta = -firstDiffByteDelta;
//                        }
//                        int oldByte = splitValuesStack[level][splitDim * bytesPerDim + prefix] & 0xFF;
//                        splitValuesStack[level][splitDim * bytesPerDim + prefix] = (byte) (oldByte + firstDiffByteDelta);
//                        in.readBytes(splitValuesStack[level], splitDim * bytesPerDim + prefix + 1, suffix - 1);
//                    } else {
//                        // our split value is == last split value in this dim, which can happen when there are many duplicate values
//                    }
//
//                    int leftNumBytes;
//                    if (nodeID * 2 < leafNodeOffset) {
//                        leftNumBytes = in.readVInt();
//                    } else {
//                        leftNumBytes = 0;
//                    }
//                    rightNodePositions[level] = Math.toIntExact(in.getFilePointer()) + leftNumBytes;
//                }
//            } catch (IOException e) {
//                throw new UncheckedIOException(e);
//            }
//        }
    }
}
