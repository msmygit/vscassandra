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

/*
 * Copyright (C) 2022 Apache Lucene
 *
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

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.MathUtil;

/**
 * Binary tree based on the Lucene 8.x kdtree.
 * <p>
 * The index is not prefix compressed yet.
 * <p>
 * The basic code has been copied from the Lucene bkd tree and modified.
 */
public class BinaryTree
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public interface IntersectVisitor
    {
        /** Called for non-leaf cells to test how the cell relates to the query, to
         *  determine how to further recurse down the tree. */
        PointValues.Relation compare(BytesRef minPackedValue, BytesRef maxPackedValue);
    }

    /**
     * Callback used to write the upper level postings.
     */
    public interface BinaryTreeTraversalCallback
    {
        /**
         * @param leafID Leaf block id
         * @param leafNodeID Binary tree node id
         * @param pathToRoot Binary tree node ids seen traversing from the root node
         */
        void onLeaf(int leafID, int leafNodeID, IntArrayList pathToRoot);
    }

    /**
     * Traverses the binary tree.
     */
    @NotThreadSafe
    public static class Reader implements Closeable
    {
        final IndexInput input;
        private final int[] rightNodePositions; // index file positions during traversal
        private final int[] leafOrdinals; // leaf block ids during traversal
        final int leafNodeOffset; // node id at which leaf nodes start
        // total number of points
        final long pointCount; // number of points in the binary tree
        // level is 1-based so that we can do level-1 w/o checking each time:
        protected int level; // current level
        protected int nodeID; // current binary tree node id

        private final BytesRef[] splitBufferValueStack; // used as a buffer stack by level to copy into during traversal
        private final BytesRef[] splitValuesStack; // node bytes values while traversing
        private final BytesRef scratch = new BytesRef(); // reusable bytes

        public Reader(int numLeaves, long pointCount, IndexInput input) throws IOException
        {
            this.pointCount = pointCount;
            this.input = input;

            this.leafNodeOffset = numLeaves;
            this.nodeID = 1;
            this.level = 1;

            final int treeDepth = getTreeDepth(numLeaves);

            this.splitBufferValueStack = new BytesRef[treeDepth + 1];
            this.splitValuesStack = new BytesRef[treeDepth + 1];
            this.rightNodePositions = new int[treeDepth + 1];
            this.leafOrdinals = new int[treeDepth + 1];

            readNodeData();
        }

        /**
         * Change the node id to 0..N leaf block id.
         */
        public int getBlockID()
        {
            return leafOrdinals[level];
        }

        @Override
        public void close()
        {
            FileUtils.closeQuietly(input);
        }

        public static void copyBytes(BytesRef source, BytesRef dest)
        {
            dest.bytes = ArrayUtil.grow(dest.bytes, source.length + dest.offset);
            dest.length = source.length;
            System.arraycopy(source.bytes, source.offset, dest.bytes, dest.offset, source.length);
        }

        public BytesRef getSplitDimValue() {
            assert !isLeafNode();
            scratch.bytes = splitValuesStack[level].bytes;
            scratch.length = splitValuesStack[level].length;
            scratch.offset = splitValuesStack[level].offset;
            return scratch;
        }

        // used as a buffer stack by level to copy into during traversal
        public BytesRef getSplitPackedValue()
        {
            assert !isLeafNode();
            assert splitBufferValueStack[level] != null: "level=" + level;
            return splitBufferValueStack[level];
        }

        public void traverse(IntArrayList pathToRoot,
                             BinaryTreeTraversalCallback callback) throws IOException
        {
            final int nodeID = getNodeID();

            if (logger.isTraceEnabled())
                logger.trace("traverse nodeID="+nodeID+" isLeafNode="+isLeafNode());

            if (isLeafNode())
            {
                // In the unbalanced case it's possible the left most node only has one child:
                if (nodeExists())
                {
                    if (logger.isTraceEnabled())
                        logger.trace("onLeaf nodeID="+getNodeID());

                    int blockID = getBlockID();
                    callback.onLeaf(blockID, nodeID, pathToRoot);
                }
            }
            else
            {
                final IntArrayList currentPath = new IntArrayList();
                currentPath.addAll(pathToRoot);
                currentPath.add(nodeID);

                pushLeft();
                traverse(currentPath, callback);
                pop();

                pushRight();
                traverse(currentPath, callback);
                pop();
            }
        }

        public void pop()
        {
            if (nodeID < 1)
                throw new IllegalStateException("Node id="+nodeID+" is less than 1.");

            nodeID /= 2;
            level--;

            if (logger.isTraceEnabled())
                logger.trace("  pop nodeID=" + nodeID);
        }

        public boolean isLeafNode()
        {
            return nodeID >= leafNodeOffset;
        }

        public boolean nodeExists()
        {
            return nodeID - leafNodeOffset < leafNodeOffset;
        }

        public int getNodeID()
        {
            return nodeID;
        }

        public void pushRight() throws IOException
        {
            final int nodePosition = rightNodePositions[level];
            assert nodePosition >= input.getFilePointer() : "nodePosition = " + nodePosition + " < currentPosition=" + input.getFilePointer();
            nodeID = nodeID * 2 + 1;
            level++;
            input.seek(nodePosition);
            readNodeData();
        }

        public void pushLeft() throws IOException
        {
            nodeID *= 2;
            level++;
            readNodeData();
        }

        private static int getTreeDepth(int numLeaves)
        {
            // First +1 because all the non-leave nodes makes another power
            // of 2; e.g. to have a fully balanced tree with 4 leaves you
            // need a depth=3 tree:

            // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
            // with 5 leaves you need a depth=4 tree:
            return MathUtil.log(numLeaves, 2) + 2;
        }

        private void readNodeData() throws IOException
        {
            if (splitBufferValueStack[level] == null)
                splitBufferValueStack[level] = new BytesRef(new byte[10]);

            if (logger.isTraceEnabled())
                logger.trace("readNodeData nodeID=" + nodeID + " isLeafNode=" + isLeafNode() + " leafNodeOffset=" + leafNodeOffset + " fp=" + input.getFilePointer() + " level=" + level);

            if (isLeafNode())
            {
                this.leafOrdinals[level] = input.readVInt();
                return;
            }

            final int len = input.readVInt();

            if (splitValuesStack[level] == null)
                splitValuesStack[level] = new BytesRef(new byte[len]);
            else
                splitValuesStack[level].bytes = ArrayUtil.grow(splitValuesStack[level].bytes, len);

            splitValuesStack[level].length = len;

            if (logger.isTraceEnabled())
                logger.trace("  readNodeData len="+len);

            // read the node bytes value
            input.readBytes(splitValuesStack[level].bytes, 0, len);
            if (logger.isTraceEnabled())
                logger.trace("  nodeID="+nodeID+" readNodeData");

            final int leftNumBytes = input.readVInt();

            if (logger.isTraceEnabled())
                logger.trace("  leftNumBytes="+leftNumBytes+" nodeID*2="+(nodeID * 2)+" leafNodeOffset="+leafNodeOffset);

            rightNodePositions[level] = Math.toIntExact(input.getFilePointer()) + leftNumBytes;

            if (logger.isTraceEnabled())
                logger.trace("  rightNodePositions=" + Arrays.toString(rightNodePositions));
        }
    }

    @NotThreadSafe
    public static class Writer
    {
        private final BytesRefBuilder spare = new BytesRefBuilder();

        public Writer()
        {
        }

        public void finish(BytesRefArray minBlockTerms, IndexOutput output) throws IOException
        {
            ByteBuffersDataOutput writeBuffer = ByteBuffersDataOutput.newResettableInstance();
            List<byte[]> blocks = new ArrayList<>();

            int totalSize = recurseIndex(minBlockTerms, 0, minBlockTerms.size(), writeBuffer, blocks);

            byte[] index = new byte[totalSize];
            int upto = 0;
            for (byte[] block : blocks)
            {
                System.arraycopy(block, 0, index, upto, block.length);
                upto += block.length;
            }
            output.writeBytes(index, 0, index.length);
        }

        private int appendBlock(ByteBuffersDataOutput writeBuffer, List<byte[]> blocks)
        {
            byte[] block = writeBuffer.toArrayCopy();
            blocks.add(block);
            writeBuffer.reset();
            return block.length;
        }

        private int recurseIndex(BytesRefArray minBlockTerms,
                                 int leavesOffset,
                                 int numLeaves,
                                 ByteBuffersDataOutput writeBuffer,
                                 List<byte[]> blocks) throws IOException
        {
            if (logger.isTraceEnabled())
                logger.trace("recurseIndex leavesOffset="+leavesOffset+" numLeaves="+numLeaves);

            if (numLeaves == 1)
            {
                writeBuffer.writeVInt(leavesOffset); // leaf ordinal
                return appendBlock(writeBuffer, blocks);
            }
            else
            {
                final int numLeftLeafNodes = getNumLeftLeafNodes(numLeaves);
                final int rightOffset = leavesOffset + numLeftLeafNodes;

                if (logger.isTraceEnabled())
                    logger.trace("  rightOffset="+rightOffset);

                assert rightOffset >= 1;
                minBlockTerms.get(spare, rightOffset); // TODO: in lucene 8.x splitOffset is used

                // write term bytes for this node
                writeBuffer.writeVInt(spare.get().length);
                writeBuffer.writeBytes(spare.get().bytes, spare.get().offset, spare.get().length);

                final int numBytes = appendBlock(writeBuffer, blocks);

                final int idxSav = blocks.size();
                blocks.add(null);

                final int leftNumBytes = recurseIndex(minBlockTerms, leavesOffset, numLeftLeafNodes, writeBuffer, blocks);

                if (logger.isTraceEnabled())
                    logger.trace("  leftNumBytes="+leftNumBytes);

                writeBuffer.writeVInt(leftNumBytes);

                byte[] bytes2 = writeBuffer.toArrayCopy();
                writeBuffer.reset();
                // replace our placeholder:
                blocks.set(idxSav, bytes2);

                final int rightNumBytes = recurseIndex(minBlockTerms, rightOffset, numLeaves - numLeftLeafNodes, writeBuffer, blocks);

                return numBytes + bytes2.length + leftNumBytes + rightNumBytes;
            }
        }

        private int getNumLeftLeafNodes(int numLeaves)
        {
            assert numLeaves > 1 : "getNumLeftLeaveNodes() called with " + numLeaves;
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
    }
}
