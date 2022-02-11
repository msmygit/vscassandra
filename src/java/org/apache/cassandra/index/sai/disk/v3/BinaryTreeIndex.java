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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.v1.kdtree.TraversingBKDReader;
import org.apache.lucene.util.MathUtil;

import static com.google.common.base.Preconditions.checkArgument;

public class BinaryTreeIndex
{
    public static final int MAX_POINTS = 1024;

    public interface PointTree
    {
        /**
         * Move to the first child node and return {@code true} upon success. Returns {@code false} for
         * leaf nodes and {@code true} otherwise.
         */
        boolean moveToChild() throws IOException;

        /**
         * Move to the next sibling node and return {@code true} upon success. Returns {@code false} if
         * the current node has no more siblings.
         */
        boolean moveToSibling() throws IOException;

        /**
         * Move to the parent node and return {@code true} upon success. Returns {@code false} for the
         * root node and {@code true} otherwise.
         */
        boolean moveToParent() throws IOException;

        /** Return the number of points below the current node. */
        long size();
    }

    interface IndexTreeTraversalCallback
    {
        void onLeaf(int leafNodeID, IntArrayList pathToRoot);
    }

    public static class BKDPointTree implements PointTree
    {
        // number of leaves
        final int leafNodeOffset;
        // total number of points
        final long pointCount;
        // last node might not be fully populated
        private final int lastLeafNodePointCount;
        // right most leaf node ID
        private final int rightMostLeafNode;
        // during clone, the node root can be different to 1
        private final int nodeRoot;
        final Multimap<Integer, Integer> nodeToChildLeaves = HashMultimap.create();
        // level is 1-based so that we can do level-1 w/o checking each time:
        protected int level;
        protected int nodeID;

        public BKDPointTree(int numLeaves, long pointCount)
        {
            this(numLeaves, pointCount, 1, 1);
        }

        public BKDPointTree(int numLeaves, long pointCount, int nodeID, int level)
        {
            this.pointCount = pointCount;
            int treeDepth = getTreeDepth(numLeaves);
            this.leafNodeOffset = numLeaves;
            this.nodeRoot = nodeID;
            this.nodeID = nodeID;
            this.level = level;

            this.rightMostLeafNode = (1 << treeDepth - 1) - 1;
            int lastLeafNodePointCount = Math.toIntExact(pointCount % MAX_POINTS);
            this.lastLeafNodePointCount = lastLeafNodePointCount == 0 ? MAX_POINTS : lastLeafNodePointCount;
        }

        public void traverse(IndexTreeTraversalCallback callback,
                             IntArrayList pathToRoot)
        {
            if (isLeafNode())
            {
                // In the unbalanced case it's possible the left most node only has one child:
                if (nodeExists())
                {
                    callback.onLeaf(nodeID, pathToRoot);
                }
            }
            else
            {
                final IntArrayList currentPath = new IntArrayList();
                currentPath.addAll(pathToRoot);
                currentPath.add(nodeID);

                pushLeft();
                traverse(callback, currentPath);
                pop();

                pushRight();
                traverse(callback, currentPath);
                pop();
            }
        }

        private boolean isLevelEligibleForPostingList(int level)
        {
            return level > 1 && level % 3 == 0;
        }

        public void onLeaf(int leafNodeID, IntArrayList pathToRoot)
        {
            System.out.println("onLeaf leafNodeID="+leafNodeID+" pathToRoot="+pathToRoot);
            checkArgument(!pathToRoot.containsInt(leafNodeID));
            checkArgument(pathToRoot.isEmpty() || leafNodeID > pathToRoot.get(pathToRoot.size() - 1));

            for (int i = 0; i < pathToRoot.size(); i++)
            {
                final int level = i + 1;
                if (isLevelEligibleForPostingList(level))
                {
                    final int nodeID = pathToRoot.get(i);
                    nodeToChildLeaves.put(nodeID, leafNodeID);
                }
            }
        }

        public void traverse(IntArrayList pathToRoot)
        {
            System.out.println("nodeID=" + nodeID);
            if (moveToChild())
            {
                do
                {
                    if (isLeafNode())
                    {
                        // In the unbalanced case it's possible the left most node only has one child:
                        if (nodeExists())
                        {
                            onLeaf(nodeID, pathToRoot);
                        }
                    }
                    else
                    {
                        final IntArrayList currentPath = new IntArrayList();
                        currentPath.addAll(pathToRoot);
                        currentPath.add(nodeID);
                        traverse(currentPath);
                    }
                } while (moveToSibling());
                moveToParent();
            }
        }

        private boolean isLeafNode()
        {
            return nodeID >= leafNodeOffset;
        }

        @Override
        public boolean moveToChild()
        {
            if (isLeafNode())
            {
                return false;
            }
            pushLeft();
            return true;
        }

        private void pushLeft()
        {
            nodeID *= 2;
            level++;
        }

        private boolean isRootNode()
        {
            return nodeID == nodeRoot;
        }

        private boolean isLeftNode()
        {
            return (nodeID & 1) == 0;
        }

        private boolean nodeExists()
        {
            return nodeID - leafNodeOffset < leafNodeOffset;
        }

        @Override
        public boolean moveToSibling()
        {
            if (isLeftNode() == false || isRootNode())
                return false;
            pop();
            pushRight();
            assert nodeExists();
            return true;
        }

        private void pushRight()
        {
            nodeID = 2 * nodeID + 1;
            level++;
        }

        private void pop()
        {
            nodeID /= 2;
            level--;
        }

        @Override
        public boolean moveToParent()
        {
            if (isRootNode())
                return false;
            pop();
            return true;
        }

        @Override
        public long size()
        {
            int leftMostLeafNode = nodeID;
            while (leftMostLeafNode < leafNodeOffset)
            {
                leftMostLeafNode = leftMostLeafNode * 2;
            }
            int rightMostLeafNode = nodeID;
            while (rightMostLeafNode < leafNodeOffset)
            {
                rightMostLeafNode = rightMostLeafNode * 2 + 1;
            }
            final int numLeaves;
            if (rightMostLeafNode >= leftMostLeafNode)
            {
                // both are on the same level
                numLeaves = rightMostLeafNode - leftMostLeafNode + 1;
            }
            else
            {
                // left is one level deeper than right
                numLeaves = rightMostLeafNode - leftMostLeafNode + 1 + leafNodeOffset;
            }
            assert numLeaves == getNumLeavesSlow(nodeID) : numLeaves + " " + getNumLeavesSlow(nodeID);

            // size for an unbalanced tree.
            return rightMostLeafNode == this.rightMostLeafNode
                   ? (long) (numLeaves - 1) * MAX_POINTS + lastLeafNodePointCount
                   : (long) numLeaves * MAX_POINTS;
        }

        // for assertions
        private int getNumLeavesSlow(int node)
        {
            if (node >= 2 * leafNodeOffset)
                return 0;
            else if (node >= leafNodeOffset)
                return 1;
            else
            {
                final int leftCount = getNumLeavesSlow(node * 2);
                final int rightCount = getNumLeavesSlow(node * 2 + 1);
                return leftCount + rightCount;
            }
        }

        private int getTreeDepth(int numLeaves)
        {
            // First +1 because all the non-leave nodes makes another power
            // of 2; e.g. to have a fully balanced tree with 4 leaves you
            // need a depth=3 tree:

            // Second +1 because MathUtil.log computes floor of the logarithm; e.g.
            // with 5 leaves you need a depth=4 tree:
            return MathUtil.log(numLeaves, 2) + 2;
        }
    }

//    private interface BKDTreeLeafNodes {
//        /** number of leaf nodes */
//        int numLeaves();
//        /**
//         * pointer to the leaf node previously written. Leaves are order from left to right, so leaf at
//         * {@code index} 0 is the leftmost leaf and the leaf at {@code numleaves()} -1 is the rightmost
//         * leaf
//         */
//        long getLeafLP(int index);
//    }

//    BKDTreeLeafNodes leafNodes = new BKDTreeLeafNodes() {
//        @Override
//        public long getLeafLP(int index) {
//            return leafBlockFPs[index];
//        }
//
//        @Override
//        public int numLeaves() {
//            return leafBlockFPs.length;
//        }
//    };
}
