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
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

public class KDTreeIndexTest
{
    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput output = dir.createOutput("name", IOContext.DEFAULT);

        List<byte[]> leafBlockStartValues = new ArrayList();

        for (int x = 0; x < 5; x++)
        {
            byte[] bytes = new byte[4];
            NumericUtils.intToSortableBytes((x * 1000) + 100_000, bytes, 0);
            leafBlockStartValues.add(bytes);
            leafBlockStartValues.add(bytes);
        }

        KDTreeIndexWriter indexWriter = new KDTreeIndexWriter(output, leafBlockStartValues);
        indexWriter.finish();
        output.close();

        byte[] rootBytes = indexWriter.bytes[1];

        KDTreeIndexReader reader = new KDTreeIndexReader(leafBlockStartValues.size());
        KDTreeIndexReader.IndexTree indexTree = reader.indexTree();

        int[] prefixes = new int[leafBlockStartValues.size()];
        int[] suffixes = new int[leafBlockStartValues.size()];

        IndexOutput suffixOut = dir.createOutput("suffix", IOContext.DEFAULT);

        suffixOut.writeBytes(rootBytes, 0, rootBytes.length);

        prefixes[1] = 0;
        suffixes[1] = rootBytes.length;

        traverse(indexTree, indexWriter.bytes, 1, prefixes, suffixes, suffixOut);
        suffixOut.close();

        for (int x = 1; x < prefixes.length; x++)
        {
            System.out.println(x + " prefix=" + prefixes[x] + " suffix=" + suffixes[x]);
        }

        reader = new KDTreeIndexReader(leafBlockStartValues.size());
        indexTree = reader.indexTree();

        int rootPrefix = prefixes[1];
        int rootSuffix = suffixes[1];

        assert rootPrefix == 0;

        IndexInput suffixInput = dir.openInput("suffix", IOContext.DEFAULT);

        byte[] bytes = new byte[rootSuffix];
        suffixInput.readBytes(bytes, 0, rootSuffix);

        BytesRefBuilder parentValue = new BytesRefBuilder();
        parentValue.append(bytes, 0, bytes.length);

        traverseIndex(indexTree, parentValue, prefixes, suffixes, suffixInput, indexWriter.bytes);
    }

    private void traverseIndex(KDTreeIndexReader.IndexTree indexTree,
                               BytesRefBuilder parentValue,
                               int[] prefixes,
                               int[] suffixes,
                               IndexInput suffixInput,
                               byte[][] nodeBytes) throws IOException
    {
        int parentInt = NumericUtils.sortableBytesToInt(parentValue.bytes(), 0);

        System.out.println("traverseIndex parentInt="+parentInt);

        if (indexTree.isLeafNode())
        {
            // In the unbalanced case it's possible the left most node only has one child:
            if (indexTree.nodeExists())
            {
            }
        }
        else
        {
            indexTree.pushLeft();

            int leftNodeID = indexTree.getNodeID();

            if (!indexTree.isLeafNode())
            {
                int prefix = (int)prefixes[leftNodeID];
                int suffix = (int)suffixes[leftNodeID];

                System.out.println("prefix="+prefix+" suffix="+suffix);

                byte[] bytes = new byte[prefix + suffix];

                for (int x = 0; x < prefix; x++)
                {
                    bytes[x] = parentValue.byteAt(x);
                }

                for (int x = prefix; x < prefix + suffix; x++)
                {
                    bytes[x] = suffixInput.readByte();
                }

                int leftValue = NumericUtils.sortableBytesToInt(bytes, 0);

                System.out.println("leftNodeID="+leftNodeID+" leftValue="+leftValue);

                BytesRefBuilder parentValue2 = new BytesRefBuilder();
                parentValue2.append(bytes, 0, bytes.length);

                assert Arrays.equals(nodeBytes[leftNodeID], bytes) : "nodeBytes.len="+nodeBytes[leftNodeID].length+
                                                                     " bytes.len="+bytes.length+
                                                                     " leftnode="+Arrays.toString(nodeBytes[leftNodeID])+
                                                                     " value="+NumericUtils.sortableBytesToInt(nodeBytes[leftNodeID], 0)+
                                                                     " bytes="+Arrays.toString(bytes)+
                                                                     " value2="+NumericUtils.sortableBytesToInt(bytes, 0);

                traverseIndex(indexTree, parentValue2, prefixes, suffixes, suffixInput, nodeBytes);
            }

            indexTree.pop();

            indexTree.pushRight();

            int rightNodeID = indexTree.getNodeID();

            if (!indexTree.isLeafNode())
            {
                int prefix = (int)prefixes[rightNodeID];
                int suffix = (int)suffixes[rightNodeID];

                byte[] bytes = new byte[prefix + suffix];

                for (int x = 0; x < prefix; x++)
                {
                    bytes[x] = parentValue.byteAt(x);
                }

                for (int x = prefix; x < prefix + suffix; x++)
                {
                    bytes[x] = suffixInput.readByte();
                }

                int rightValue = NumericUtils.sortableBytesToInt(bytes, 0);

                System.out.println("rightNodeID="+leftNodeID+" rightValue="+rightValue);

                BytesRefBuilder parentValue2 = new BytesRefBuilder();
                parentValue2.append(bytes, 0, bytes.length);

                assert Arrays.equals(nodeBytes[rightNodeID], bytes);

                traverseIndex(indexTree, parentValue2, prefixes, suffixes, suffixInput, nodeBytes);
            }

            indexTree.pop();
        }
    }

    private void traverse(KDTreeIndexReader.IndexTree indexTree,
                          byte[][] bytes,
                          int parentNodeID,
                          int[] prefixes,
                          int[] suffixes,
                          IndexOutput suffixOut) throws IOException
    {
        if (indexTree.isLeafNode())
        {
            // In the unbalanced case it's possible the left most node only has one child:
            if (indexTree.nodeExists())
            {
            }
        }
        else
        {
            indexTree.pushLeft();

            int leftNodeID = indexTree.getNodeID();

            if (!indexTree.isLeafNode())
            {
                byte[] leftBytes = bytes[leftNodeID];

                int prefix = prefix(leftBytes, bytes[parentNodeID]);
                int suffix = leftBytes.length - prefix;

                prefixes[leftNodeID] = prefix;
                suffixes[leftNodeID] = suffix;

                suffixOut.writeBytes(leftBytes, prefix, suffix);

                int leftVal = NumericUtils.sortableBytesToInt(leftBytes, 0);

                System.out.println("parentNodeID=" + parentNodeID + " nodeID=" + indexTree.getNodeID() + " leftVal=" + leftVal + " prefix=" + prefix + " suffixOutLen=" + suffixOut.getFilePointer());
            }

            traverse(indexTree, bytes, leftNodeID, prefixes, suffixes, suffixOut);

            indexTree.pop();

            indexTree.pushRight();

            int rightNodeID = indexTree.getNodeID();

            if (!indexTree.isLeafNode())
            {
                byte[] rightBytes = bytes[rightNodeID];

                int prefix = prefix(rightBytes, bytes[parentNodeID]);
                int suffix = rightBytes.length - prefix;

                prefixes[rightNodeID] = prefix;
                suffixes[rightNodeID] = suffix;

                suffixOut.writeBytes(rightBytes, prefix, suffix);

                int rightVal = NumericUtils.sortableBytesToInt(rightBytes, 0);

                System.out.println("parentNodeID=" + parentNodeID + " nodeID=" + indexTree.getNodeID() + " rightVal=" + rightVal + " prefix=" + prefix + " suffixOutLen=" + suffixOut.getFilePointer());
            }

            traverse(indexTree, bytes, rightNodeID, prefixes, suffixes, suffixOut);

            indexTree.pop();
        }
    }

    public static int prefix(byte[] bytes, byte[] parentBytes)
    {
        int prefix = 0;
        int minLen = Math.min(bytes.length, parentBytes.length);
        for (; prefix < minLen; prefix++)
        {
            if (bytes[prefix] != parentBytes[prefix])
            {
                break;
            }
        }
        return prefix;
    }
}
