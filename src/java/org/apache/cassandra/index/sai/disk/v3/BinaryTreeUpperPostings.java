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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PackedLongsPostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.lucene.store.IndexOutput;

public class BinaryTreeUpperPostings
{
    public static final int MIN_LEAVES = 32;

    public static class Writer
    {
        final BlockTerms.Reader reader;

        public Writer(final BlockTerms.Reader reader) throws IOException
        {
            this.reader = reader;
        }

        public UpperPostings.MetaCRC finish(final SegmentMetadata.ComponentMetadataMap components,
                                            final boolean segmented) throws IOException
        {
            final BinaryTreeIndex.BKDPointTree tree = new BinaryTreeIndex.BKDPointTree(reader.meta.numPostingBlocks, reader.meta.pointCount);

            final LongArray leafPostingFPs = reader.blockPostingOffsetsReader.open();

            IntArrayList pathToRoot = new IntArrayList();
            tree.traverse(pathToRoot);

            final List<Integer> internalNodeIDs = tree.nodeToChildLeaves.keySet()
                                                                        .stream()
                                                                        .filter(i -> tree.nodeToChildLeaves.get(i).size() >= MIN_LEAVES)
                                                                        .collect(Collectors.toList());

            final Collection<Integer> leafNodeIDs = new ArrayList<>();

            int leafNode = tree.leafNodeOffset;
            for (int x = 0; x < reader.meta.numPostingBlocks; x++)
            {
                leafNodeIDs.add(leafNode);
                leafNode++;
            }

            for (int nodeID : Iterables.concat(internalNodeIDs, leafNodeIDs))
            {
                Collection<Integer> leaves = tree.nodeToChildLeaves.get(nodeID);

                if (leaves.isEmpty())
                    continue;

                System.out.println("nodeID="+nodeID+" leaves="+leaves);

//                for (int x = 0; x < reader.meta.numPostingBlocks; x++)
//                {
//                    long fp = leafPostingFPs.get(x);
//
//                    System.out.println("leaf nodeid="+x+" fp="+fp);
//                }

                final PriorityQueue<PostingList.PeekablePostingList> postingLists = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));
//                for (Integer leaf : leaves)
//                    postingLists.add(new PackedLongsPostingList(leafToPostings.get(leaf)).peekable());

//                final PostingList mergedPostings = MergePostingList.merge(postingLists);
//                final long postingFilePosition = postingsWriter.write(mergedPostings);
//                // During compaction we could end up with an empty postings due to deletions.
//                // The writer will return a fp of -1 if no postings were written.
//                if (postingFilePosition >= 0)
//                    nodeIDToPostingsFilePointer.put(nodeID, postingFilePosition);
//
//                try (IndexOutput upperPostingsOut = reader.indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_UPPER_POSTINGS, reader.context, true, segmented))
//                {
//                    PostingsWriter postingsWriter = new PostingsWriter(upperPostingsOut);
//                    long fp = postingsWriter.write(mergedPostings);
//
//
//                }
            }
            return null;
        }
    }
}
