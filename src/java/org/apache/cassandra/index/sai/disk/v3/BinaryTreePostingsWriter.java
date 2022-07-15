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
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static com.google.common.base.Preconditions.checkArgument;

public class BinaryTreePostingsWriter implements BinaryTree.BinaryTreeTraversalCallback
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final Multimap<Integer, Integer> nodeToChildLeaves = HashMultimap.create();
    private final IntArrayList leafNodeIDs = new IntArrayList();
    //private final int skip = 3;
    //private final int minLeaves = 64;
    private final IndexWriterConfig config;

    public BinaryTreePostingsWriter(IndexWriterConfig config)
    {
       this.config = config;
    }

    @Override
    public void onLeaf(int leafID, int leafNodeID, IntArrayList pathToRoot)
    {
        //checkArgument(!pathToRoot.containsInt(leafNodeID));
        //checkArgument(pathToRoot.isEmpty() || leafNodeID > pathToRoot.get(pathToRoot.size() - 1));

        leafNodeIDs.add(leafNodeID);

        for (int i = 0; i < pathToRoot.size(); i++)
        {
            final int level = i + 1;
            if (isLevelEligibleForPostingList(level))
            {
                final int nodeID = pathToRoot.get(i);
                nodeToChildLeaves.put(nodeID, leafID);
            }
        }
    }

    public static class Result
    {
        public final long startFP;
        public final long indexFilePointer;

        public Result(long startFP, long indexFilePointer)
        {
            this.startFP = startFP;
            this.indexFilePointer = indexFilePointer;
        }
    }

    public Result finish(BlockTerms.Reader reader, IndexOutput out) throws IOException
    {
        try (LongArray postingOffsets = reader.blockPostingOffsetsReader.open())
        {
            return finish(reader.meta.numPostingBlocks, reader.postingsHandle, postingOffsets, out);
        }
    }

    public Result finish(int numPostingBlocks,
                         FileHandle postingsHandle,
                         LongArray leafPostingsOffsets,
                         IndexOutput out) throws IOException
    {
        final long startFP = out.getFilePointer();
        final PostingsWriter postingsWriter = new PostingsWriter(out);

        try (final IndexInput leafPostingsInput = IndexInputReader.create(postingsHandle))
        {
            final List<Integer> internalNodeIDs = nodeToChildLeaves.keySet()
                                                                   .stream()
                                                                   .filter(i -> nodeToChildLeaves.get(i).size() >= config.getBkdPostingsMinLeaves())
                                                                   .collect(Collectors.toList());

//        logger.debug(indexContext.logMessage("Writing posting lists for {} internal and {} leaf kd-tree nodes. Leaf postings memory usage: {}."),
//                     internalNodeIDs.size(),
//                     leafNodeIDs.size(),
//                     FBUtilities.prettyPrintMemory(postingsRamBytesUsed));

            final Stopwatch flushTime = Stopwatch.createStarted();
            final TreeMap<Integer, Long> nodeIDToPostingsFilePointer = new TreeMap<>();

            int seenLeafNodeCount = 0;

            for (final int nodeID : Iterables.concat(internalNodeIDs, leafNodeIDs))
            {
                final Collection<Integer> leaves = nodeToChildLeaves.get(nodeID);

                // leaf level posting lists are already written
                // leaf nodes have no leaves
                // skip
                if (leaves.isEmpty())
                {
                    //assert nodeID >= numPostingBlocks; // assert empty leaves is a leaf node id

                    seenLeafNodeCount++;

                    continue;
                }

                final PriorityQueue<PostingList.PeekablePostingList> leafPostings = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));

                for (Integer leafID : leaves)
                {
                    final long leafPostingsFP = leafPostingsOffsets.get(leafID);

                    if (leafPostingsFP != -1)
                    {
                        final PostingsReader postings = new PostingsReader(leafPostingsInput, leafPostingsFP, QueryEventListener.PostingListEventListener.NO_OP);
                        if (postings.size() > 0) // TODO: how possible
                            leafPostings.add(postings.peekable());
                    }
                }

                final PostingList mergedPostingList = MergePostingList.merge(leafPostings);
                final long postingFilePosition = postingsWriter.write(mergedPostingList);
                // During compaction we could end up with an empty postings due to deletions.
                // The writer will return a fp of -1 if no postings were written.
                if (postingFilePosition >= 0)
                    nodeIDToPostingsFilePointer.put(nodeID, postingFilePosition);
            }
            flushTime.stop();

            // System.out.println("seenLeafNodeCount="+seenLeafNodeCount+" numLeafBlocks="+numPostingBlocks);

            logger.debug("Flushed {} upper posting lists for binary-tree nodes in {} ms.",
                         nodeIDToPostingsFilePointer.size(),
                         flushTime.elapsed(TimeUnit.MILLISECONDS));
//
//            logger.debug(indexContext.logMessage("Flushed {} of posting lists for kd-tree nodes in {} ms."),
//                     FBUtilities.prettyPrintMemory(out.getFilePointer() - startFP),
//                     flushTime.elapsed(TimeUnit.MILLISECONDS));

            final long indexFilePointer = out.getFilePointer();
            writeMap(nodeIDToPostingsFilePointer, out);
            postingsWriter.complete();

            return new Result(startFP, indexFilePointer);
        }
    }

    private void writeMap(Map<Integer, Long> map, IndexOutput out) throws IOException
    {
        out.writeVInt(map.size());

        for (Map.Entry<Integer, Long> e : map.entrySet())
        {
            out.writeVInt(e.getKey());
            out.writeVLong(e.getValue());
        }
    }

    private boolean isLevelEligibleForPostingList(int level)
    {
        return level > 1 && level % config.getBkdPostingsSkip() == 0;
    }

//    private boolean isLevelEligibleForPostingList(int level)
//    {
//        return level > 1 && level % skip == 0;
//    }
}
