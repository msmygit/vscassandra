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
import java.util.Comparator;
import java.util.PriorityQueue;

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.OrdinalPostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static org.apache.cassandra.index.sai.disk.v3.SegmentNumericValuesWriter.MONOTONIC_BLOCK_SIZE;

public class UpperPostings
{
    public static class Reader implements AutoCloseable
    {
        private final BlockTerms.Reader reader;
        private final LongArray postingOffsets;
        private final FileHandle postingsFile, bitpackedFile;

        public Reader(final BlockTerms.Reader reader) throws IOException
        {
            this.reader = reader;

            this.postingsFile = reader.indexFiles.getFileAndCache(IndexComponent.BLOCK_UPPER_POSTINGS);
            this.bitpackedFile = reader.indexFiles.getFileAndCache(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS);

            final SegmentMetadata.ComponentMetadata meta = reader.componentMetadatas.get(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS);
            final NumericValuesMeta numericMeta = new NumericValuesMeta(meta.attributes);
            final MonotonicBlockPackedReader postingOffsetsReader = new MonotonicBlockPackedReader(bitpackedFile, numericMeta);
            reader.ramBytesUsed += postingOffsetsReader.memoryUsage();
            this.postingOffsets = postingOffsetsReader.open();
        }

        @Override
        public void close() throws Exception
        {
            postingsFile.close();
        }

        public PostingList search(long start, long end) throws IOException
        {
            final IndexInput input = IndexInputReader.create(postingsFile);

            long startAtLevel = start / reader.meta.postingsBlockSize;
            long endAtLevel = end / reader.meta.postingsBlockSize;

            System.out.println("startAtLevel="+startAtLevel+" endAtLevel="+endAtLevel);

//            postingOffsets.get();
//
//            PostingsReader postings = new PostingsReader(input, fp, QueryEventListener.PostingListEventListener.NO_OP);

            return null;
        }
    }

    public static class Writer
    {
        private final BlockTerms.Reader reader;
        private final PostingsWriter postingsWriter;
        private final IndexOutput upperPostingsOut;
        private final int baseUnit = 10;

        public Writer(BlockTerms.Reader reader) throws IOException
        {
            this.reader = reader;
            this.upperPostingsOut = reader.indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_UPPER_POSTINGS, reader.context);
            this.postingsWriter = new PostingsWriter(upperPostingsOut);

            // TOOD: process next level * 10
//            processLevel(unit, blockSize, pointCount);
        }

        public void finish(final SegmentMetadata.ComponentMetadataMap components, boolean segmented) throws IOException
        {
            long unit = baseUnit;

            final int blockSize = reader.meta.postingsBlockSize;
            final long pointCount = reader.meta.pointCount;

            LongArrayList level1FPs = processLevel(unit, blockSize, pointCount);
            System.out.println("level1FPs="+level1FPs);
            unit *= baseUnit;

            try (final IndexOutput bitPackedOut = reader.indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS, reader.context, true, segmented);
                 final SegmentNumericValuesWriter numericWriter = new SegmentNumericValuesWriter(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS,
                                                                                                 bitPackedOut,
                                                                                                 components,
                                                                                                 true,
                                                                                                 MONOTONIC_BLOCK_SIZE))
            {
                for (int x = 0; x < level1FPs.size(); x++)
                {
                    long fp = level1FPs.getLong(x);
                    numericWriter.add(fp);
                }
            }

            postingsWriter.complete();
            upperPostingsOut.close();
        }

        private LongArrayList processLevel(final long unit, final int blockSize, final long pointCount) throws IOException
        {
            final long numBlocks = pointCount / (blockSize * unit);

            long currentBlock = 0;

            final LongArrayList filePointers = new LongArrayList();

            for (long block = 0; block < numBlocks; block++)
            {
                long endBlock = unit - 1 + currentBlock;
                final long filePointer = processBlock(currentBlock, endBlock);
                filePointers.addLong(filePointer);
                currentBlock = endBlock + 1;
            }
            return filePointers;
        }

        private long processBlock(final long startBlock, final long endBlock) throws IOException
        {
            final PriorityQueue<PostingList.PeekablePostingList> queue = new PriorityQueue<>(Comparator.comparingLong(PostingList.PeekablePostingList::peek));
            for (long block = startBlock; block <= endBlock; block++)
            {
                OrdinalPostingList postings = reader.openBlockPostings(block);
                queue.add(postings.peekable());
            }
            System.out.println("processBlock startBlock=" + startBlock + " endBlock=" + endBlock);
            final PostingList mergedPostings = MergePostingList.merge(queue);
            return postingsWriter.write(mergedPostings);
        }
    }
}
