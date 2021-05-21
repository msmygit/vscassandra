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
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;


/**
 * Reads, decompresses and decodes postings lists written by {@link PostingsWriter}.
 *
 * Holds exactly one postings block in memory at a time. Does binary search over skip table to find a postings block to
 * load.
 */
@NotThreadSafe
public class MinBlockPostingsReader implements OrdinalPostingList
{
    protected final IndexInput input;
    private final int blockSize;
    private final long numPostings;
    private final LongArray blockOffsets;
    private final LongArray blockMinValues;
    private final SeekingRandomAccessInput seekingInput;
    private final QueryEventListener.PostingListEventListener listener;

    // TODO: Expose more things through the summary, now that it's an actual field?
    private final BlocksSummary summary;

    private int postingsBlockIdx;
    private int blockIdx; // position in block
    private long totalPostingsRead;

    private long currentPosition;
    private DirectReaders.Reader currentFORValues;
    private long postingsDecoded = 0;
    private long minRowID;

    @VisibleForTesting
    MinBlockPostingsReader(IndexInput input, long summaryOffset, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this(input, new BlocksSummary(input, summaryOffset, () -> {}), listener);
    }

    @VisibleForTesting
    public MinBlockPostingsReader(IndexInput input, BlocksSummary summary, QueryEventListener.PostingListEventListener listener) throws IOException
    {
        this.input = input;
        this.seekingInput = new SeekingRandomAccessInput(input);
        this.blockOffsets = summary.offsets;
        this.blockSize = summary.blockSize;
        this.numPostings = summary.numPostings;
        this.blockMinValues = summary.minValues;
        this.listener = listener;

        this.summary = summary;

        reBuffer();
    }

//    public static class ReversePostingListImpl implements ReversePostingList
//    {
//        final MinBlockPostingsReader parent;
//        int revBlockIdx = 0;
//        int revPostingsBlockIdx;
//        long minRowID;
//        long ordinal;
//
//        public ReversePostingListImpl(MinBlockPostingsReader parent) throws IOException
//        {
//            this.parent = parent;
//            int lastBlockIdx = (int) (parent.numPostings / parent.blockSize) - 1;
//            lastPosInBlock(lastBlockIdx);
//            reBuffer();
//        }
//
//        private void reBuffer() throws IOException
//        {
//            final long pointer = parent.blockOffsets.get(revPostingsBlockIdx);
//            minRowID = parent.blockMinValues.get(revPostingsBlockIdx);
//
//            parent.input.seek(pointer);
//
//            //final long left = parent.numPostings - (parent.numPostings - totalPostingsRead);
//            //assert left > 0;
//
//            parent.readFoRBlock(parent.input);
//
//            revBlockIdx = (int)Math.min(ordinal, parent.blockSize - 1);
//        }
//
//        private void minusOnePosition()
//        {
//            ordinal--;
//            revBlockIdx--;
//        }
//
//        @Override
//        public long nextPosting() throws IOException
//        {
//            final long next = peekNext();
//            if (next != ReversePostingList.REVERSE_END_OF_STREAM)
//            {
//                minusOnePosition();
//            }
//            return next;
//        }
//
//        @VisibleForTesting
//        int getBlockSize()
//        {
//            return parent.blockSize;
//        }
//
//        private long peekNext() throws IOException
//        {
//            if ( (parent.numPostings - ordinal) >= parent.numPostings)
//            {
//                return ReversePostingList.REVERSE_END_OF_STREAM;
//            }
//            if (revBlockIdx == -1)
//            {
//                if (revPostingsBlockIdx == 0)
//                {
//                    return ReversePostingList.REVERSE_END_OF_STREAM;
//                }
//                lastPosInBlock(revPostingsBlockIdx - 1);
//                reBuffer();
//            }
//
//            return minRowID + nextRowID();
//        }
//
//        private long nextRowID()
//        {
//            // currentFORValues is null when the all the values in the block are the same
//            if (parent.currentFORValues == null)
//            {
//                return 0;
//            }
//            else
//            {
//                final long id = parent.currentFORValues.get(parent.seekingInput, parent.currentPosition, revBlockIdx);
//                parent.postingsDecoded++;
//                return id;
//            }
//        }
//
//        private void lastPosInBlock(int block)
//        {
//            //totalPostingsRead += (parent.blockSize - blockIdx) + (block - postingsBlockIdx + 1) * blockSize;
//            revPostingsBlockIdx = block;
//            ordinal = ( (revPostingsBlockIdx + 1) * parent.blockSize) - 1;
//            revBlockIdx = parent.blockSize;
//        }
//
//        @Override
//        public void close() throws IOException
//        {
//            parent.close();
//        }
//
//        @Override
//        public long size()
//        {
//            return parent.size();
//        }
//
//        @Override
//        public long advance(long targetRowID) throws IOException
//        {
//            parent.listener.onAdvance();
//            int block = parent.binarySearchBlock(targetRowID);
//
//            if (block < 0)
//            {
//                block = -block - 1;
//            }
//
//            block = block > 0 ? block - 1 : block;
//
//            if (revPostingsBlockIdx == block)
//            {
//                // we're in the same block, just iterate through
//                return slowAdvance(targetRowID);
//            }
//            //assert block > 0;
//            // Even if there was an exact match, block might contain duplicates.
//            // We iterate to the target token from the beginning.
//            lastPosInBlock(block);
//            return slowAdvance(targetRowID);
//        }
//
//        private long slowAdvance(long targetRowID) throws IOException
//        {
//            while (ordinal < parent.numPostings)
//            {
//                final long segmentRowId = peekNext();
//
//                minusOnePosition();
//
//                if (segmentRowId <= targetRowID)
//                {
//                    return segmentRowId;
//                }
//            }
//            return REVERSE_END_OF_STREAM;
//        }
//    }

//    public ReversePostingList reversePostingList() throws IOException
//    {
//        return new ReversePostingListImpl(this);
//    }

    @Override
    public long getOrdinal()
    {
        return totalPostingsRead;
    }

    interface InputCloser
    {
        void close() throws IOException;
    }

    @VisibleForTesting
    public static class BlocksSummary
    {
        final int blockSize;
        final int numPostings;
        final LongArray offsets;
        final LongArray minValues;

        private final InputCloser runOnClose;

        @VisibleForTesting
        public BlocksSummary(IndexInput input, long offset) throws IOException
        {
            this(input, offset, input::close);
        }

        BlocksSummary(IndexInput input, long offset, InputCloser runOnClose) throws IOException
        {
            this.runOnClose = runOnClose;

            input.seek(offset);
            this.blockSize = input.readVInt();
            //TODO This should need to change because we can potentially end up with postings of more than Integer.MAX_VALUE?
            this.numPostings = input.readVInt();

            final SeekingRandomAccessInput randomAccessInput = new SeekingRandomAccessInput(input);
            final int numBlocks = input.readVInt();
            final long minBlockValuesLength = input.readVLong();
            final long minBlockValuesOffset = input.getFilePointer() + minBlockValuesLength;

            final byte offsetBitsPerValue = input.readByte();
            if (offsetBitsPerValue > 64)
            {
                throw new CorruptIndexException(
                        String.format("Postings list header is corrupted: Bits per value for block offsets must be no more than 64 and is %d.", offsetBitsPerValue), input);
            }
            this.offsets = new LongArrayReader(randomAccessInput, DirectReaders.getReaderForBitsPerValue(offsetBitsPerValue), input.getFilePointer(), numBlocks);

            input.seek(minBlockValuesOffset);
            final byte valuesBitsPerValue = input.readByte();
            if (valuesBitsPerValue > 64)
            {
                throw new CorruptIndexException(
                        String.format("Postings list header is corrupted: Bits per value for values samples must be no more than 64 and is %d.", valuesBitsPerValue), input);
            }
            this.minValues = new LongArrayReader(randomAccessInput, DirectReaders.getReaderForBitsPerValue(valuesBitsPerValue), input.getFilePointer(), numBlocks);
        }

        void close() throws IOException
        {
            runOnClose.close();
        }

        private static class LongArrayReader implements LongArray
        {
            private final RandomAccessInput input;
            private final DirectReaders.Reader reader;
            private final long offset;
            private final int length;

            private LongArrayReader(RandomAccessInput input, DirectReaders.Reader reader, long offset, int length)
            {
                this.input = input;
                this.reader = reader;
                this.offset = offset;
                this.length = length;
            }

            @Override
            public long findTokenRowID(long value)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public long get(long idx)
            {
                return reader.get(input, offset, idx);
            }

            @Override
            public long length()
            {
                return length;
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        listener.postingDecoded(postingsDecoded);
        try
        {
            input.close();
        }
        finally
        {
            summary.close();
        }
    }

    @Override
    public long size()
    {
        return numPostings;
    }

    /**
     * Advances to the first row ID beyond the current that is greater than or equal to the
     * target, and returns that row ID. Exhausts the iterator and returns {@link #END_OF_STREAM} if
     * the target is greater than the highest row ID.
     *
     * Does binary search over the skip table to find the next block to load into memory.
     *
     * Note: Callers must use the return value of this method before calling {@link #nextPosting()}, as calling
     * that method will return the next posting, not the one to which we have just advanced.
     *
     * @param targetRowID target row ID to advance to
     *
     * @return first segment row ID which is >= the target row ID or {@link PostingList#END_OF_STREAM} if one does not exist
     */
    @Override
    public long advance(long targetRowID) throws IOException
    {
        listener.onAdvance();
        int block = binarySearchBlock(targetRowID);

        if (block < 0)
        {
            block = -block - 1;
        }

        block = block > 0 ? block - 1 : block;

        if (postingsBlockIdx == block + 1)
        {
            // we're in the same block, just iterate through
            return slowAdvance(targetRowID);
        }
        assert block > 0;
        // Even if there was an exact match, block might contain duplicates.
        // We iterate to the target token from the beginning.
        lastPosInBlock(block - 1);
        return slowAdvance(targetRowID);
    }

    private long slowAdvance(long targetRowID) throws IOException
    {
        while (totalPostingsRead < numPostings)
        {
            long segmentRowId = peekNext();

            advanceOnePosition(segmentRowId);

            if (segmentRowId >= targetRowID)
            {
                return segmentRowId;
            }
        }
        return END_OF_STREAM;
    }

    private int binarySearchBlock(long targetRowID)
    {
        int low = postingsBlockIdx - 1;
        int high = Math.toIntExact(blockMinValues.length()) - 1;

        // in current block
        if (low <= high && targetRowID <= blockMinValues.get(low))
            return low;

        while (low <= high)
        {
            int mid = low + ((high - low) >> 1);

            long midVal = blockMinValues.get(mid);

            if (midVal < targetRowID)
            {
                low = mid + 1;
            }
            else if (midVal > targetRowID)
            {
                high = mid - 1;
            }
            else
            {
                // target found, but we need to check for duplicates
                if (mid > 0 && blockMinValues.get(mid - 1L) == targetRowID)
                {
                    // there are duplicates, pivot left
                    high = mid - 1;
                }
                else
                {
                    // no duplicates
                    return mid;
                }
            }
        }
        return -(low + 1);  // target not found
    }

    private void lastPosInBlock(int block)
    {
        // blockMaxValues is integer only
        //actualSegmentRowId = blockMinValues.get(block);
        //upper bound, since we might've advanced to the last block, but upper bound is enough
        totalPostingsRead += (blockSize - blockIdx) + (block - postingsBlockIdx + 1) * blockSize;

        postingsBlockIdx = block + 1;
        blockIdx = blockSize;
    }

    @Override
    public long nextPosting() throws IOException
    {
        final long next = peekNext();
        if (next != END_OF_STREAM)
        {
            advanceOnePosition(next);
        }
        return next;
    }

    @VisibleForTesting
    int getBlockSize()
    {
        return blockSize;
    }

    private long peekNext() throws IOException
    {
        if (totalPostingsRead >= numPostings)
        {
            return END_OF_STREAM;
        }
        if (blockIdx == blockSize)
        {
            reBuffer();
        }

        return minRowID + nextRowID();
    }

    private int nextRowID()
    {
        // currentFORValues is null when the all the values in the block are the same
        if (currentFORValues == null)
        {
            return 0;
        }
        else
        {
            final long id = currentFORValues.get(seekingInput, currentPosition, blockIdx);
            postingsDecoded++;
            return Math.toIntExact(id);
        }
    }

    private void advanceOnePosition(long nextRowID)
    {
        //actualSegmentRowId = nextRowID;
        totalPostingsRead++;
        blockIdx++;
    }

    private void reBuffer() throws IOException
    {
        final long pointer = blockOffsets.get(postingsBlockIdx);
        minRowID = blockMinValues.get(postingsBlockIdx);

        input.seek(pointer);

        final long left = numPostings - totalPostingsRead;
        assert left > 0;

        readFoRBlock(input);

        postingsBlockIdx++;
        blockIdx = 0;
    }

    private void readFoRBlock(IndexInput in) throws IOException
    {
        final byte bitsPerValue = in.readByte();

        currentPosition = in.getFilePointer();

        if (bitsPerValue == 0)
        {
            // currentFORValues is null when the all the values in the block are the same
            currentFORValues = null;
            return;
        }
        else if (bitsPerValue > 64)
        {
            throw new CorruptIndexException(
                    String.format("Postings list #%s block is corrupted. Bits per value should be no more than 64 and is %d.", postingsBlockIdx, bitsPerValue), input);
        }
        currentFORValues = DirectReaders.getReaderForBitsPerValue(bitsPerValue);
    }
}
