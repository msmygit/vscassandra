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

package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;

import com.google.common.base.MoreObjects;

import org.apache.cassandra.index.sai.disk.io.CryptoUtils;
import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.lucene.codecs.MutablePointValues;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.GrowableByteArrayDataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMOutputStream;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.Sorter;
import org.apache.lucene.util.bkd.MutablePointsReaderUtils;

/**
 * Recursively builds a block KD-tree to assign all incoming points in N-dim space to smaller
 * and smaller N-dim rectangles (cells) until the number of points in a given
 * rectangle is &lt;= <code>maxPointsInLeafNode</code>.  The tree is
 * fully balanced, which means the leaf nodes will have between 50% and 100% of
 * the requested <code>maxPointsInLeafNode</code>.  Values that fall exactly
 * on a cell boundary may be in either cell.
 *
 * <p>The number of dimensions can only be 1, but every byte[] value is fixed length.
 *
 * <p>
 * See <a href="https://www.cs.duke.edu/~pankaj/publications/papers/bkd-sstd.pdf">this paper</a> for details.
 *
 * <p>This consumes heap during writing: it allocates a <code>LongBitSet(numPoints)</code>,
 * and then uses up to the specified {@code maxMBSortInHeap} heap space for writing.
 *
 * <p>
 * <b>NOTE</b>: This can write at most Integer.MAX_VALUE * <code>maxPointsInLeafNode</code> total points.
 *
 * @lucene.experimental
 */

public class BKDWriter implements Closeable
{
    /** Default maximum number of point in each leaf block */
    public static final int DEFAULT_MAX_POINTS_IN_LEAF_NODE = 1024;

    /** How many bytes each value in each dimension takes. */
    protected final int bytesPerDim;

    /** numDims * bytesPerDim */
    protected final int packedBytesLength;

    final BytesRef scratchBytesRef1 = new BytesRef();

    protected final LongBitSet docsSeen;

    protected final int maxPointsInLeafNode;

    /** Minimum per-dim values, packed */
    protected final byte[] minPackedValue;

    /** Maximum per-dim values, packed */
    protected final byte[] maxPackedValue;

    int commonPrefixLength;

    protected long pointCount;

    /** An upper bound on how many points the caller will add (includes deletions) */
    private final long totalPointCount;

    private final long maxDoc;

    public BKDWriter(long maxDoc, int bytesPerDim, int maxPointsInLeafNode, long totalPointCount)
    {
        verifyParams(maxPointsInLeafNode, totalPointCount);

        this.maxPointsInLeafNode = maxPointsInLeafNode;
        this.bytesPerDim = bytesPerDim;
        this.totalPointCount = totalPointCount;
        this.maxDoc = maxDoc;
        docsSeen = new LongBitSet(maxDoc);
        packedBytesLength = bytesPerDim;

        minPackedValue = new byte[packedBytesLength];
        maxPackedValue = new byte[packedBytesLength];
    }

    /** How many points have been added so far */
    public long getPointCount()
    {
        return pointCount;
    }

    /**
     * Write a field from a {@link MutablePointValues}. This way of writing
     * points is faster than regular writes with BKDWriter#add since
     * there is opportunity for reordering points before writing them to
     * disk. This method does not use transient disk in order to reorder points.
     */
    public long writeField(IndexOutput out, MutableOneDimPointValues reader, final OneDimensionBKDWriterCallback callback) throws IOException
    {
        SAICodecUtils.writeHeader(out);

        if (reader.size() > 1)
            MutablePointsReaderUtils.sort(Math.toIntExact(maxDoc), packedBytesLength, reader, 0, Math.toIntExact(reader.size()));

        final OneDimensionBKDWriter oneDimWriter = new OneDimensionBKDWriter(out, callback);

        reader.intersect((docID, packedValue) -> oneDimWriter.add(packedValue, docID));

        final long fp = oneDimWriter.finish();

        SAICodecUtils.writeFooter(out);
        return fp;
    }

    private void verifyParams(int maxPointsInLeafNode, long totalPointCount)
    {
        if (maxPointsInLeafNode <= 0)
        {
            throw new IllegalArgumentException("maxPointsInLeafNode must be > 0; got " + maxPointsInLeafNode);
        }
        if (maxPointsInLeafNode > ArrayUtil.MAX_ARRAY_LENGTH)
        {
            throw new IllegalArgumentException("maxPointsInLeafNode must be <= ArrayUtil.MAX_ARRAY_LENGTH (= " + ArrayUtil.MAX_ARRAY_LENGTH + "); got " + maxPointsInLeafNode);
        }
        if (totalPointCount < 0)
        {
            throw new IllegalArgumentException("totalPointCount must be >=0 (got: " + totalPointCount + ")");
        }
        if (totalPointCount > Integer.MAX_VALUE)
        {
            throw new IllegalArgumentException("totalPointCount cannot exceed " + Integer.MAX_VALUE);
        }
    }

    // reused when writing leaf blocks
    private final GrowableByteArrayDataOutput scratchOut = new GrowableByteArrayDataOutput(32 * 1024);

    private final GrowableByteArrayDataOutput scratchOut2 = new GrowableByteArrayDataOutput(2 * 1024);

    interface OneDimensionBKDWriterCallback
    {
        void writeLeafDocs(int leafNum, RowIDAndIndex[] leafDocs, int offset, int count);
    }

    public static class RowIDAndIndex
    {
        public int valueOrderIndex;
        public long rowID;

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this)
                              .add("valueOrderIndex", valueOrderIndex)
                              .add("rowID", rowID)
                              .toString();
        }
    }

    private class OneDimensionBKDWriter
    {

        final IndexOutput out;
        final List<Long> leafBlockFPs = new ArrayList<>();
        final List<byte[]> leafBlockStartValues = new ArrayList<>();
        final byte[] leafValues = new byte[maxPointsInLeafNode * packedBytesLength];
        final long[] leafDocs = new long[maxPointsInLeafNode];
        private long valueCount;
        private int leafCount;
        final RowIDAndIndex[] rowIDAndIndexes = new RowIDAndIndex[maxPointsInLeafNode];
        final int[] orderIndex = new int[maxPointsInLeafNode];
        final OneDimensionBKDWriterCallback callback;
        // for asserts
        final byte[] lastPackedValue;
        private long lastDocID;

        OneDimensionBKDWriter(IndexOutput out, OneDimensionBKDWriterCallback callback)
        {
            if (pointCount != 0)
            {
                throw new IllegalStateException("cannot mix add and merge");
            }

            for (int x = 0; x < this.rowIDAndIndexes.length; x++)
            {
                this.rowIDAndIndexes[x] = new RowIDAndIndex();
            }

            this.out = out;
            this.callback = callback;

            lastPackedValue = new byte[packedBytesLength];
        }

        void add(byte[] packedValue, long docID) throws IOException
        {
            assert valueInOrder(valueCount + leafCount, lastPackedValue, packedValue, 0, docID, lastDocID);

            System.arraycopy(packedValue, 0, leafValues, leafCount * packedBytesLength, packedBytesLength);
            leafDocs[leafCount] = docID;
            docsSeen.set(docID);
            leafCount++;

            if (valueCount > totalPointCount)
            {
                throw new IllegalStateException("totalPointCount=" + totalPointCount + " was passed when we were created, but we just hit " + pointCount + " values");
            }

            if (leafCount == maxPointsInLeafNode)
            {
                // We write a block once we hit exactly the max count ... this is different from
                // when we write N > 1 dimensional points where we write between max/2 and max per leaf block
                writeLeafBlock();
                leafCount = 0;
            }

            assert (lastDocID = docID) >= 0; // only assign when asserts are enabled
        }

        public long finish() throws IOException
        {
            if (leafCount > 0)
            {
                writeLeafBlock();
                leafCount = 0;
            }

            if (valueCount == 0)
            {
                return -1;
            }

            pointCount = valueCount;

            long indexFP = out.getFilePointer();

            int numInnerNodes = leafBlockStartValues.size();

            byte[] index = new byte[(1 + numInnerNodes) * (1 + bytesPerDim)];
            rotateToTree(1, 0, numInnerNodes, index, leafBlockStartValues);
            long[] arr = new long[leafBlockFPs.size()];
            for (int i = 0; i < leafBlockFPs.size(); i++)
            {
                arr[i] = leafBlockFPs.get(i);
            }
            writeIndex(out, maxPointsInLeafNode, arr, index);
            return indexFP;
        }

        private void writeLeafBlock() throws IOException
        {
            assert leafCount != 0;
            if (valueCount == 0)
            {
                System.arraycopy(leafValues, 0, minPackedValue, 0, packedBytesLength);
            }
            System.arraycopy(leafValues, (leafCount - 1) * packedBytesLength, maxPackedValue, 0, packedBytesLength);

            valueCount += leafCount;

            if (leafBlockFPs.size() > 0)
            {
                // Save the first (minimum) value in each leaf block except the first, to build the split value index in the end:
                leafBlockStartValues.add(ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength));
            }
            leafBlockFPs.add(out.getFilePointer());
            checkMaxLeafNodeCount(leafBlockFPs.size());

            // Find per-dim common prefix:
            int prefix = bytesPerDim;
            int offset = (leafCount - 1) * packedBytesLength;
            for (int j = 0; j < bytesPerDim; j++)
            {
                if (leafValues[j] != leafValues[offset + j])
                {
                    prefix = j;
                    break;
                }
            }

            commonPrefixLength = prefix;

            assert scratchOut.getPosition() == 0;

            out.writeVInt(leafCount);

            for (int x = 0; x < leafCount; x++)
            {
                rowIDAndIndexes[x].valueOrderIndex = x;
                rowIDAndIndexes[x].rowID = leafDocs[x];
            }

            final Sorter sorter = new IntroSorter()
            {
                RowIDAndIndex pivot;

                @Override
                protected void swap(int i, int j)
                {
                    RowIDAndIndex o = rowIDAndIndexes[i];
                    rowIDAndIndexes[i] = rowIDAndIndexes[j];
                    rowIDAndIndexes[j] = o;
                }

                @Override
                protected void setPivot(int i)
                {
                    pivot = rowIDAndIndexes[i];
                }

                @Override
                protected int comparePivot(int j)
                {
                    return Long.compare(pivot.rowID, rowIDAndIndexes[j].rowID);
                }
            };

            sorter.sort(0, leafCount);

            // write leaf rowID -> orig index
            scratchOut2.reset();

            // iterate in row ID order to get the row ID index for the given value order index
            // place into an array to be written as packed ints
            for (int x = 0; x < leafCount; x++)
            {
                final int valueOrderIndex = rowIDAndIndexes[x].valueOrderIndex;
                orderIndex[valueOrderIndex] = x;
            }

            LeafOrderMap.write(orderIndex, leafCount, maxPointsInLeafNode - 1, scratchOut2);

            out.writeVInt(scratchOut2.getPosition());
            out.writeBytes(scratchOut2.getBytes(), 0, scratchOut2.getPosition());

            if (callback != null) callback.writeLeafDocs(leafBlockFPs.size() - 1, rowIDAndIndexes, 0, leafCount);

            writeCommonPrefixes(scratchOut, leafValues);

            scratchBytesRef1.length = packedBytesLength;
            scratchBytesRef1.bytes = leafValues;

            final IntFunction<BytesRef> packedValues = (i) -> {
                    scratchBytesRef1.offset = packedBytesLength * i;
                    return scratchBytesRef1;
            };
            assert valuesInOrderAndBounds(leafCount, ArrayUtil.copyOfSubArray(leafValues, 0, packedBytesLength),
                                          ArrayUtil.copyOfSubArray(leafValues, (leafCount - 1) * packedBytesLength, leafCount * packedBytesLength),
                                          packedValues, leafDocs, 0);

            writeLeafBlockPackedValues(scratchOut, leafCount, packedValues);

            out.writeBytes(scratchOut.getBytes(), 0, scratchOut.getPosition());

            scratchOut.reset();
        }
    }

    // TODO: there must be a simpler way?
    private void rotateToTree(int nodeID, int offset, int count, byte[] index, List<byte[]> leafBlockStartValues)
    {
        if (count == 1)
        {
            // Leaf index node
            System.arraycopy(leafBlockStartValues.get(offset), 0, index, nodeID * (1 + bytesPerDim) + 1, bytesPerDim);
        }
        else if (count > 1)
        {
            // Internal index node: binary partition of count
            int countAtLevel = 1;
            int totalCount = 0;
            while (true)
            {
                int countLeft = count - totalCount;
                if (countLeft <= countAtLevel)
                {
                    // This is the last level, possibly partially filled:
                    int lastLeftCount = Math.min(countAtLevel / 2, countLeft);
                    assert lastLeftCount >= 0;
                    int leftHalf = (totalCount - 1) / 2 + lastLeftCount;

                    int rootOffset = offset + leftHalf;

                    System.arraycopy(leafBlockStartValues.get(rootOffset), 0, index, nodeID * (1 + bytesPerDim) + 1, bytesPerDim);

                    // TODO: we could optimize/specialize, when we know it's simply fully balanced binary tree
                    //  under here, to save this while loop on each recursion

                    // Recurse left
                    rotateToTree(2 * nodeID, offset, leftHalf, index, leafBlockStartValues);

                    // Recurse right
                    rotateToTree(2 * nodeID + 1, rootOffset + 1, count - leftHalf - 1, index, leafBlockStartValues);
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

    private void checkMaxLeafNodeCount(int numLeaves)
    {
        if ((1 + bytesPerDim) * (long) numLeaves > ArrayUtil.MAX_ARRAY_LENGTH)
        {
            throw new IllegalStateException("too many nodes; increase maxPointsInLeafNode (currently " + maxPointsInLeafNode + ") and reindex");
        }
    }

    /** Packs the two arrays, representing a balanced binary tree, into a list of byte[] blocks. */
    @SuppressWarnings("resource")
    private int packIndex(long[] leafBlockFPs, byte[] splitPackedValues, List<byte[]> packedIndex) throws IOException
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
        RAMIndexOutput writeBuffer = new RAMIndexOutput("");

        byte[] lastSplitValues = new byte[bytesPerDim];
        return recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, 0l, packedIndex, 1, lastSplitValues, new boolean[1], false);
    }

    /** Appends the current contents of writeBuffer as another block on the growing in-memory file */
    private int appendBlock(RAMIndexOutput writeBuffer, List<byte[]> blocks) throws IOException
    {
        int pos = Math.toIntExact(writeBuffer.getFilePointer());
        byte[] bytes = new byte[pos];
        writeBuffer.writeTo(bytes);
        writeBuffer.reset();
        blocks.add(bytes);
        return pos;
    }

    /**
     * lastSplitValues is per-dimension split value previously seen; we use this to prefix-code the split byte[] on each
     * inner node
     */
    private int recursePackIndex(RAMIndexOutput writeBuffer, long[] leafBlockFPs, byte[] splitPackedValues, long minBlockFP, List<byte[]> blocks,
                                 int nodeID, byte[] lastSplitValues, boolean[] negativeDeltas, boolean isLeft) throws IOException
    {
        if (nodeID >= leafBlockFPs.length)
        {
            int leafID = nodeID - leafBlockFPs.length;

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
                    writeBuffer.writeVLong(delta);
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
                writeBuffer.writeVLong(delta);
            }
            else
            {
                // The left tree's left most leaf block FP is always the minimal FP:
                leftBlockFP = minBlockFP;
            }

            int address = nodeID * (1 + bytesPerDim);
            int splitDim = splitPackedValues[address++] & 0xff;

            // find common prefix with last split value in this dim:
            int prefix = 0;
            for (; prefix < bytesPerDim; prefix++)
            {
                if (splitPackedValues[address + prefix] != lastSplitValues[splitDim * bytesPerDim + prefix])
                {
                    break;
                }
            }

            int firstDiffByteDelta;
            if (prefix < bytesPerDim)
            {
                firstDiffByteDelta = (splitPackedValues[address + prefix] & 0xFF) - (lastSplitValues[splitDim * bytesPerDim + prefix] & 0xFF);
                if (negativeDeltas[splitDim])
                {
                    firstDiffByteDelta = -firstDiffByteDelta;
                }
                assert firstDiffByteDelta > 0;
            }
            else
            {
                firstDiffByteDelta = 0;
            }

            // pack the prefix, splitDim and delta first diff byte into a single vInt:
            int code = (firstDiffByteDelta * (1 + bytesPerDim) + prefix) + splitDim;

            writeBuffer.writeVInt(code);

            // write the split value, prefix coded vs. our parent's split value:
            int suffix = bytesPerDim - prefix;
            byte[] savSplitValue = new byte[suffix];
            if (suffix > 1)
            {
                writeBuffer.writeBytes(splitPackedValues, address + prefix + 1, suffix - 1);
            }

            byte[] cmp = lastSplitValues.clone();

            System.arraycopy(lastSplitValues, splitDim * bytesPerDim + prefix, savSplitValue, 0, suffix);

            // copy our split value into lastSplitValues for our children to prefix-code against
            System.arraycopy(splitPackedValues, address + prefix, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

            int numBytes = appendBlock(writeBuffer, blocks);

            // placeholder for left-tree numBytes; we need this so that at search time if we only need to recurse into the right sub-tree we can
            // quickly seek to its starting point
            int idxSav = blocks.size();
            blocks.add(null);

            boolean savNegativeDelta = negativeDeltas[splitDim];
            negativeDeltas[splitDim] = true;

            int leftNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, leftBlockFP, blocks, 2 * nodeID, lastSplitValues, negativeDeltas, true);

            if (nodeID * 2 < leafBlockFPs.length)
            {
                writeBuffer.writeVInt(leftNumBytes);
            }
            else
            {
                assert leftNumBytes == 0 : "leftNumBytes=" + leftNumBytes;
            }
            int numBytes2 = Math.toIntExact(writeBuffer.getFilePointer());
            byte[] bytes2 = new byte[numBytes2];
            writeBuffer.writeTo(bytes2);
            writeBuffer.reset();
            // replace our placeholder:
            blocks.set(idxSav, bytes2);

            negativeDeltas[splitDim] = false;
            int rightNumBytes = recursePackIndex(writeBuffer, leafBlockFPs, splitPackedValues, leftBlockFP, blocks, 2 * nodeID + 1, lastSplitValues, negativeDeltas, false);

            negativeDeltas[splitDim] = savNegativeDelta;

            // restore lastSplitValues to what caller originally passed us:
            System.arraycopy(savSplitValue, 0, lastSplitValues, splitDim * bytesPerDim + prefix, suffix);

            assert Arrays.equals(lastSplitValues, cmp);

            return numBytes + numBytes2 + leftNumBytes + rightNumBytes;
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

    private void writeIndex(IndexOutput out, int countPerLeaf, long[] leafBlockFPs, byte[] splitPackedValues) throws IOException
    {
        List<byte[]> packedIndex = new ArrayList<>();
        int totalSize = packIndex(leafBlockFPs, splitPackedValues, packedIndex);
        writeIndex(out, countPerLeaf, leafBlockFPs.length, packedIndex, totalSize);
    }

    private void writeIndex(IndexOutput out, int countPerLeaf, int numLeaves, List<byte[]> packedIndex, int totalSize) throws IOException
    {
        out.writeVInt(1);
        out.writeVInt(countPerLeaf);
        out.writeVInt(bytesPerDim);

        assert numLeaves > 0;
        out.writeVInt(numLeaves);

        out.writeBytes(minPackedValue, 0, packedBytesLength);
        out.writeBytes(maxPackedValue, 0, packedBytesLength);

        out.writeVLong(pointCount);
        out.writeVLong(docsSeen.cardinality());

        out.writeVInt(totalSize);
        for (byte[] block : packedIndex)
            out.writeBytes(block, 0, block.length);
    }

    private void writeLeafBlockPackedValues(DataOutput out, int count, IntFunction<BytesRef> packedValues) throws IOException
    {
        if (commonPrefixLength == packedBytesLength)
        {
            // all values in this block are equal
            out.writeByte((byte) -1);
        }
        else
        {
            assert commonPrefixLength < bytesPerDim;
            out.writeByte((byte) 0);
            int compressedByteOffset = commonPrefixLength;
            commonPrefixLength++;
            for (int i = 0; i < count; )
            {
                // do run-length compression on the byte at compressedByteOffset
                int runLen = runLen(packedValues, i, Math.min(i + 0xff, count), compressedByteOffset);
                assert runLen <= 0xff;
                BytesRef first = packedValues.apply(i);
                byte prefixByte = first.bytes[first.offset + compressedByteOffset];
                out.writeByte(prefixByte);
                out.writeByte((byte) runLen);
                writeLeafBlockPackedValuesRange(out, commonPrefixLength, i, i + runLen, packedValues);
                i += runLen;
                assert i <= count;
            }
        }
    }

    private void writeLeafBlockPackedValuesRange(DataOutput out, int commonPrefixLength, int start, int end, IntFunction<BytesRef> packedValues) throws IOException
    {
        for (int i = start; i < end; ++i)
        {
            BytesRef ref = packedValues.apply(i);
            assert ref.length == packedBytesLength;

            out.writeBytes(ref.bytes, ref.offset + commonPrefixLength, bytesPerDim - commonPrefixLength);
        }
    }

    private static int runLen(IntFunction<BytesRef> packedValues, int start, int end, int byteOffset)
    {
        BytesRef first = packedValues.apply(start);
        byte b = first.bytes[first.offset + byteOffset];
        for (int i = start + 1; i < end; ++i)
        {
            BytesRef ref = packedValues.apply(i);
            byte b2 = ref.bytes[ref.offset + byteOffset];
            assert Byte.toUnsignedInt(b2) >= Byte.toUnsignedInt(b);
            if (b != b2)
            {
                return i - start;
            }
        }
        return end - start;
    }

    private void writeCommonPrefixes(DataOutput out, byte[] packedValue) throws IOException
    {
        out.writeVInt(commonPrefixLength);
        out.writeBytes(packedValue, 0, commonPrefixLength);
    }

    @Override
    public void close() throws IOException
    {
    }

    /** Called only in assert */
    private boolean valueInBounds(BytesRef packedValue, byte[] minPackedValue, byte[] maxPackedValue)
    {
        if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset, packedValue.offset + bytesPerDim, minPackedValue, 0, bytesPerDim) < 0)
        {
            return false;
        }
        if (FutureArrays.compareUnsigned(packedValue.bytes, packedValue.offset, packedValue.offset + bytesPerDim, maxPackedValue, 0, bytesPerDim) > 0)
        {
            return false;
        }

        return true;
    }

    // only called from assert
    private boolean valuesInOrderAndBounds(int count, byte[] minPackedValue, byte[] maxPackedValue,
                                           IntFunction<BytesRef> values, long[] docs, int docsOffset) throws IOException
    {
        byte[] lastPackedValue = new byte[packedBytesLength];
        long lastDoc = -1;
        for (int i = 0; i < count; i++)
        {
            BytesRef packedValue = values.apply(i);
            assert packedValue.length == packedBytesLength;
            assert valueInOrder(i, lastPackedValue, packedValue.bytes, packedValue.offset, docs[docsOffset + i], lastDoc);
            lastDoc = docs[docsOffset + i];

            // Make sure this value does in fact fall within this leaf cell:
            assert valueInBounds(packedValue, minPackedValue, maxPackedValue);
        }
        return true;
    }

    // only called from assert
    private boolean valueInOrder(long ord, byte[] lastPackedValue, byte[] packedValue, int packedValueOffset, long doc, long lastDoc)
    {
        if (ord > 0)
        {
            int cmp = FutureArrays.compareUnsigned(lastPackedValue, 0, bytesPerDim, packedValue, packedValueOffset, packedValueOffset + bytesPerDim);
            if (cmp > 0)
            {
                throw new AssertionError("values out of order: last value=" + new BytesRef(lastPackedValue) + " current value=" + new BytesRef(packedValue, packedValueOffset, packedBytesLength) + " ord=" + ord);
            }
            if (cmp == 0 && doc < lastDoc)
            {
                throw new AssertionError("docs out of order: last doc=" + lastDoc + " current doc=" + doc + " ord=" + ord);
            }
        }
        System.arraycopy(packedValue, packedValueOffset, lastPackedValue, 0, packedBytesLength);
        return true;
    }
}
