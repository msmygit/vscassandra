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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.cassandra.index.sai.disk.format.IndexComponent.BLOCK_POSTINGS;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.BLOCK_TERMS_DATA;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.BLOCK_TERMS_INDEX;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.BLOCK_UPPER_POSTINGS;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_MASK;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.copyBytes;
import static org.apache.cassandra.index.sai.disk.v3.SegmentNumericValuesWriter.BLOCK_SIZE;
import static org.apache.cassandra.index.sai.disk.v3.SegmentNumericValuesWriter.MONOTONIC_BLOCK_SIZE;

/**
 * Writes block terms and postings.
 */
@NotThreadSafe
public class BlockTermsWriter
{
    final BytesRefArray minBlockTerms = new BytesRefArray(Counter.newCounter()); // per posting block min terms
    private final BytesRefBuilder prevTerm = new BytesRefBuilder();
    private final BytesRefBuilder spare = new BytesRefBuilder();
    private final BytesRefBuilder minTerm = new BytesRefBuilder();
    private final int postingsBlockSize; // size of each postings block
    private final IndexOutputWriter termsOut, termsIndexOut, postingsOut, orderMapOut;
    private final LongArrayList termBlockFPs = new LongArrayList(); // file pointers per prefix encoded bytes block
    private final LongArrayList blockPostingsFPs = new LongArrayList(); // per postings block postings file pointer
    private final LongArrayList blockOrderMapFPs = new LongArrayList(); // per postings block order map file pointer
    private final IndexDescriptor indexDescriptor;
    private final IndexContext context;
    private final PostingsWriter postingsWriter;
    private final RowIDIndex[] rowIDIndices;
    private final boolean segmented;
    private final long orderMapStartFP, termsStartFP, termsIndexStartFP;
    private final BytesRefBuilder writeBytesBlockPrevTerm = new BytesRefBuilder();
    protected BlockTerms.Meta meta;
    public long maxRowId = -1;
    public long minRowId = Long.MAX_VALUE;
    private int maxTermLength = 0;
    private int postingsBlockCount = 0;
    private long pointId = 0; // point count
    private long distinctTermCount = 0;
    private CachedBlock currentPostingsBlock = new CachedBlock(), previousPostingsBlock = new CachedBlock();
    private LongBitSet rowIdsSeen = new LongBitSet(BlockTerms.DEFAULT_POSTINGS_BLOCK_SIZE);
    private long lastBytesBlockFP = -1;
    private long lastRowID = -1;
    private BytesBlockCache currentBytesCache = new BytesBlockCache();
    private BytesBlockCache previousBytesCache = new BytesBlockCache();

    public BlockTermsWriter(IndexDescriptor indexDescriptor, IndexContext context, boolean segmented) throws IOException
    {
        this(BlockTerms.DEFAULT_POSTINGS_BLOCK_SIZE, indexDescriptor, context, segmented);
    }

    public BlockTermsWriter(IndexDescriptor indexDescriptor, IndexContext context) throws IOException
    {
        this(BlockTerms.DEFAULT_POSTINGS_BLOCK_SIZE, indexDescriptor, context, false);
    }

    public BlockTermsWriter(int postingsBlockSize, IndexDescriptor indexDescriptor, IndexContext context, boolean segmented) throws IOException
    {
        this.indexDescriptor = indexDescriptor;
        this.context = context;
        this.postingsBlockSize = postingsBlockSize;
        this.segmented = segmented;
        this.termsOut = indexDescriptor.openPerIndexOutput(BLOCK_TERMS_DATA, context, true, segmented);
        this.termsStartFP = this.termsOut.getFilePointer();
        this.termsIndexOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_TERMS_INDEX, context, true, segmented);
        this.termsIndexStartFP = this.termsIndexOut.getFilePointer();
        this.orderMapOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_ORDERMAP, context, true, segmented);
        this.orderMapStartFP = this.orderMapOut.getFilePointer();
        this.postingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_POSTINGS, context, true, segmented);
        this.postingsWriter = new PostingsWriter(postingsOut, postingsBlockSize);
        this.rowIDIndices = new RowIDIndex[postingsBlockSize];
        for (int x = 0; x < postingsBlockSize; x++)
        {
            this.rowIDIndices[x] = new RowIDIndex();
        }
    }

    public static String unisgnedIntsToString(BytesRef term)
    {
        List<Integer> list = new ArrayList<>();
        for (int x = term.offset; x < term.offset + term.length; x++)
        {
            list.add(term.bytes[x] & 0xff);
        }
        return list.toString();
    }

    /**
     * Writes merged segments.
     */
    public SegmentMetadata.ComponentMetadataMap writeAll(final MergePointsIterators iterator,
                                                         final TermRowIDCallback callback) throws Exception
    {
        while (iterator.next())
        {
            final BytesRef term = iterator.term();
            final long rowid = iterator.rowId();
            add(term, rowid, callback);
        }

        final SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();

        if (callback != null)
            callback.finish(components);

        this.meta = finish(components);
        return components;
    }

    /**
     * Callback for the indexing to the trie/equality.
     */
    interface TermRowIDCallback
    {
        void added(BytesRef term, long rowid) throws IOException;

        void finish(SegmentMetadata.ComponentMetadataMap components) throws IOException;
    }

    /**
     * Called by memtable index writer.
     */
    public SegmentMetadata.ComponentMetadataMap writeAll(final TermsIterator terms,
                                                         final TermRowIDCallback callback) throws IOException
    {
        while (terms.hasNext())
        {
            final ByteComparable term = terms.next();
            try (final PostingList postings = terms.postings())
            {
                while (true)
                {
                    final long rowid = postings.nextPosting();
                    if (rowid == PostingList.END_OF_STREAM)
                        break;
                    add(term, rowid, callback);
                }
            }
        }

        final SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();

        if (callback != null)
            callback.finish(components);

        try
        {
            this.meta = finish(components);
        }
        catch (Exception ex)
        {
            throw Throwables.cleaned(ex);
        }
        return components;
    }

    /**
     * Finish writing the index.
     * <p>
     * Closes outputs.
     *
     * @param components Write to segment meta data
     * @return Index meta data
     * @throws Exception Error on writing final bits.
     */
    public BlockTerms.Meta finish(final SegmentMetadata.ComponentMetadataMap components) throws Exception
    {
        if (components == null)
            throw new IllegalStateException();

        // empty index
        if (pointId == 0)
            return null;

        // flush buffers
        flushLastBuffers();

        postingsWriter.complete();

        SAICodecUtils.writeFooter(orderMapOut);
        SAICodecUtils.writeFooter(termsOut);
        SAICodecUtils.writeFooter(postingsOut);

        orderMapOut.close();
        termsOut.close();
        postingsOut.close();

        // TODO: segments don't require an index though the index is not large
        final BinaryTree.Writer treeWriter = new BinaryTree.Writer();
        treeWriter.finish(minBlockTerms, termsIndexOut);
        SAICodecUtils.writeFooter(termsIndexOut);
        termsIndexOut.close();

        final long termsIndexFP = 0;

        BinaryTreePostingsWriter.Result treePostingsResult = null;

        final long upperPostingsStartFP, upperPostingsEndFP;

        // only write upper postings for the final single merged segment
        if (!segmented)
        {
            try (final V3PerIndexFiles perIndexFiles = new V3PerIndexFiles(indexDescriptor, context, segmented);
                 final FileHandle binaryTreeFile = perIndexFiles.getFile(BLOCK_TERMS_INDEX);
                 final IndexInput binaryTreeInput = IndexInputReader.create(binaryTreeFile);
                 final IndexOutput upperPostingsOut = indexDescriptor.openPerIndexOutput(BLOCK_UPPER_POSTINGS, context, true, segmented);
                 final FileHandle blockPostingsHandle = perIndexFiles.getFile(BLOCK_POSTINGS))
            {
                upperPostingsStartFP = upperPostingsOut.getFilePointer();
                try (final BinaryTree.Reader treeReader = new BinaryTree.Reader(minBlockTerms.size(),
                                                                                pointCount(),
                                                                                binaryTreeInput))
                {
                    IndexWriterConfig config = IndexWriterConfig.fromOptions("", UTF8Type.instance, new HashMap<>());

                    final BinaryTreePostingsWriter treePostingsWriter = new BinaryTreePostingsWriter(config);
                    treeReader.traverse(new IntArrayList(), treePostingsWriter);

                    treePostingsResult = treePostingsWriter.finish(blockPostingsHandle,
                                                                   new BlockTerms.LongArrayImpl(blockPostingsFPs),
                                                                   upperPostingsOut,
                                                                   context);
                    SAICodecUtils.writeFooter(upperPostingsOut);
                    upperPostingsEndFP = upperPostingsOut.getFilePointer();
                }
            }
        }
        else
        {
            upperPostingsStartFP = 0;
            upperPostingsEndFP = 0;
        }

        // write the 3 bit packed data structures into one file
        final long bitpackStartFP;
        final long bitpackEndFP;

        try (final IndexOutput bitPackOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_BITPACKED, context, true, segmented))
        {
            bitpackStartFP = bitPackOut.getFilePointer();
            try (SegmentNumericValuesWriter blockTermsOffsetsWriter = new SegmentNumericValuesWriter(IndexComponent.BLOCK_TERMS_OFFSETS,
                                                                                                     bitPackOut,
                                                                                                     components,
                                                                                                     true,
                                                                                                     MONOTONIC_BLOCK_SIZE))
            {
                for (int x = 0; x < termBlockFPs.size(); x++)
                {
                    long fp = termBlockFPs.getLong(x);
                    blockTermsOffsetsWriter.add(fp);
                }
            }

            try (final SegmentNumericValuesWriter numericWriter = new SegmentNumericValuesWriter(IndexComponent.BLOCK_POSTINGS_OFFSETS,
                                                                                                 bitPackOut,
                                                                                                 components,
                                                                                                 false,
                                                                                                 BLOCK_SIZE))
            {
                for (int x = 0; x < blockPostingsFPs.size(); x++)
                {
                    long fp = blockPostingsFPs.getLong(x);
                    numericWriter.add(fp);
                }
            }

            try (final SegmentNumericValuesWriter numericWriter = new SegmentNumericValuesWriter(IndexComponent.BLOCK_ORDERMAP,
                                                                                                 bitPackOut,
                                                                                                 components,
                                                                                                 false,
                                                                                                 BLOCK_SIZE))
            {
                for (int x = 0; x < this.blockOrderMapFPs.size(); x++)
                {
                    long fp = blockOrderMapFPs.getLong(x);
                    numericWriter.add(fp);
                }
            }
            SAICodecUtils.writeFooter(bitPackOut);
            bitpackEndFP = bitPackOut.getFilePointer();
        }

        final FileValidation.Meta termsIndexCRC = createCRCMeta(termsIndexStartFP, IndexComponent.BLOCK_TERMS_INDEX);
        final FileValidation.Meta termsCRC = createCRCMeta(termsStartFP, BLOCK_TERMS_DATA);
        final FileValidation.Meta orderMapCRC = createCRCMeta(orderMapStartFP, IndexComponent.BLOCK_ORDERMAP);
        final FileValidation.Meta postingsCRC = createCRCMeta(postingsWriter.getStartOffset(), IndexComponent.BLOCK_POSTINGS);
        final FileValidation.Meta bitpackedCRC = createCRCMeta(bitpackStartFP, IndexComponent.BLOCK_BITPACKED);
        final FileValidation.Meta upperPostingsMetaCRC = treePostingsResult != null ? createCRCMeta(treePostingsResult.startFP, BLOCK_UPPER_POSTINGS) : null;

        meta = new BlockTerms.Meta(pointId,
                                   postingsBlockSize,
                                   maxTermLength,
                                   termsStartFP,
                                   termsIndexFP,
                                   minBlockTerms.size(),
                                   distinctTermCount,
                                   treePostingsResult != null ? treePostingsResult.indexFilePointer : -1,
                                   BytesRef.deepCopyOf(minTerm.get()).bytes,
                                   BytesRef.deepCopyOf(prevTerm.get()).bytes,
                                   termsIndexCRC,
                                   termsCRC,
                                   orderMapCRC,
                                   postingsCRC,
                                   bitpackedCRC,
                                   upperPostingsMetaCRC,
                                   minRowId,
                                   maxRowId);

        final long postingsLength = postingsOut.getFilePointer() - postingsWriter.getStartOffset();
        final long termsLength = termsOut.getFilePointer() - termsStartFP;
        final long termsIndexLength = termsIndexOut.getFilePointer() - termsIndexStartFP;
        final long bitPackedLength = bitpackEndFP - bitpackStartFP;

        // Postings list file pointers are stored directly in TERMS_DATA, so a root is not needed.
        components.put(BLOCK_TERMS_DATA, termsStartFP, termsStartFP, termsLength, meta.stringMap());
        components.put(IndexComponent.BLOCK_POSTINGS, -1, postingsWriter.getStartOffset(), postingsLength, meta.stringMap());
        components.put(IndexComponent.BLOCK_TERMS_INDEX, meta.termsIndexFP, termsIndexStartFP, termsIndexLength, meta.stringMap());
        components.put(IndexComponent.BLOCK_BITPACKED, bitpackStartFP, bitpackStartFP, bitPackedLength, meta.stringMap());
        components.put(IndexComponent.BLOCK_UPPER_POSTINGS, upperPostingsStartFP, upperPostingsStartFP, upperPostingsEndFP - upperPostingsStartFP, meta.stringMap());
        return meta;
    }

    private FileValidation.Meta createCRCMeta(long rootFP, IndexComponent comp) throws IOException
    {
        try (IndexInput input = indexDescriptor.openPerIndexInput(comp, context, segmented))
        {
            return FileValidation.createValidate(rootFP, input);
        }
    }

    public long pointCount()
    {
        return pointId;
    }

    public int blockCount()
    {
        return postingsBlockCount;
    }

    public boolean add(ByteComparable term, long rowid) throws IOException
    {
        copyBytes(term, spare);
        return add(spare.get(), rowid, null);
    }

    public void add(ByteComparable term, long rowid, TermRowIDCallback callback) throws IOException
    {
        copyBytes(term, spare);
        add(spare.get(), rowid, callback);
    }

    /**
     * Block of prefix bytes cache.
     */
    private static class BytesBlockCache
    {
        private final BytesRefArray bytesArray = new BytesRefArray(Counter.newCounter(false));
        private final BytesRefBuilder spare = new BytesRefBuilder();
        private boolean sameTerms = true;

        public int count()
        {
            return bytesArray.size();
        }

        public BytesRef minTerm()
        {
            return bytesArray.get(spare, 0);
        }

        private void add(BytesRef term)
        {
            if (bytesArray.size() > 0)
            {
                final BytesRef prevTerm = bytesArray.get(spare, bytesArray.size() - 1);
                if (!prevTerm.equals(term))
                    sameTerms = false;
            }
            bytesArray.append(term);
        }

        private void reset()
        {
            sameTerms = true;
            bytesArray.clear();
        }
    }

    /**
     * Write the bytes block to disk.
     */
    private void writeBytesBlock(final BytesBlockCache bytesCache) throws IOException
    {
        writeBytesBlockPrevTerm.clear();
        for (int x = 0; x < bytesCache.count(); x++)
        {
            final BytesRef term = bytesCache.bytesArray.get(bytesCache.spare, x);
            if ((x & TERMS_DICT_BLOCK_MASK) == 0)
            {
                lastBytesBlockFP = termsOut.getFilePointer();
                termBlockFPs.add(lastBytesBlockFP);

                termsOut.writeVInt(term.length);
                termsOut.writeBytes(term.bytes, term.offset, term.length);
            }
            else
            {
                final int prefixLength = BlockTerms.bytesDifference(writeBytesBlockPrevTerm.get(), term);
                final int suffixLength = term.length - prefixLength;

                // avoid writing the prefix and suffix vints if the lengths can be encoded in a single byte
                termsOut.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));

                if (prefixLength >= 15)
                    termsOut.writeVInt(prefixLength - 15);
                if (suffixLength >= 16)
                    termsOut.writeVInt(suffixLength - 16);

                termsOut.writeBytes(term.bytes, term.offset + prefixLength, term.length - prefixLength);
            }
            writeBytesBlockPrevTerm.copyBytes(term);
        }
    }

    /**
     * Two blocks are equals if the number of terms are the same
     * and the terms bytes are the same.
     */
    public static boolean equals(final BytesBlockCache cache1, final BytesBlockCache cache2)
    {
        if (cache1.count() != cache2.count())
            return false;
        for (int x = 0; x < cache1.count(); x++)
        {
            final BytesRef term1 = cache1.bytesArray.get(cache1.spare, x);
            final BytesRef term2 = cache2.bytesArray.get(cache2.spare, x);
            if (!term1.equals(term2))
                return false;
        }
        return true;
    }

    public boolean add(BytesRef term, long rowid) throws IOException
    {
        return add(term, rowid, null);
    }

    /**
     * Cached block is indexed into, flushed, and swapped with a 2nd cached block on full.
     * Adds a term and row id to the current block.
     *
     * @param term  Bytes ascending order
     * @param rowid Row id in secondary ascending order
     * @throws IOException Error when flushing the block
     */
    public boolean add(BytesRef term, long rowid, TermRowIDCallback callback) throws IOException
    {
        boolean sameTermAsLast = prevTerm.get().equals(term);

        if (term.length <= 0)
            throw new IllegalArgumentException("Term length cannot be zero or less");

        if (minTerm.length() == 0)
            minTerm.copyBytes(term);

        if (prevTerm.get().compareTo(term) > 0)
            throw new IllegalArgumentException("Terms must be in ascending lexicographic order prevTerm=" + unisgnedIntsToString(prevTerm.get()) + " term=" + unisgnedIntsToString(term));

        minRowId = Math.min(minRowId, rowid);
        maxRowId = Math.max(maxRowId, rowid);

        rowIdsSeen = LongBitSet.ensureCapacity(rowIdsSeen, rowid + 1);
        rowIdsSeen.set(rowid);

        if (!sameTermAsLast)
            distinctTermCount++;
        else if (rowid < lastRowID)
            throw new IllegalStateException("rowid=" + rowid + "less than lastRowID=" + lastRowID);

        if (callback != null)
            callback.added(term, rowid);

        // add term and row id to current postings block cache
        currentPostingsBlock.add(term, rowid);

        // add term bytes to current bytes block cache
        currentBytesCache.add(term);

        // bytes block previous and current swap and flush handling
        if ((currentBytesCache.count() & TERMS_DICT_BLOCK_MASK) == 0)
        {
            // previous block is the same, reuse the file pointer, write nothing
            if (previousBytesCache.count() == currentBytesCache.count()
                && termBlockFPs.size() > 0
                && currentBytesCache.sameTerms
                && previousBytesCache.sameTerms
                && currentBytesCache.minTerm().equals(previousBytesCache.minTerm()))
            {
                assert equals(currentBytesCache, previousBytesCache);

                assert previousBytesCache.count() > 0;

                assert lastBytesBlockFP != -1;
                termBlockFPs.add(lastBytesBlockFP);
            }
            else // write bytes block to disk
            {
                assert currentBytesCache.count() > 0;

                writeBytesBlock(currentBytesCache);
            }
            // swap prev and next pointers
            final BytesBlockCache next = previousBytesCache;
            previousBytesCache = currentBytesCache;
            currentBytesCache = next;
            currentBytesCache.reset();
        }

        if (currentPostingsBlock.count == postingsBlockSize)
            flushPreviousBufferAndSwap();

        maxTermLength = Math.max(maxTermLength, term.length);
        prevTerm.copyBytes(term);
        pointId++;
        lastRowID = rowid;

        return sameTermAsLast;
    }

    /**
     * Write a cached block's postings and order map if necessary.
     * <p>
     * Order map is optional, the order map file pointer may be -1.
     * <p>
     * Returns if the order map was written.
     */
    protected void writePostingsAndOrderMap(CachedBlock buffer) throws IOException
    {
        assert buffer.count > 0;

        for (int x = 0; x < buffer.count; x++)
        {
            rowIDIndices[x].rowid = buffer.postings.getLong(x);
            rowIDIndices[x].index = x;
        }

        // sort by row id
        Arrays.sort(rowIDIndices, 0, buffer.count, Comparator.comparingLong(o -> o.rowid));

        boolean inRowIDOrder = true;
        for (int x = 0; x < buffer.count; x++)
        {
            final long rowID = rowIDIndices[x].rowid;

            if (rowIDIndices[x].index != x)
                inRowIDOrder = false;

            postingsWriter.add(rowID);
        }

        // write an order map if the row ids are not in order
        if (!inRowIDOrder)
        {
            final long orderMapFP = orderMapOut.getFilePointer();
            final int bits = DirectWriter.unsignedBitsRequired(postingsBlockSize - 1);
            final DirectWriter writer = DirectWriter.getInstance(orderMapOut, buffer.count, bits);
            for (int i = 0; i < buffer.count; i++)
                writer.add(rowIDIndices[i].index);

            writer.finish();
            blockOrderMapFPs.addLong(orderMapFP);
        }
        else
        {
            // -1 demarcs blocks with no order map
            blockOrderMapFPs.addLong(-1);
        }
    }

    private void processBlock(CachedBlock block)
    {
        minBlockTerms.append(block.minTerm());
    }

    private void flushPreviousBufferAndSwap() throws IOException
    {
        flushPostingsBlock();

        // swap buffer pointers
        final CachedBlock next = previousPostingsBlock;
        previousPostingsBlock = currentPostingsBlock;
        currentPostingsBlock = next;
        postingsBlockCount++;
    }

    private void flushLastBuffers() throws IOException
    {
        if (currentBytesCache.count() > 0)
            writeBytesBlock(currentBytesCache);

        flushPostingsBlock();

        if (currentPostingsBlock.isNotEmpty())
        {
            processBlock(currentPostingsBlock);
            writePostingsAndOrderMap(currentPostingsBlock);

            final long postingsFP = postingsWriter.finishPostings();
            this.blockPostingsFPs.add(postingsFP);
        }
    }

    private void flushPostingsBlock() throws IOException
    {
        if (previousPostingsBlock.isNotEmpty())
        {
            processBlock(previousPostingsBlock);
            writePostingsAndOrderMap(previousPostingsBlock);

            final long postingsFP = postingsWriter.finishPostings();
            blockPostingsFPs.addLong(postingsFP);
            previousPostingsBlock.reset();
        }
    }

    /**
     * Data structure representing a singular cached block consisting of terms postings and other meta data.
     */
    protected static class CachedBlock
    {
        private final BytesRefArray terms = new BytesRefArray(Counter.newCounter());
        private final BytesRefBuilder spare = new BytesRefBuilder();
        private final LongArrayList postings = new LongArrayList();
        private int count = 0;
        private boolean sameTerms = true;

        public boolean isNotEmpty()
        {
            return count != 0;
        }

        public BytesRef minTerm()
        {
            return terms.get(spare, 0);
        }

        public void add(BytesRef bytes, long rowid)
        {
            if (count > 0)
            {
                if (!terms.get(spare, count - 1).equals(bytes))
                    sameTerms = false;
            }
            terms.append(bytes);
            postings.add(rowid);
            count++;
        }

        public void reset()
        {
            sameTerms = true;
            count = 0;
            terms.clear();
            postings.clear();
        }
    }

    private static class RowIDIndex
    {
        public int index;
        public long rowid;

        @Override
        public String toString()
        {
            return "RowIDIndex{" +
                   "index=" + index +
                   ", rowid=" + rowid +
                   '}';
        }
    }
}
