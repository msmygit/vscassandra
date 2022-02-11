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
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Stopwatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.kdtree.LeafOrderMap;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.OrdinalPostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FastByteOperations;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.cassandra.index.sai.disk.format.IndexComponent.BLOCK_TERMS_DATA;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.BLOCK_UPPER_POSTINGS;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_MASK;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_SHIFT;

/**
 * Reader for a block terms index.
 */
@ThreadSafe
public class BlockTermsReader implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final Comparator<PostingList.PeekablePostingList> COMPARATOR = Comparator.comparingLong(PostingList.PeekablePostingList::peek);

    final BlockTerms.Meta meta;
    final BinaryTreePostingsReader binaryTreePostingsReader;
    protected long ramBytesUsed;

    protected final IndexDescriptor indexDescriptor;
    protected final IndexContext indexContext;
    protected final SegmentMetadata.ComponentMetadataMap componentMetadatas;
    protected final FileHandle bitpackedHandle, postingsHandle, blockOrderMapFile, termsDataHandle, termsIndexHandle, upperPostingsHandle;
    protected final V3PerIndexFiles indexFiles;
    final BlockPackedReader blockPostingOffsetsReader;
    private final SeekingRandomAccessInput orderMapRandoInput;
    private final DirectReaders.Reader orderMapReader;
    private final MonotonicBlockPackedReader blockTermsOffsetsReader;
    private final BlockPackedReader blockOrderMapFPReader;

    /**
     * Segment reader version.
     *
     * @throws IOException
     */
    public BlockTermsReader(IndexDescriptor indexDescriptor,
                            IndexContext indexContext,
                            V3PerIndexFiles indexFiles,
                            SegmentMetadata.ComponentMetadataMap componentMetadatas) throws IOException
    {
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        this.indexFiles = indexFiles;
        this.componentMetadatas = componentMetadatas;

        // derive Meta from a ComponentMetadata
        final SegmentMetadata.ComponentMetadata termsDataCompMeta = componentMetadatas.get(BLOCK_TERMS_DATA);
        this.meta = new BlockTerms.Meta(termsDataCompMeta.attributes);

        this.termsIndexHandle = indexFiles.getFileAndCache(IndexComponent.BLOCK_TERMS_INDEX);
        validateCRC(termsIndexHandle, meta.termsIndexCRC);

        this.termsDataHandle = indexFiles.getFileAndCache(BLOCK_TERMS_DATA);
        validateCRC(termsDataHandle, meta.termsCRC);

        this.bitpackedHandle = indexFiles.getFileAndCache(IndexComponent.BLOCK_BITPACKED);
        validateCRC(bitpackedHandle, meta.bitpackedCRC);

        this.postingsHandle = indexFiles.getFileAndCache(IndexComponent.BLOCK_POSTINGS);
        validateCRC(postingsHandle, meta.postingsCRC);

        blockOrderMapFile = this.indexFiles.getFileAndCache(IndexComponent.BLOCK_ORDERMAP);
        validateCRC(blockOrderMapFile, meta.orderMapCRC);

        final SegmentMetadata.ComponentMetadata blockTermsOffsetsCompMeta = componentMetadatas.get(IndexComponent.BLOCK_TERMS_OFFSETS);
        NumericValuesMeta blockTermsOffsetsMeta = new NumericValuesMeta(blockTermsOffsetsCompMeta.attributes);

        blockTermsOffsetsReader = new MonotonicBlockPackedReader(bitpackedHandle, blockTermsOffsetsMeta);
        this.ramBytesUsed += blockTermsOffsetsReader.memoryUsage();

        final SegmentMetadata.ComponentMetadata blockPostingsOffsetsCompMeta = componentMetadatas.get(IndexComponent.BLOCK_POSTINGS_OFFSETS);
        NumericValuesMeta blockPostingsOffsetsMeta = new NumericValuesMeta(blockPostingsOffsetsCompMeta.attributes);
        this.blockPostingOffsetsReader = new BlockPackedReader(bitpackedHandle, blockPostingsOffsetsMeta);
        this.ramBytesUsed += blockPostingOffsetsReader.memoryUsage();

        final SegmentMetadata.ComponentMetadata blockOrderMapCompMeta = componentMetadatas.get(IndexComponent.BLOCK_ORDERMAP);
        NumericValuesMeta blockOrderMapMeta = new NumericValuesMeta(blockOrderMapCompMeta.attributes);
        this.blockOrderMapFPReader = new BlockPackedReader(bitpackedHandle, blockOrderMapMeta);
        this.ramBytesUsed += blockOrderMapFPReader.memoryUsage();

        this.orderMapReader = DirectReaders.getReaderForBitsPerValue((byte) DirectWriter.unsignedBitsRequired(meta.postingsBlockSize - 1));
        this.orderMapRandoInput = new SeekingRandomAccessInput(IndexInputReader.create(blockOrderMapFile));

        // upper level postings does not exist for segments being compacted
        if (indexDescriptor.componentExists(IndexComponent.BLOCK_UPPER_POSTINGS, indexContext, indexFiles.isTemporary()))
        {
            upperPostingsHandle = indexFiles.getFileAndCache(BLOCK_UPPER_POSTINGS);
            try (final IndexInput upperPostingsInput = IndexInputReader.create(upperPostingsHandle))
            {
                binaryTreePostingsReader = new BinaryTreePostingsReader(meta.upperPostingsIndexFP, upperPostingsInput);
            }
        }
        else
        {
            binaryTreePostingsReader = null;
            upperPostingsHandle = null;
        }
    }

    private void validateCRC(FileHandle handle, FileValidation.Meta meta) throws IOException
    {
        try (IndexInput input = IndexInputReader.create(handle))
        {
            validateCRC(input, meta);
        }
    }

    private void validateCRC(IndexInput input, FileValidation.Meta meta) throws IOException
    {
        if (!FileValidation.validate(input, meta))
            throw new IOException("File validation failed file="+input.toString());
    }

    /**
     * Enables iterating all points (term + rowids) in index order.
     * <p>
     * Used for segment merging.
     */
    @NotThreadSafe
    public class PointsIterator implements AutoCloseable, Comparable<PointsIterator>
    {
        private final BytesCursor bytesCursor;
        private long point = 0;
        private long currentRowId = -1;
        private long currentPostingsBlock = -1;
        private long currentPostingsBlockFP = -1;
        private long currentOrderMapFP = -1;
        private final AutoCloseable toClose;
        private final long rowIdOffset;
        private final long[] orderMapBlockRowIds = new long[meta.postingsBlockSize];
        private final LongArray orderMapFPs, postingsFPs;
        private PostingList postings;
        private final IndexInput postingsInput;

        public PointsIterator(AutoCloseable toClose, long rowIdOffset)
        {
            this.toClose = toClose;
            this.rowIdOffset = rowIdOffset;
            this.postingsInput = IndexInputReader.create(postingsHandle);
            bytesCursor = cursor();
            orderMapFPs = blockOrderMapFPReader.open();
            postingsFPs = blockPostingOffsetsReader.open();
        }

        /**
         * Points iterators are sorted by current term then rowid
         */
        @Override
        public int compareTo(PointsIterator other)
        {
            final int cmp = bytesCursor.currentTerm.compareTo(other.bytesCursor.currentTerm);
            if (cmp != 0)
                return cmp;
            return Long.compare(rowId(), other.rowId());
        }

        /**
         * Current point id
         */
        public long pointId()
        {
            return point - 1;
        }

        /**
         * Current rowid
         */
        public long rowId()
        {
            return currentRowId + rowIdOffset;
        }

        /**
         * Current term
         */
        public BytesRef term()
        {
            return bytesCursor.currentTerm;
        }

        @Override
        public void close() throws Exception
        {
            FileUtils.closeQuietly(bytesCursor, postingsInput, postings, postingsFPs, orderMapFPs);
            if (toClose != null)
                toClose.close();
        }

        /**
         * Moves iterator to the next point.
         *
         * @return
         * @throws IOException
         */
        public boolean next() throws IOException
        {
            if (point >= meta.pointCount)
                return false;

            final BytesRef term = bytesCursor.seekToPointId(point);

            if (term == null)
                throw new IllegalStateException();

            final long block = point / meta.postingsBlockSize;
            if (block > currentPostingsBlock)
            {
                final long fp = postingsFPs.get(block);
                this.currentPostingsBlock = block;
                if (fp > currentPostingsBlockFP)
                {
                    this.currentPostingsBlockFP = fp;

                    this.postings = new PostingsReader(postingsInput, currentPostingsBlockFP, QueryEventListener.PostingListEventListener.NO_OP);

                    currentOrderMapFP = orderMapFPs.get(currentPostingsBlock);
                    // if the currentOrderMapFP is -1, the postings row ids are in order
                    if (currentOrderMapFP != -1)
                    {
                        for (int x = 0; x < meta.postingsBlockSize; x++)
                        {
                            final long rowid = postings.nextPosting();
                            if (rowid == PostingList.END_OF_STREAM)
                                break;

                            final int ordinal = LeafOrderMap.getValue(orderMapRandoInput, currentOrderMapFP, x, orderMapReader);
                            orderMapBlockRowIds[ordinal] = rowid;
                        }
                    }
                }
            }

            if (currentOrderMapFP != -1)
                currentRowId = orderMapBlockRowIds[(int) (point % meta.postingsBlockSize)];
            else
                currentRowId = this.postings.nextPosting();

            if (currentRowId == PostingList.END_OF_STREAM)
                throw new IllegalStateException("currentRowId should never be end of stream");

            this.point++;
            return true;
        }
    }

    /**
     * @param toClose To be closed upon PointsIterator close
     * @return Yields a PointsIterator to be used to merge segments or indexes.
     */
    public PointsIterator pointsIterator(AutoCloseable toClose, long rowIdOffset)
    {
        return new PointsIterator(toClose, rowIdOffset);
    }

    public ByteComparable minTerm()
    {
        return ByteComparable.fixedLength(meta.minTerm);
    }

    public ByteComparable maxTerm()
    {
        return ByteComparable.fixedLength(meta.maxTerm);
    }

    public long memoryUsage()
    {
        return ramBytesUsed;
    }

    @Override
    public void close()
    {
        // PerIndexFiles caches file handles, close handles only if PerIndexFiles is not present
        FileUtils.closeQuietly(orderMapRandoInput, termsIndexHandle, termsDataHandle, bitpackedHandle, blockOrderMapFile, postingsHandle);
    }

    /**
     * If necessary, exclusive ranges are processed by arithmetic byte increment and/or decrement operation.
     * Avoids IF exclusive code handling elsewhere in the code which is overly complicated.
     */
    public PostingList search(ByteComparable start,
                              boolean startExclusive,
                              ByteComparable end,
                              boolean endExclusive,
                              SSTableQueryContext context,
                              QueryEventListener.BKDIndexEventListener perColumnEventListener) throws IOException
    {
        ByteComparable realStart = start;
        ByteComparable realEnd = end;

        QueryEventListener.BKDIndexEventListener listener = MulticastQueryEventListeners.of(context.queryContext, perColumnEventListener);

        if (startExclusive && start != null)
        {
            final byte[] startBytes = ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41));
            realStart = ByteComparable.fixedLength(BlockTerms.nudge(startBytes));
        }
        if (endExclusive && end != null)
        {
            final byte[] endBytes = ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41));
            realEnd = ByteComparable.fixedLength(BlockTerms.nudgeReverse(endBytes, meta.maxTermLength));
        }

        if (start == null)
            realStart = minTerm();

        if (end == null)
            realEnd = maxTerm();

        return search(realStart, realEnd, listener, context.queryContext);
    }

    /**
     * Convert ByteComparable to BytesRef and search.
     */
    public PostingList search(ByteComparable start,
                              ByteComparable end,
                              QueryEventListener.BKDIndexEventListener listener,
                              QueryContext context) throws IOException
    {
        final byte[] startBytes = ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41));
        final byte[] endBytes = ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41));
        return search(new BytesRef(startBytes),
                      new BytesRef(endBytes),
                      listener,
                      context);
    }

    @SuppressWarnings("resource")
    PostingList search(BytesRef startTerm,
                       BytesRef endTerm,
                       QueryEventListener.BKDIndexEventListener listener,
                       QueryContext context) throws IOException
    {
        if (startTerm == null)
            throw new IllegalArgumentException("Start term null");
        if (endTerm == null)
            throw new IllegalArgumentException("End term null");

        final BinaryTree.IntersectVisitor visitor = BinaryTreePostingsReader.bkdQueryFrom(startTerm, endTerm);
        return intersect(visitor, listener, context);
    }

    @SuppressWarnings("resource")
    public PostingList intersect(BinaryTree.IntersectVisitor visitor,
                                 QueryEventListener.BKDIndexEventListener listener,
                                 QueryContext context) throws IOException
    {
        final PointValues.Relation relation = visitor.compare(new BytesRef(meta.minTerm), new BytesRef(meta.maxTerm));

        if (relation == PointValues.Relation.CELL_OUTSIDE_QUERY)
        {
            listener.onIntersectionEarlyExit();
            return null;
        }

        final LongArray postingBlockFPs = new LongArray.DeferredLongArray(blockPostingOffsetsReader::open);
        final LongArray orderMapFPs = new LongArray.DeferredLongArray(blockOrderMapFPReader::open);

        listener.onSegmentHit();

        // it's important to create the input streams once per query as
        // there is a significant performance cost atm which is solved by mmap'ing
        final IndexInputReader treeIndexInput = IndexInputReader.create(termsIndexHandle);
        final IndexInputReader postingsInput = IndexInputReader.create(postingsHandle);
        final IndexInputReader upperPostingsInput = IndexInputReader.create(upperPostingsHandle);

        try (final BinaryTree.Reader treeReader = new BinaryTree.Reader(meta.numPostingBlocks,
                                                                        meta.pointCount,
                                                                        treeIndexInput))
        {
            final Intersection intersection = relation == PointValues.Relation.CELL_INSIDE_QUERY ?
                                              new Intersection(postingsInput,
                                                               upperPostingsInput,
                                                               treeReader,
                                                               postingBlockFPs,
                                                               orderMapFPs,
                                                               context,
                                                               listener) :
                                              new FilteringIntersection(postingsInput,
                                                                        upperPostingsInput,
                                                                        treeReader,
                                                                        visitor,
                                                                        postingBlockFPs,
                                                                        orderMapFPs,
                                                                        context,
                                                                        listener);

            return intersection.execute();
        }
    }

    private class Intersection
    {
        protected final BinaryTree.Reader treeReader;
        protected final IndexInput postingsInput, upperPostingsInput;
        protected final LongArray postingBlockFPs, orderMapFPs;
        protected final QueryContext queryContext;
        protected final QueryEventListener.BKDIndexEventListener listener;
        private final Stopwatch queryExecutionTimer = Stopwatch.createStarted();

        public Intersection(IndexInput postingsInput,
                            IndexInput upperPostingsInput,
                            BinaryTree.Reader treeReader,
                            final LongArray postingBlockFPs,
                            final LongArray orderMapFPs,
                            QueryContext queryContext,
                            QueryEventListener.BKDIndexEventListener listener)
        {
            this.postingsInput = postingsInput;
            this.upperPostingsInput = upperPostingsInput;
            this.treeReader = treeReader;
            this.postingBlockFPs = postingBlockFPs;
            this.orderMapFPs = orderMapFPs;
            this.queryContext = queryContext;
            this.listener = listener;
        }

        protected void executeInternal(final PriorityQueue<PostingList.PeekablePostingList> postingLists) throws IOException
        {
            collectPostingLists(postingLists);
        }

        public PostingList execute()
        {
            try
            {
                PriorityQueue<PostingList.PeekablePostingList> postingLists = new PriorityQueue<>(100, COMPARATOR);
                executeInternal(postingLists);

                return mergePostings(postingLists);
            }
            catch (Throwable t)
            {
                if (!(t instanceof AbortedOperationException))
                    logger.error(indexContext.logMessage("block terms intersection failed"), t);

                closeOnException();
                throw Throwables.cleaned(t);
            }
        }

        protected void closeOnException()
        {
            FileUtils.closeQuietly(postingsInput, upperPostingsInput, treeReader, postingBlockFPs, orderMapFPs);
        }

        protected PostingList mergePostings(PriorityQueue<PostingList.PeekablePostingList> postingLists)
        {
            final long elapsedMicros = queryExecutionTimer.stop().elapsed(TimeUnit.MICROSECONDS);

            listener.onIntersectionComplete(elapsedMicros, TimeUnit.MICROSECONDS);
            listener.postingListsHit(postingLists.size());

            if (postingLists.isEmpty())
            {
                FileUtils.closeQuietly(postingsInput, upperPostingsInput, treeReader, postingBlockFPs, orderMapFPs);
                return null;
            }
            else
            {
                if (logger.isTraceEnabled())
                    logger.trace(indexContext.logMessage("[{}] Intersection completed in {} microseconds. {} leaf and internal posting lists hit."),
                                            elapsedMicros, postingLists.size());

                return MergePostingList.merge(postingLists, () -> FileUtils.close(postingsInput, upperPostingsInput, treeReader, postingBlockFPs, orderMapFPs));
            }
        }

        @SuppressWarnings("resource")
        public void collectPostingLists(PriorityQueue<PostingList.PeekablePostingList> postingLists) throws IOException
        {
            queryContext.checkpoint();

            final int nodeID = treeReader.getNodeID();

            if (treeReader.isLeafNode())
            {
                final int blockId = treeReader.getBlockID();
                final long blockPostingsFP = postingBlockFPs.get(blockId);

                // A -1 postings fp means there is a single posting list
                // for multiple leaf blocks because the values are the same
                if (blockPostingsFP != -1)
                {
                    final PostingsReader postings = new PostingsReader(postingsInput,
                                                                       blockPostingsFP,
                                                                       listener.bkdPostingListEventListener());
                    postingLists.add(postings.peekable());
                }
                return;
            }
            else if (binaryTreePostingsReader.exists(nodeID))
            {
                final long upperPostingsFP = binaryTreePostingsReader.getPostingsFilePointer(nodeID);
                final PostingsReader postingsReader = new PostingsReader(upperPostingsInput, upperPostingsFP, listener.bkdPostingListEventListener());
                postingLists.add(postingsReader.peekable());
                return;
            }

            // Recurse on left sub-tree:
            treeReader.pushLeft();
            collectPostingLists(postingLists);
            treeReader.pop();

            // Recurse on right sub-tree:
            treeReader.pushRight();
            collectPostingLists(postingLists);
            treeReader.pop();
        }
    }

    private class FilteringIntersection extends Intersection
    {
        private final BinaryTree.IntersectVisitor visitor;

        public FilteringIntersection(IndexInput postingsInput,
                                     IndexInput upperPostingsInput,
                                     BinaryTree.Reader treeReader,
                                     BinaryTree.IntersectVisitor visitor,
                                     final LongArray postingBlockFPs,
                                     final LongArray orderMapFPs,
                                     QueryContext queryContext,
                                     QueryEventListener.BKDIndexEventListener listener)
        {
            super(postingsInput, upperPostingsInput, treeReader, postingBlockFPs, orderMapFPs, queryContext, listener);
            this.visitor = visitor;
        }

        @Override
        public void executeInternal(final PriorityQueue<PostingList.PeekablePostingList> postingLists) throws IOException
        {
            collectPostingLists(postingLists, new BytesRef(meta.minTerm), new BytesRef(meta.maxTerm));
        }

        public void collectPostingLists(PriorityQueue<PostingList.PeekablePostingList> postingLists,
                                        BytesRef cellMinPacked,
                                        BytesRef cellMaxPacked) throws IOException
        {
            final PointValues.Relation r = visitor.compare(cellMinPacked, cellMaxPacked);

            if (r == PointValues.Relation.CELL_OUTSIDE_QUERY)
            {
                // This cell is fully outside of the query shape: stop recursing
                return;
            }

            if (r == PointValues.Relation.CELL_INSIDE_QUERY)
            {
                // This cell is fully inside of the query shape: recursively add all points in this cell without filtering
                super.collectPostingLists(postingLists);
                return;
            }

            if (treeReader.isLeafNode())
            {
                if (treeReader.nodeExists())
                    filterLeaf(postingLists);
                return;
            }

            visitNode(postingLists, cellMinPacked, cellMaxPacked);
        }

        void filterLeaf(PriorityQueue<PostingList.PeekablePostingList> postingLists) throws IOException
        {
            final MutableValueLong postingsFP = new MutableValueLong();

            final int block = treeReader.getBlockID();

            final PostingList postings = filterBlock(visitor,
                                                     block,
                                                     postingsFP,
                                                     postingBlockFPs,
                                                     orderMapFPs,
                                                     postingsInput,
                                                     listener);

            if (postings != null)
            {
                postingLists.add(postings.peekable());
            }
        }

        void visitNode(PriorityQueue<PostingList.PeekablePostingList> postingLists,
                       BytesRef cellMinPacked,
                       BytesRef cellMaxPacked) throws IOException
        {
            BytesRef splitPackedValue = treeReader.getSplitPackedValue();
            BytesRef splitDimValue = treeReader.getSplitDimValue().clone();

            // Recurse on left sub-tree:
            BinaryTree.Reader.copyBytes(cellMaxPacked, splitPackedValue);
            BinaryTree.Reader.copyBytes(splitDimValue, splitPackedValue);

            treeReader.pushLeft();
            collectPostingLists(postingLists, cellMinPacked, splitPackedValue);
            treeReader.pop();

            // Restore the split dim value since it may have been overwritten while recursing:
            BinaryTree.Reader.copyBytes(splitPackedValue, splitDimValue);

            // Recurse on right sub-tree:
            BinaryTree.Reader.copyBytes(cellMinPacked, splitPackedValue);
            BinaryTree.Reader.copyBytes(splitDimValue, splitPackedValue);

            treeReader.pushRight();
            collectPostingLists(postingLists, splitPackedValue, cellMaxPacked);
            treeReader.pop();
        }
    }

    @SuppressWarnings("resource")
    PostingList filterBlock(final BinaryTree.IntersectVisitor visitor,
                            final int block,
                            final MutableValueLong postingsFP,
                            final LongArray blockPostingsFPs,
                            final LongArray orderMapFPs,
                            final IndexInput postingsInput,
                            final QueryEventListener.BKDIndexEventListener listener) throws IOException
    {
        final long start = (long) block * (long) meta.postingsBlockSize;

        long maxord = ((long) block + 1) * (long) meta.postingsBlockSize - 1;
        maxord = maxord >= meta.pointCount ? meta.pointCount - 1 : maxord;

        postingsFP.value = blockPostingsFPs.get(block);

        if (postingsFP.value == -1)
            return null;

        final PostingsReader.BlocksSummary blocksSummary = new PostingsReader.BlocksSummary(postingsInput, postingsFP.value);

        final OrdinalPostingList postings = new PostingsReader(postingsInput,
                                                               blocksSummary,
                                                               listener.bkdPostingListEventListener());

        final long orderMapFP = orderMapFPs.get(block); // here for assertion, move lower

        final Pair<Long, Long> minMaxPoints = filterPoints(visitor, start, maxord);

        if (minMaxPoints.left == -1)
            return null;

        final int cardinality = (int) (minMaxPoints.right - minMaxPoints.left) + 1;

        if (cardinality == 0)
            return null;

        final int startOrd = (int) (minMaxPoints.left % meta.postingsBlockSize);
        final int endOrd = (int) (minMaxPoints.right % meta.postingsBlockSize);

        return new FilteringPostingList(cardinality,
                                        // get the row id's term ordinal to compare against the startOrdinal
                                        (postingsOrd, rowID) -> {
                                            int ord = postingsOrd;

                                            // if there's no order map use the postings order
                                            if (orderMapFP != -1)
                                                ord = (int) this.orderMapReader.get(this.orderMapRandoInput, orderMapFP, postingsOrd);

                                            return ord >= startOrd && ord <= endOrd;
                                        },
                                        postings);
    }

    Pair<Long, Long> filterPoints(BinaryTree.IntersectVisitor visitor,
                                  final long start,
                                  final long end) throws IOException
    {
        long idx = start;
        long startIdx = -1;
        long endIdx = end;

        final BytesRef startTerm = ((BinaryTreePostingsReader.RangeQueryVisitor) visitor).getLowerBytes();
        final BytesRef endTerm = ((BinaryTreePostingsReader.RangeQueryVisitor) visitor).getUpperBytes();

        try (final BytesCursor bytesCursor = cursor())
        {
            BytesRef term = bytesCursor.seekToPointId(idx);

            for (; idx <= end; idx++)
            {
                if (term == null)
                    break;

                if (startTerm != null && startIdx == -1 && term.compareTo(startTerm) >= 0)
                {
                    startIdx = idx;
                }

                if (endTerm != null && term.compareTo(endTerm) > 0)
                {
                    endIdx = idx - 1;
                    break;
                }

                if (bytesCursor.next())
                    term = bytesCursor.currentTerm;
                else
                    break;
            }
        }
        return Pair.create(startIdx, endIdx);
    }

    /**
     * Create a BytesCursor.  Needs to be closed after use.
     *
     * @return BytesCursor
     */
    public BytesCursor cursor()
    {
        return new BytesCursor();
    }

    /**
     * Cursor to obtain prefixed encoded bytes from a point id.
     */
    @NotThreadSafe
    public class BytesCursor implements AutoCloseable
    {
        private final IndexInput termsData;
        private final BytesRef currentTerm;
        private final LongArray termBlockFPs;
        private long pointId;

        public BytesCursor()
        {
            this.currentTerm = new BytesRef(meta.maxTermLength);
            this.termsData = IndexInputReader.create(termsDataHandle);
            termBlockFPs = blockTermsOffsetsReader.open();
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(termsData, termBlockFPs);
        }

        /**
         * Get bytes at a point id.
         */
        public BytesRef seekToPointId(final long target) throws IOException
        {
            if (target < -1 || target >= meta.pointCount)
                throw new IndexOutOfBoundsException();

            if (target == -1 || target == meta.pointCount)
            {
                termsData.seek(meta.termsDataFp);   // matters only if target is -1
                pointId = target;
                currentTerm.length = 0;
            }
            else
            {
                final long blockIndex = target >>> TERMS_DICT_BLOCK_SHIFT;
                final long blockAddress = termBlockFPs.get(blockIndex);

                termsData.seek(blockAddress);
                pointId = (blockIndex << TERMS_DICT_BLOCK_SHIFT) - 1;
                while (pointId < target)
                {
                    boolean advanced = next();
                    assert advanced : "unexpected eof";   // must return true because target is in range
                }
            }
            return currentTerm;
        }

        public boolean next() throws IOException
        {
            if (pointId >= meta.pointCount || ++pointId >= meta.pointCount)
            {
                currentTerm.length = 0;
                return false;
            }

            int prefixLength;
            int suffixLength;
            if ((pointId & TERMS_DICT_BLOCK_MASK) == 0L)
            {
                prefixLength = 0;
                suffixLength = termsData.readVInt();
            }
            else
            {
                final int token = Byte.toUnsignedInt(termsData.readByte());
                prefixLength = token & 0x0F;
                suffixLength = 1 + (token >>> 4);
                if (prefixLength == 15)
                    prefixLength += termsData.readVInt();
                if (suffixLength == 16)
                    suffixLength += termsData.readVInt();
            }

            assert prefixLength + suffixLength <= meta.maxTermLength : "prefixLength=" + prefixLength + " suffixLength=" + suffixLength + " maxTermLength=" + meta.maxTermLength;
            currentTerm.length = prefixLength + suffixLength;
            termsData.readBytes(currentTerm.bytes, prefixLength, suffixLength);
            return true;
        }
    }
}
