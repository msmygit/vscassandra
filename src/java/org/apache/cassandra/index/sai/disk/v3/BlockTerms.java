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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.UnsignedBytes;

import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.kdtree.BKDReader;
import org.apache.cassandra.index.sai.disk.v1.kdtree.LeafOrderMap;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.OrdinalPostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.FutureArrays;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.mutable.MutableValueLong;
import org.apache.lucene.util.packed.DirectWriter;

import static org.apache.cassandra.index.sai.disk.format.IndexComponent.BLOCK_POSTINGS;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.BLOCK_TERMS_DATA;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.BLOCK_TERMS_INDEX;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.BLOCK_UPPER_POSTINGS;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_MASK;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_SHIFT;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.copyBytes;
import static org.apache.cassandra.index.sai.disk.v3.SegmentNumericValuesWriter.BLOCK_SIZE;
import static org.apache.cassandra.index.sai.disk.v3.SegmentNumericValuesWriter.MONOTONIC_BLOCK_SIZE;

/**
 * Block terms + postings disk index data structure.
 *
 * Raw bytes are prefix encoded in blocks of 16 for fast(er) random access than the posting block default size of 1024.
 *
 * Block terms reader uses no significant heap space, all major data structures are disk based.
 *
 * A point is defined as a term + rowid monotonic id.
 *
 * The default postings block size is 1024.
 *
 * All major data structures are on disk thereby enabling many indexes.
 */
public class BlockTerms
{
    public static final int DEFAULT_POSTINGS_BLOCK_SIZE = 1024;

    public static int toInt(BytesRef term)
    {
        ByteSource.Peekable peekable = ByteComparable.fixedLength(term.bytes, term.offset, term.length).asPeekableBytes(ByteComparable.Version.OSS41);
        ByteBuffer buffer2 = Int32Type.instance.fromComparableBytes(peekable, ByteComparable.Version.OSS41);
        return Int32Type.instance.compose(buffer2);
    }

    /**
     * Block terms index meta object.
     */
    public static class Meta
    {
        public final long pointCount;
        public final int postingsBlockSize;
        public final int maxTermLength;
        public final long termsDataFp;
        public final long termsIndexFP;
        public final int numPostingBlocks;
        public final long distinctTermCount;
        public final long sameTermsPostingsFP;
        public final long upperPostingsIndexFP;
        public final byte[] minTerm;
        public final byte[] maxTerm;
        public final FileValidation.Meta termsIndexCRC,
        termsCRC,
        orderMapCRC,
        postingsCRC,
        bitpackedCRC,
        upperPostingsCRC,
        upperPostingsOffsetsCRC;
        public final long minRowid, maxRowid;

        public Meta(Map<String, String> map)
        {
            pointCount = Long.parseLong(map.get("pointCount"));
            postingsBlockSize = Integer.parseInt(map.get("postingsBlockSize"));
            maxTermLength = Integer.parseInt(map.get("maxTermLength"));
            termsDataFp = Long.parseLong(map.get("termsDataFp"));
            termsIndexFP = Long.parseLong(map.get("termsIndexFP"));
            numPostingBlocks = Integer.parseInt(map.get("numPostingBlocks"));
            distinctTermCount = Long.parseLong(map.get("distinctTermCount"));
            sameTermsPostingsFP = Long.parseLong(map.get("sameTermsPostingsFP"));
            upperPostingsIndexFP = Long.parseLong(map.get("upperPostingsIndexFP"));
            minTerm = Base64.getDecoder().decode(map.get("minTerm"));
            maxTerm = Base64.getDecoder().decode(map.get("maxTerm"));
            termsIndexCRC = new FileValidation.Meta(map.get("termsIndexCRC"));
            termsCRC = new FileValidation.Meta(map.get("termsCRC"));
            orderMapCRC = new FileValidation.Meta(map.get("orderMapCRC"));
            postingsCRC = new FileValidation.Meta(map.get("postingsCRC"));
            bitpackedCRC = new FileValidation.Meta(map.get("bitpackedCRC"));

            if (map.containsKey("upperPostingsCRC"))
                upperPostingsCRC = new FileValidation.Meta(map.get("upperPostingsCRC"));
            else
                upperPostingsCRC = null;
            if (map.containsKey("upperPostingsOffsetsCRC"))
                upperPostingsOffsetsCRC = new FileValidation.Meta(map.get("upperPostingsOffsetsCRC"));
            else
                upperPostingsOffsetsCRC = null;
            minRowid = Long.parseLong(map.get("minRowid"));
            maxRowid = Long.parseLong(map.get("maxRowid"));
        }

        public Map<String, String> stringMap()
        {
            Map<String, String> map = new HashMap<>();
            map.put("pointCount", Long.toString(pointCount));
            map.put("postingsBlockSize", Integer.toString(postingsBlockSize));
            map.put("maxTermLength", Integer.toString(maxTermLength));
            map.put("termsDataFp", Long.toString(termsDataFp));
            map.put("termsIndexFP", Long.toString(termsIndexFP));
            map.put("numPostingBlocks", Integer.toString(numPostingBlocks));
            map.put("distinctTermCount", Long.toString(distinctTermCount));
            map.put("sameTermsPostingsFP", Long.toString(sameTermsPostingsFP));
            map.put("upperPostingsIndexFP", Long.toString(upperPostingsIndexFP));
            map.put("minTerm", Base64.getEncoder().encodeToString(minTerm));
            map.put("maxTerm", Base64.getEncoder().encodeToString(maxTerm));
            map.put("termsIndexCRC", termsIndexCRC.toBase64());
            map.put("termsCRC", termsCRC.toBase64());
            map.put("orderMapCRC", orderMapCRC.toBase64());
            map.put("postingsCRC", postingsCRC.toBase64());
            map.put("bitpackedCRC", bitpackedCRC.toBase64());
            if (upperPostingsCRC != null)
                map.put("upperPostingsCRC", upperPostingsCRC.toBase64());
            if (upperPostingsOffsetsCRC != null)
                map.put("upperPostingsOffsetsCRC", upperPostingsOffsetsCRC.toBase64());
            map.put("minRowid", Long.toString(minRowid));
            map.put("maxRowid", Long.toString(maxRowid));
            return map;
        }

        public static byte[] readBytes(IndexInput input) throws IOException
        {
            int len = input.readVInt();
            byte[] bytes = new byte[len];
            input.readBytes(bytes, 0, len);
            return bytes;
        }

        public static void writeBytes(byte[] bytes, IndexOutput out) throws IOException
        {
            out.writeVInt(bytes.length);
            out.writeBytes(bytes, bytes.length);
        }

        public Meta(long pointCount,
                    int blockSize,
                    int maxTermLength,
                    long termsDataFp,
                    long termsIndexFP,
                    int numPostingBlocks,
                    long distinctTermCount,
                    long sameTermsPostingsFP,
                    long upperPostingsIndexFP,
                    byte[] minTerm,
                    byte[] maxTerm,
                    FileValidation.Meta termsIndexCRC,
                    FileValidation.Meta termsCRC,
                    FileValidation.Meta orderMapCRC,
                    FileValidation.Meta postingsCRC,
                    FileValidation.Meta bitpackedCRC,
                    FileValidation.Meta upperPostingsCRC,
                    FileValidation.Meta upperPostingsOffsetsCRC,
                    long minRowid,
                    long maxRowid)
        {
            this.pointCount = pointCount;
            this.postingsBlockSize = blockSize;
            this.maxTermLength = maxTermLength;
            this.termsDataFp = termsDataFp;
            this.termsIndexFP = termsIndexFP;
            this.numPostingBlocks = numPostingBlocks;
            this.distinctTermCount = distinctTermCount;
            this.sameTermsPostingsFP = sameTermsPostingsFP;
            this.upperPostingsIndexFP = upperPostingsIndexFP;
            this.minTerm = minTerm;
            this.maxTerm = maxTerm;
            this.termsIndexCRC = termsIndexCRC;
            this.termsCRC = termsCRC;
            this.orderMapCRC = orderMapCRC;
            this.postingsCRC = postingsCRC;
            this.bitpackedCRC = bitpackedCRC;
            this.upperPostingsCRC = upperPostingsCRC;
            this.upperPostingsOffsetsCRC = upperPostingsOffsetsCRC;
            this.minRowid = minRowid;
            this.maxRowid = maxRowid;
        }
    }

    /**
     * Reader for a block terms index.
     */
    @ThreadSafe
    public static class Reader implements AutoCloseable
    {
        private static final Comparator<PostingList.PeekablePostingList> COMPARATOR = Comparator.comparingLong(PostingList.PeekablePostingList::peek);

        protected final IndexDescriptor indexDescriptor;
        protected final IndexContext context;
        protected final SegmentMetadata.ComponentMetadataMap componentMetadatas;
        protected final FileHandle bitpackedHandle, postingsHandle, blockOrderMapFile, termsDataHandle, termsIndexHandle, upperPostingsHandle;
        protected final V3PerIndexFiles indexFiles;
        final BlockPackedReader blockPostingOffsetsReader;
        private final NumericValuesMeta blockTermsOffsetsMeta, blockPostingsOffsetsMeta, blockOrderMapMeta;
        private final SeekingRandomAccessInput orderMapRandoInput;
        private final DirectReaders.Reader orderMapReader;
        private final MonotonicBlockPackedReader blockTermsOffsetsReader;
        private final BlockPackedReader blockOrderMapFPReader;
        final Meta meta;
        final BinaryTreePostingsReader binaryTreePostingsReader;
        protected long ramBytesUsed;

        /**
         * Segment reader version.
         *
         * @param indexDescriptor
         * @param context
         * @param indexFiles
         * @param componentMetadatas
         * @throws IOException
         */
        public Reader(IndexDescriptor indexDescriptor,
                      IndexContext context,
                      V3PerIndexFiles indexFiles,
                      SegmentMetadata.ComponentMetadataMap componentMetadatas) throws IOException
        {
            this.indexDescriptor = indexDescriptor;
            this.context = context;
            this.indexFiles = indexFiles;
            this.componentMetadatas = componentMetadatas;

            // derive Meta from a ComponentMetadata
            final SegmentMetadata.ComponentMetadata termsDataCompMeta = componentMetadatas.get(BLOCK_TERMS_DATA);
            this.meta = new Meta(termsDataCompMeta.attributes);

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
            this.blockTermsOffsetsMeta = new NumericValuesMeta(blockTermsOffsetsCompMeta.attributes);

            blockTermsOffsetsReader = new MonotonicBlockPackedReader(bitpackedHandle, blockTermsOffsetsMeta);
            this.ramBytesUsed += blockTermsOffsetsReader.memoryUsage();

            final SegmentMetadata.ComponentMetadata blockPostingsOffsetsCompMeta = componentMetadatas.get(IndexComponent.BLOCK_POSTINGS_OFFSETS);
            this.blockPostingsOffsetsMeta = new NumericValuesMeta(blockPostingsOffsetsCompMeta.attributes);
            this.blockPostingOffsetsReader = new BlockPackedReader(bitpackedHandle, blockPostingsOffsetsMeta);
            this.ramBytesUsed += blockPostingOffsetsReader.memoryUsage();

            final SegmentMetadata.ComponentMetadata blockOrderMapCompMeta = componentMetadatas.get(IndexComponent.BLOCK_ORDERMAP);
            this.blockOrderMapMeta = new NumericValuesMeta(blockOrderMapCompMeta.attributes);
            this.blockOrderMapFPReader = new BlockPackedReader(bitpackedHandle, blockOrderMapMeta);
            this.ramBytesUsed += blockOrderMapFPReader.memoryUsage();

//            if (meta.numPostingBlocks != postingBlockFPs.length())
//                throw new IllegalStateException();
//
//            if (meta.numPostingBlocks != orderMapFPs.length())
//                throw new IllegalStateException();

            this.orderMapReader = DirectReaders.getReaderForBitsPerValue((byte) DirectWriter.unsignedBitsRequired(meta.postingsBlockSize - 1));
            this.orderMapRandoInput = new SeekingRandomAccessInput(IndexInputReader.create(blockOrderMapFile));

            if (indexDescriptor.componentExists(IndexComponent.BLOCK_UPPER_POSTINGS, context, indexFiles.isTemporary()))
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
                throw new IOException("corrupt");
        }

        /**
         * Enables iterating all points (term + rowids) in index order.
         *
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
            final IndexInput postingsInput;

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
            public int compareTo(PointsIterator o)
            {
                final int cmp = bytesCursor.currentTerm.compareTo(o.bytesCursor.currentTerm);
                if (cmp != 0)
                   return cmp;
                return Long.compare(rowId(), o.rowId());
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
                bytesCursor.close();
                FileUtils.closeQuietly(postingsInput, postings, postingsFPs, orderMapFPs);
                if (toClose != null)
                    toClose.close();
            }

            /**
             * Moves iterator to the next point.
             * @return
             * @throws IOException
             */
            public boolean next() throws IOException
            {
                try
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
                        throw new IllegalStateException();

                    this.point++;
                    return true;
                }
                catch (EOFException ex)
                {
                    throw new IOException("point="+point+" meta.pointCount="+meta.pointCount, ex);
                }
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

        PostingList sameValuePostings() throws IOException
        {
            if (meta.sameTermsPostingsFP != -1)
            {
                final IndexInput input = IndexInputReader.create(postingsHandle);
                return new PostingsReader(input, meta.sameTermsPostingsFP, QueryEventListener.PostingListEventListener.NO_OP);
            }
            else
                return null;
        }

        public long memoryUsage()
        {
            return ramBytesUsed;
        }

        @Override
        public void close() throws Exception
        {
            // PerIndexFiles caches file handles, close handles only if PerIndexFiles is not present
            if (indexFiles == null)
                FileUtils.closeQuietly(termsIndexHandle, termsDataHandle, bitpackedHandle, blockOrderMapFile, postingsHandle);
            FileUtils.closeQuietly(orderMapRandoInput);
        }

        /**
         * If necessary, exclusive ranges are processed by arithmetic byte increment and/or decrement operation.
         * Avoids IF exclusive code handling elsewhere in the code which is overly complicated.
         */
        public PostingList search(ByteComparable start,
                                  boolean startExclusive,
                                  ByteComparable end,
                                  boolean endExclusive) throws IOException
        {
            ByteComparable realStart = start;
            ByteComparable realEnd = end;

            if (startExclusive && start != null)
            {
                final byte[] startBytes = ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41));
                realStart = ByteComparable.fixedLength(nudge(startBytes));
            }
            if (endExclusive && end != null)
            {
                final byte[] endBytes = ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41));
                realEnd = ByteComparable.fixedLength(nudgeReverse(endBytes, meta.maxTermLength));
            }

            if (start == null)
                realStart = minTerm();

            if (end == null)
                realEnd = maxTerm();

            return search(realStart, realEnd);
        }

        /**
         * Convert ByteComparable to BytesRef and search.
         */
        public PostingList search(ByteComparable start, ByteComparable end) throws IOException
        {
            final byte[] startBytes = ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41));
            final byte[] endBytes = ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41));
            return search(new BytesRef(startBytes), new BytesRef(endBytes));
        }

        public PostingList intersect(BinaryTree.IntersectVisitor visitor) throws IOException
        {
            final PointValues.Relation relation = visitor.compare(new BytesRef(meta.minTerm), new BytesRef(meta.maxTerm));

            if (relation == PointValues.Relation.CELL_OUTSIDE_QUERY)
                return null;

            final LongArray postingBlockFPs = new LongArray.DeferredLongArray(() -> blockPostingOffsetsReader.open());
            final LongArray orderMapFPs = new LongArray.DeferredLongArray(() -> blockOrderMapFPReader.open());

            // it's important to create the input streams once per query as
            // there is a significant performance cost atm
            final IndexInputReader treeIndexInput = IndexInputReader.create(termsIndexHandle);
            final IndexInputReader postingsInput = IndexInputReader.create(postingsHandle);
            final IndexInputReader upperPostingsInput = IndexInputReader.create(upperPostingsHandle);

            try (final BinaryTree.Reader treeReader = new BinaryTree.Reader(meta.numPostingBlocks,
                                                                            meta.pointCount,
                                                                            meta.postingsBlockSize,
                                                                            treeIndexInput))
            {
                final Intersection intersection = relation == PointValues.Relation.CELL_INSIDE_QUERY ?
                                                  new Intersection(postingsInput,
                                                                   upperPostingsInput,
                                                                   treeReader,
                                                                   postingBlockFPs,
                                                                   orderMapFPs) :
                                                  new FilteringIntersection(postingsInput,
                                                                            upperPostingsInput,
                                                                            treeReader,
                                                                            visitor,
                                                                            postingBlockFPs,
                                                                            orderMapFPs);

                return intersection.execute();
            }
        }

        public class Intersection
        {
            protected final BinaryTree.Reader treeReader;
            protected final IndexInput postingsInput, upperPostingsInput;
            protected final LongArray postingBlockFPs, orderMapFPs;

            public Intersection(IndexInput postingsInput,
                                IndexInput upperPostingsInput,
                                BinaryTree.Reader treeReader,
                                final LongArray postingBlockFPs,
                                final LongArray orderMapFPs)
            {
                this.postingsInput = postingsInput;
                this.upperPostingsInput = upperPostingsInput;
                this.treeReader = treeReader;
                this.postingBlockFPs = postingBlockFPs;
                this.orderMapFPs = orderMapFPs;
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
//                    if (!(t instanceof AbortedOperationException))
//                        logger.error(indexContext.logMessage("kd-tree intersection failed on {}"), indexFile.path(), t);
//
//                    closeOnException();
                    throw Throwables.cleaned(t);
                }
            }

            protected PostingList mergePostings(PriorityQueue<PostingList.PeekablePostingList> postingLists)
            {
                if (postingLists.isEmpty())
                {
                    FileUtils.closeQuietly(postingsInput, upperPostingsInput, treeReader, postingBlockFPs, orderMapFPs);
                    return null;
                }
                else
                {
//                    if (logger.isTraceEnabled())
//                        logger.trace(indexContext.logMessage("[{}] Intersection completed in {} microseconds. {} leaf and internal posting lists hit."),
//                                     indexFile.path(), elapsedMicros, postingLists.size());

                    // System.out.println("postingLists.size="+postingLists.size());

                    return MergePostingList.merge(postingLists, () -> FileUtils.close(postingsInput, upperPostingsInput, treeReader, postingBlockFPs, orderMapFPs));
                }
            }

            public void collectPostingLists(PriorityQueue<PostingList.PeekablePostingList> postingLists) throws IOException
            {
                final int nodeID = treeReader.getNodeID();

                if (treeReader.isLeafNode())
                {
                    final int blockId = treeReader.getBlockID();
                    final long blockPostingsFP = postingBlockFPs.get(blockId);

                    // System.out.println("collectPostingLists nodeID="+nodeID+" blockId="+blockId+" blockPostingsFP="+blockPostingsFP);

                    // A -1 postings fp means there is a single posting list
                    // for multiple leaf blocks because the values are the same
                    if (blockPostingsFP != -1)
                    {
                        final PostingsReader postings = new PostingsReader(postingsInput,
                                                                           blockPostingsFP,
                                                                           QueryEventListener.PostingListEventListener.NO_OP);
                        postingLists.add(postings.peekable());
                    }
                    return;
                }
                else if (binaryTreePostingsReader.exists(nodeID))
                {
                    // System.out.println("Upper level postings nodeID="+nodeID);
                    final long upperPostingsFP = binaryTreePostingsReader.getPostingsFilePointer(nodeID);
                    final PostingsReader postingsReader = new PostingsReader(upperPostingsInput, upperPostingsFP, QueryEventListener.PostingListEventListener.NO_OP);
                    postingLists.add(postingsReader.peekable());
                    return;
                }

                // Preconditions.checkState(!treeReader.isLeafNode(), "Leaf node %s does not have upper postings.", treeReader.getNodeID());

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
                                         final LongArray orderMapFPs)
            {
                super(postingsInput, upperPostingsInput, treeReader, postingBlockFPs, orderMapFPs);
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

                // System.out.println("collectPostingLists nodeid="+treeReader.getNodeID()+" blockid="+treeReader.getBlockID()+" nodeExists="+treeReader.nodeExists()+" isLeafNode="+treeReader.isLeafNode()+" cellMinPacked="+toInt(cellMinPacked)+" cellMaxPacked="+toInt(cellMaxPacked)+" relation="+r);

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

                // System.out.println("filterLeaf block="+block);

                final PostingList postings = filterBlock(visitor,
                                                         block,
                                                         postingsFP,
                                                         postingBlockFPs,
                                                         orderMapFPs,
                                                         postingsInput);

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

                // System.out.println("visitNode splitPackedValue="+toInt(splitPackedValue)+" splitDimValue="+toInt(splitDimValue));

                // Recurse on left sub-tree:
                BinaryTree.Reader.copyBytes(cellMaxPacked, splitPackedValue);
                BinaryTree.Reader.copyBytes(splitDimValue, splitPackedValue);

                treeReader.pushLeft();
                // System.out.println("visitNode cellMinPacked="+toInt(cellMinPacked)+" splitPackedValue="+toInt(splitPackedValue));
                collectPostingLists(postingLists, cellMinPacked, splitPackedValue);
                treeReader.pop();

                // Restore the split dim value since it may have been overwritten while recursing:
                BinaryTree.Reader.copyBytes(splitPackedValue, splitDimValue);

                // Recurse on right sub-tree:
                BinaryTree.Reader.copyBytes(cellMinPacked, splitPackedValue);
                BinaryTree.Reader.copyBytes(splitDimValue, splitPackedValue);

                treeReader.pushRight();
                // System.out.println("visitNode2 splitPackedValue="+toInt(splitPackedValue)+" cellMaxPacked="+toInt(cellMaxPacked));
                collectPostingLists(postingLists, splitPackedValue, cellMaxPacked);
                treeReader.pop();
            }
        }

        @SuppressWarnings("resource")
        PostingList search(BytesRef startTerm,
                           BytesRef endTerm) throws IOException
        {
            if (startTerm == null)
                throw new IllegalArgumentException("startTerm="+startTerm);
            if (endTerm == null)
                throw new IllegalArgumentException();

            final BinaryTree.IntersectVisitor visitor = BinaryTreePostingsReader.bkdQueryFrom(startTerm, endTerm);
            return intersect(visitor);
        }

        PostingList filterBlock(BinaryTree.IntersectVisitor visitor,
                                final int block,
                                final MutableValueLong postingsFP,
                                final LongArray blockPostingsFPs,
                                final LongArray orderMapFPs,
                                final IndexInput postingsInput) throws IOException
        {
            final long start = (long)block * (long)meta.postingsBlockSize;

            long maxord = ((long)block + 1) * (long)meta.postingsBlockSize - 1;
            maxord = maxord >= meta.pointCount ? meta.pointCount - 1 : maxord;

            postingsFP.value = blockPostingsFPs.get(block);

            if (postingsFP.value == -1)
            {
                return null;
            }

            final OrdinalPostingList postings = new PostingsReader(postingsInput,
                                                                   postingsFP.value,
                                                                   QueryEventListener.PostingListEventListener.NO_OP);

            final long orderMapFP = orderMapFPs.get(block); // here for assertion, move lower

            // must be a same terms posting list
            // return as is
            // resurect for STAR-1524
//            if (postings.size() > meta.postingsBlockSize)
//            {
//                assert orderMapFP == -1;
//                // System.out.println("filterBlock postings size="+postings.size()+" max postings size="+meta.postingsBlockSize);
//                return postings;
//            }

            final Pair<Long, Long> minMaxPoints = filterPoints(visitor, start, maxord);

            // System.out.println("minMaxPoints="+minMaxPoints);

            if (minMaxPoints.left.longValue() == -1)
                return null;

            final int cardinality = (int) (minMaxPoints.right - minMaxPoints.left) + 1;

            if (cardinality == 0)
                return null;

            final int startOrd = (int) (minMaxPoints.left % meta.postingsBlockSize);
            final int endOrd = (int) (minMaxPoints.right % meta.postingsBlockSize);

            // System.out.println("startOrd="+startOrd+" endOrd="+endOrd);

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

            final BytesRef startTerm = ((BinaryTreePostingsReader.RangeQueryVisitor)visitor).getLowerBytes();
            final BytesRef endTerm = ((BinaryTreePostingsReader.RangeQueryVisitor)visitor).getUpperBytes();

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
                termsData.close();
                FileUtils.closeQuietly(termBlockFPs);
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

                    System.out.println("meta.termsDataFp="+meta.termsDataFp+" blockIndex="+blockIndex+" blockAddress="+blockAddress);

                    termsData.seek(blockAddress); // MAYBE THE MAIN PROBLEM
                    //termsData.seek(blockAddress + meta.termsDataFp);
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

                System.out.println("BlockTerms.next() filepointer="+termsData.getFilePointer());

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

                assert prefixLength + suffixLength <= meta.maxTermLength : "prefixLength="+prefixLength+" suffixLength="+suffixLength+" maxTermLength="+meta.maxTermLength;
                currentTerm.length = prefixLength + suffixLength;
                termsData.readBytes(currentTerm.bytes, prefixLength, suffixLength);
                return true;
            }
        }
    }

    /**
     * Writes block terms and postings.
     */
    @NotThreadSafe
    public static class Writer
    {
        final BytesRefArray minBlockTerms = new BytesRefArray(Counter.newCounter()); // per posting block min terms
        private final BytesRefBuilder prevTerm = new BytesRefBuilder();
        private final BytesRefBuilder spare = new BytesRefBuilder();
        private final BytesRefBuilder minTerm = new BytesRefBuilder();
        private final int postingsBlockSize; // size of each postings block
        private final IndexOutputWriter termsOut, termsIndexOut, postingsOut, orderMapOut;
        private final LongArrayList termBlockFPs = new LongArrayList();
        private final LongArrayList blockPostingsFPs = new LongArrayList(); // per postings block postings file pointer
        private final LongArrayList blockOrderMapFPs = new LongArrayList(); // per postings block order map file pointer
        private final IndexDescriptor indexDescriptor;
        private final IndexContext context;
        private final PostingsWriter postingsWriter;
        private final RowIDIndex[] rowIDIndices;
        private final boolean segmented;
        private final long orderMapStartFP, termsStartFP, termsIndexStartFP;
        private int maxTermLength = 0;
        private int postingsBlockCount = 0;
        private long pointId = 0; // point count
        private long distinctTermCount = 0;
        private CachedBlock currentPostingsBlock = new CachedBlock(), previousPostingsBlock = new CachedBlock();
        private LongBitSet postingsBlockSameTerms = new LongBitSet(DEFAULT_POSTINGS_BLOCK_SIZE); // postings blocks with same terms
        private LongBitSet rowIdsSeen = new LongBitSet(DEFAULT_POSTINGS_BLOCK_SIZE);
        private BytesBlockCache currentBytesCache = new BytesBlockCache();
        private BytesBlockCache previousBytesCache = new BytesBlockCache();
        private long lastBytesBlockFP = -1;
        protected Meta meta;
        public long maxRowId = -1;
        public long minRowId = Long.MAX_VALUE;
        private long lastRowID = -1;

        public Writer(IndexDescriptor indexDescriptor, IndexContext context, boolean segmented) throws IOException
        {
            this(DEFAULT_POSTINGS_BLOCK_SIZE, indexDescriptor, context, segmented);
        }

        public Writer(IndexDescriptor indexDescriptor, IndexContext context) throws IOException
        {
            this(DEFAULT_POSTINGS_BLOCK_SIZE, indexDescriptor, context, false);
        }

        public Writer(int postingsBlockSize, IndexDescriptor indexDescriptor, IndexContext context, boolean segmented) throws IOException
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

        interface TermRowIDCallback
        {
            void added(BytesRef term, long rowid, boolean sameTermAsLast) throws IOException;

            void finish(SegmentMetadata.ComponentMetadataMap components) throws IOException;
        }

        /**
         * Called by memtable index writer.
         */
        public SegmentMetadata.ComponentMetadataMap writeAll(final TermsIterator terms,
                                                             TermRowIDCallback callback) throws IOException
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
                         boolean newTerm = add(term, rowid, callback);
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
         *
         * Closes outputs.
         *
         * @param components Write to segment meta data
         * @return Index meta data
         * @throws IOException
         */
        public Meta finish(final SegmentMetadata.ComponentMetadataMap components) throws Exception
        {
            if (components == null)
                throw new IllegalStateException();

            // flush buffers
            flushLastBuffers();

            // write postingsBlockSameTerms bitset to disk as posting list
            final PostingList blockSameTermsPostings = LongBitSetPostings.postings(postingsBlockSameTerms);
            final long sameTermsPostingsFP;
            if (blockSameTermsPostings != null)
                sameTermsPostingsFP = postingsWriter.write(blockSameTermsPostings);
            else
                sameTermsPostingsFP = -1;

            postingsWriter.complete();

            orderMapOut.close();
            termsOut.close();
            postingsOut.close();

            // backfill posting FP's with -1 to a real FP
//            for (int x = blockPostingsFPs.size() - 2; x >= 0; x--)
//            {
//                if (blockPostingsFPs.getLong(x) == -1)
//                    blockPostingsFPs.setLong(x, blockPostingsFPs.getLong(x + 1));
//            }

            assert minBlockTerms.size() <= postingsBlockSameTerms.length() : "minBlockTerms.size=" + minBlockTerms.size() + " postingsBlockSameTerms.length=" + postingsBlockSameTerms.length() + " postingsBlockCount=" + postingsBlockCount;

            final BinaryTree.Writer treeWriter = new BinaryTree.Writer();
            treeWriter.finish(minBlockTerms, termsIndexOut);
            termsIndexOut.close();

            final long termsIndexFP = 0;

            BinaryTreePostingsWriter.Result treePostingsResult = null;

            if (!segmented)
            {
                try (final V3PerIndexFiles perIndexFiles = new V3PerIndexFiles(indexDescriptor, context, segmented);
                     final FileHandle binaryTreeFile = perIndexFiles.getFile(BLOCK_TERMS_INDEX);
                     final IndexInput binaryTreeInput = IndexInputReader.create(binaryTreeFile);
                     final IndexOutput upperPostingsOut = indexDescriptor.openPerIndexOutput(BLOCK_UPPER_POSTINGS, context, true, segmented);
                     final FileHandle blockPostingsHandle = perIndexFiles.getFile(BLOCK_POSTINGS))
                {
                    final BinaryTree.Reader treeReader = new BinaryTree.Reader(minBlockTerms.size(),
                                                                               pointCount(),
                                                                               postingsBlockSize,
                                                                               binaryTreeInput);

                    IndexWriterConfig config = IndexWriterConfig.fromOptions("", UTF8Type.instance, new HashMap<>());

                    final BinaryTreePostingsWriter treePostingsWriter = new BinaryTreePostingsWriter(config);
                    treeReader.traverse(new IntArrayList(), treePostingsWriter);

                    treePostingsResult = treePostingsWriter.finish(minBlockTerms.size(),
                                                                   blockPostingsHandle,
                                                                   new LongArrayImpl(blockPostingsFPs),
                                                                   upperPostingsOut);
                }
            }

            final long bitpackStartFP;
            try (final IndexOutput bitPackOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_BITPACKED, context, true, segmented))
            {
                bitpackStartFP = bitPackOut.getFilePointer();
            }

            try (final IndexOutput bitPackOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_BITPACKED, context, true, segmented))
            {
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
            }

            final FileValidation.Meta termsIndexCRC = createCRCMeta(termsIndexStartFP, IndexComponent.BLOCK_TERMS_INDEX);
            final FileValidation.Meta termsCRC = createCRCMeta(termsStartFP, BLOCK_TERMS_DATA);
            final FileValidation.Meta orderMapCRC = createCRCMeta(orderMapStartFP, IndexComponent.BLOCK_ORDERMAP);
            final FileValidation.Meta postingsCRC = createCRCMeta(postingsWriter.getStartOffset(), IndexComponent.BLOCK_POSTINGS);
            final FileValidation.Meta bitpackedCRC = createCRCMeta(bitpackStartFP, IndexComponent.BLOCK_BITPACKED);

            meta = new Meta(pointId,
                            postingsBlockSize,
                            maxTermLength,
                            termsStartFP,
                            termsIndexFP,
                            minBlockTerms.size(),
                            distinctTermCount,
                            sameTermsPostingsFP,
                            treePostingsResult != null ? treePostingsResult.indexFilePointer : -1,
                            BytesRef.deepCopyOf(minTerm.get()).bytes,
                            BytesRef.deepCopyOf(prevTerm.get()).bytes,
                            termsIndexCRC,
                            termsCRC,
                            orderMapCRC,
                            postingsCRC,
                            bitpackedCRC,
                            null,
                            null,
                            minRowId,
                            maxRowId);

            final long postingsLength = postingsOut.getFilePointer() - postingsWriter.getStartOffset();
            final long termsLength = termsOut.getFilePointer() - termsStartFP;

            IndexOutput termsOut2 = indexDescriptor.openPerIndexOutput(BLOCK_TERMS_DATA, context, true, segmented);

            System.out.println("terms file length="+termsOut2.getFilePointer()+" termsStartFP="+termsStartFP+" termsLength="+termsLength+" pointId="+pointId);
            termsOut2.close();

            final long termsIndexLength = termsIndexOut.getFilePointer() - termsIndexStartFP;

            // Postings list file pointers are stored directly in TERMS_DATA, so a root is not needed.
            components.put(BLOCK_TERMS_DATA, termsStartFP, termsStartFP, termsLength, meta.stringMap());
            components.put(IndexComponent.POSTING_LISTS, -1, postingsWriter.getStartOffset(), postingsLength, meta.stringMap());
            components.put(IndexComponent.BLOCK_TERMS_INDEX, meta.termsIndexFP, termsIndexStartFP, termsIndexLength, meta.stringMap());

            // the upper postings crc's are available
            // after writing the upper postings
            // a new meta is created
            meta = new Meta(pointId,
                            postingsBlockSize,
                            maxTermLength,
                            termsStartFP,
                            termsIndexFP,
                            minBlockTerms.size(),
                            distinctTermCount,
                            sameTermsPostingsFP,
                            treePostingsResult != null ? treePostingsResult.indexFilePointer : -1,
                            BytesRef.deepCopyOf(minTerm.get()).bytes,
                            BytesRef.deepCopyOf(prevTerm.get()).bytes,
                            termsIndexCRC,
                            termsCRC,
                            orderMapCRC,
                            postingsCRC,
                            bitpackedCRC,
                            null,
                            null,
                            //upperPostingsMetaCRC != null ? upperPostingsMetaCRC.upperPostingsCRC : null,
                            //upperPostingsMetaCRC != null ? upperPostingsMetaCRC.upperPostingsOffsetsCRC : null,
                            minRowId,
                            maxRowId);

            // replace relevant components with the new meta string map
//            if (upperPostingsMetaCRC != null)
//            {
//                final Map<String, String> map = meta.stringMap();
//                map.putAll(upperPostingsMetaCRC.meta.stringMap());
//                final Set<IndexComponent> comps = ImmutableSet.of(BLOCK_TERMS_DATA, BLOCK_TERMS_INDEX, BLOCK_UPPER_POSTINGS, BLOCK_POSTINGS);
//                components.replaceMaps(map, comps);
//            }

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

        public boolean blockSameValue(int block)
        {
            return postingsBlockSameTerms.get(block);
        }

        public boolean add(ByteComparable term, long rowid) throws IOException
        {
            copyBytes(term, spare);
            return add(spare.get(), rowid, null);
        }

        public boolean add(ByteComparable term, long rowid, TermRowIDCallback callback) throws IOException
        {
            copyBytes(term, spare);
            return add(spare.get(), rowid, callback);
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
            final BytesRefBuilder prevTerm = new BytesRefBuilder();
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
                    final int prefixLength = bytesDifference(prevTerm.get(), term);
                    final int suffixLength = term.length - prefixLength;

                    termsOut.writeByte((byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));

                    if (prefixLength >= 15)
                        termsOut.writeVInt(prefixLength - 15);
                    if (suffixLength >= 16)
                        termsOut.writeVInt(suffixLength - 16);

                    termsOut.writeBytes(term.bytes, term.offset + prefixLength, term.length - prefixLength);
                }
                prevTerm.copyBytes(term);
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
                {
                    return false;
                }
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
         * @param term Bytes ascending order
         * @param rowid Row id in secondary ascending order
         * @throws IOException
         */
        public boolean add(BytesRef term, long rowid, TermRowIDCallback callback) throws IOException
        {
            boolean sameTermAsLast = prevTerm.get().equals(term);

            if (term.length <= 0)
                throw new IllegalArgumentException("Term length cannot be zero or less");

            if (minTerm.length() == 0)
                minTerm.copyBytes(term);

            if (prevTerm.get().compareTo(term) > 0)
               throw new IllegalArgumentException("Terms must be in ascending lexicographic order");

            minRowId = Math.min(minRowId, rowid);
            maxRowId = Math.max(maxRowId, rowid);

            rowIdsSeen = LongBitSet.ensureCapacity(rowIdsSeen, rowid + 1);
            rowIdsSeen.set(rowid);

            if (!sameTermAsLast)
                distinctTermCount++;
            else if (rowid < lastRowID)
                throw new IllegalStateException("rowid=" + rowid + "less than lastRowID=" + lastRowID);

            if (callback != null)
                callback.added(term, rowid, sameTermAsLast);

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
         *
         * Order map is optional, the order map file pointer may be -1.
         *
         * Returns if the order map was written.
         */
        protected boolean writePostingsAndOrderMap(CachedBlock buffer) throws IOException
        {
            assert buffer.count > 0;

            for (int x = 0; x < buffer.count; x++)
            {
                final long rowid = buffer.postings.getLong(x);
                rowIDIndices[x].rowid = rowid;
                rowIDIndices[x].index = x;
            }

            // sort by row id
            Arrays.sort(rowIDIndices, 0, buffer.count, (o1, o2) -> Long.compare(o1.rowid, o2.rowid));

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
                return true;
            }
            else
            {
                // -1 demarcs blocks with no order map
                blockOrderMapFPs.addLong(-1);
                return false;
            }
        }

        private void processBlock(CachedBlock block)
        {
            minBlockTerms.append(block.minTerm());
        }

        private void flushPreviousBufferAndSwap() throws IOException
        {
            this.postingsBlockSameTerms = LongBitSet.ensureCapacity(this.postingsBlockSameTerms, postingsBlockCount + 1);

            if (!currentPostingsBlock.isEmpty() && currentPostingsBlock.sameTerms())
            {
                postingsBlockSameTerms.set(postingsBlockCount);
            }

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

            postingsBlockSameTerms = LongBitSet.ensureCapacity(postingsBlockSameTerms, postingsBlockCount + 1);

            if (!currentPostingsBlock.isEmpty() && currentPostingsBlock.sameTerms())
                postingsBlockSameTerms.set(postingsBlockCount);

            flushPostingsBlock();

            if (!currentPostingsBlock.isEmpty())
            {
                processBlock(currentPostingsBlock);
                writePostingsAndOrderMap(currentPostingsBlock);

                final long postingsFP = postingsWriter.finishPostings();
                this.blockPostingsFPs.add(postingsFP);
            }
        }

        private void flushPostingsBlock() throws IOException
        {
            if (!previousPostingsBlock.isEmpty())
            {
                processBlock(previousPostingsBlock);
                boolean orderMapWritten = writePostingsAndOrderMap(previousPostingsBlock);

                // if the previous buffer is all same terms as the current buffer
                // then the postings writer is kept open
                // there should not be an order map because the row ids should be in order
//                if (!currentPostingsBlock.isEmpty()
//                    && currentPostingsBlock.sameTerms()
//                    && previousPostingsBlock.sameTerms()
//                    && previousPostingsBlock.minTerm().equals(currentPostingsBlock.minTerm()))
//                {
//                    assert !orderMapWritten;
//                    // -1 demarcs a same terms block to be backfilled later with a real posting file pointer
//                    blockPostingsFPs.addLong(-1);
//                }
//                else
//                {
                final long postingsFP = postingsWriter.finishPostings();
                blockPostingsFPs.addLong(postingsFP);
                //}
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

            public boolean isEmpty()
            {
                return count == 0;
            }

            public BytesRef minTerm()
            {
                return terms.get(spare, 0);
            }

            public boolean sameTerms()
            {
                return sameTerms;
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

    /**
     * Compare the bytes difference, return 0 if there is none.
     */
    public static int bytesDifference(BytesRef priorTerm, BytesRef currentTerm)
    {
        int mismatch = FutureArrays.mismatch(priorTerm.bytes, priorTerm.offset, priorTerm.offset + priorTerm.length, currentTerm.bytes, currentTerm.offset, currentTerm.offset + currentTerm.length);
        return mismatch < 0 ? 0 : mismatch;
    }

    public static ByteComparable fixedLength(BytesRef bytes)
    {
        return ByteComparable.fixedLength(bytes.bytes, bytes.offset, bytes.length);
    }

    /**
     * Appends unsigned byte 0 to the array to order correctly.
     * @param orig Original byte array to operate on
     * @return A new byte array that sorts one unit ahead of the given byte array
     */
    public static byte[] nudge(byte[] orig)
    {
        byte[] bytes = Arrays.copyOf(orig, orig.length + 1);
        bytes[orig.length] = (int) 0; // unsigned requires cast
        return bytes;
    }

    /**
     * @param orig Byte array to opreate on
     * @param maxLength The correct ordering the absolute max bytes length of all compared bytes is specified.
     * @return A byte array that sorts 1 byte unit increment lower
     */
    public static byte[] nudgeReverse(byte[] orig, int maxLength)
    {
        assert orig.length > 0 : "Input byte array must be not empty";
        int lastIdx = orig.length - 1;

        if (UnsignedBytes.toInt(orig[lastIdx]) == 0)
            return Arrays.copyOf(orig, lastIdx);
        else
        {
            // bytes must be max length for correct ordering
            byte[] result = Arrays.copyOf(orig, maxLength);
            result[lastIdx] = unsignedDecrement(orig[lastIdx]);
            Arrays.fill(result, orig.length, result.length, (byte) 255);
            return result;
        }
    }

    public static byte unsignedDecrement(byte b)
    {
        int i = UnsignedBytes.toInt(b);
        int i2 = i - 1; // unsigned decrement
        return (byte) i2;
    }

    public static class LongArrayImpl implements LongArray
    {
        final LongArrayList list;

        public LongArrayImpl(LongArrayList list)
        {
            this.list = list;
        }

        @Override
        public long get(long idx)
        {
            return list.getLong((int)idx);
        }

        @Override
        public long length()
        {
            return list.size();
        }

        @Override
        public long findTokenRowID(long targetToken)
        {
            throw new UnsupportedOperationException();
        }
    }
}
