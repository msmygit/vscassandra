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
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.agrona.collections.IntArrayList;
import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
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
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.disk.v2.sortedterms.TrieRangeIterator;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
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

import static org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryReader.trieSerializer;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_MASK;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.TERMS_DICT_BLOCK_SHIFT;
import static org.apache.cassandra.index.sai.disk.v2.sortedterms.SortedTermsWriter.copyBytes;
import static org.apache.cassandra.index.sai.disk.v3.SegmentNumericValuesWriter.BLOCK_SIZE;
import static org.apache.cassandra.index.sai.disk.v3.SegmentNumericValuesWriter.MONOTONIC_BLOCK_SIZE;

/**
 * Block terms + postings disk index data structure.
 * Each unique posting block min term is indexed into a trie, postings of block size, and optionally an order map when row ids are out of order.
 * A string of posting blocks with the same min term are placed into a multi-block spanning posting list.
 * Posting block min terms are indexed into a trie.  The payload consists of min and max posting block ids encoded as 2 integers into a long.
 * Raw bytes are prefix encoded in blocks of 16 for fast(er) random access than the posting block default size of 1024.
 * Block terms reader uses no significant heap space, all major data structures are disk based.
 * A point is defined as a term + rowid monotonic id.
 * The default postings block size is 1024 or 1k.
 */
// TODO: upper level postings
public class BlockTerms
{
    public static final int DEFAULT_POSTINGS_BLOCK_SIZE = 1024;

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
        public final byte[] minTerm;
        public final byte[] maxTerm;
        public final FileValidation.Meta termsIndexCRC, termsCRC, orderMapCRC, postingsCRC, bitpackedCRC;
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
            minTerm = Base64.getDecoder().decode(map.get("minTerm"));
            maxTerm = Base64.getDecoder().decode(map.get("maxTerm"));
            termsIndexCRC = new FileValidation.Meta(map.get("termsIndexCRC"));
            termsCRC = new FileValidation.Meta(map.get("termsCRC"));
            orderMapCRC = new FileValidation.Meta(map.get("orderMapCRC"));
            postingsCRC = new FileValidation.Meta(map.get("postingsCRC"));
            bitpackedCRC = new FileValidation.Meta(map.get("bitpackedCRC"));
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
            map.put("minTerm", Base64.getEncoder().encodeToString(minTerm));
            map.put("maxTerm", Base64.getEncoder().encodeToString(maxTerm));
            map.put("termsIndexCRC", termsIndexCRC.toBase64());
            map.put("termsCRC", termsCRC.toBase64());
            map.put("orderMapCRC", orderMapCRC.toBase64());
            map.put("postingsCRC", postingsCRC.toBase64());
            map.put("bitpackedCRC", bitpackedCRC.toBase64());
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
                    byte[] minTerm,
                    byte[] maxTerm,
                    FileValidation.Meta termsIndexCRC,
                    FileValidation.Meta termsCRC,
                    FileValidation.Meta orderMapCRC,
                    FileValidation.Meta postingsCRC,
                    FileValidation.Meta bitpackedCRC,
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
            this.minTerm = minTerm;
            this.maxTerm = maxTerm;
            this.termsIndexCRC = termsIndexCRC;
            this.termsCRC = termsCRC;
            this.orderMapCRC = orderMapCRC;
            this.postingsCRC = postingsCRC;
            this.bitpackedCRC = bitpackedCRC;
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
        protected final IndexDescriptor indexDescriptor;
        protected final IndexContext context;
        protected final SegmentMetadata.ComponentMetadataMap componentMetadatas;
        protected final FileHandle bitpackedHandle, postingsHandle, blockOrderMapFile, termsDataHandle, termsIndexHandle;
        protected final V3PerIndexFiles indexFiles;
        private final LongArray termBlockFPs, postingBlockFPs, orderMapFPs;
        private final NumericValuesMeta blockTermsOffsetsMeta, blockPostingsOffsetsMeta, blockOrderMapMeta;
        private final SeekingRandomAccessInput orderMapRandoInput;
        private final DirectReaders.Reader orderMapReader;
        final Meta meta;
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
            final SegmentMetadata.ComponentMetadata termsDataCompMeta = componentMetadatas.get(IndexComponent.BLOCK_TERMS_DATA);
            this.meta = new Meta(termsDataCompMeta.attributes);

            this.termsIndexHandle = indexFiles.getFileAndCache(IndexComponent.BLOCK_TERMS_INDEX);
            validateCRC(termsIndexHandle, meta.termsIndexCRC);

            this.termsDataHandle = indexFiles.getFileAndCache(IndexComponent.BLOCK_TERMS_DATA);
            validateCRC(termsDataHandle, meta.termsCRC);

            this.bitpackedHandle = indexFiles.getFileAndCache(IndexComponent.BLOCK_BITPACKED);
            validateCRC(bitpackedHandle, meta.bitpackedCRC);

            this.postingsHandle = indexFiles.getFileAndCache(IndexComponent.BLOCK_POSTINGS);
            validateCRC(postingsHandle, meta.postingsCRC);

            blockOrderMapFile = this.indexFiles.getFileAndCache(IndexComponent.BLOCK_ORDERMAP);
            validateCRC(blockOrderMapFile, meta.orderMapCRC);

            final SegmentMetadata.ComponentMetadata blockTermsOffsetsCompMeta = componentMetadatas.get(IndexComponent.BLOCK_TERMS_OFFSETS);
            this.blockTermsOffsetsMeta = new NumericValuesMeta(blockTermsOffsetsCompMeta.attributes);

            final MonotonicBlockPackedReader blockTermsOffsetsReader = new MonotonicBlockPackedReader(bitpackedHandle, blockTermsOffsetsMeta);
            this.ramBytesUsed += blockTermsOffsetsReader.memoryUsage();
            this.termBlockFPs = blockTermsOffsetsReader.open();

            final SegmentMetadata.ComponentMetadata blockPostingsOffsetsCompMeta = componentMetadatas.get(IndexComponent.BLOCK_POSTINGS_OFFSETS);
            this.blockPostingsOffsetsMeta = new NumericValuesMeta(blockPostingsOffsetsCompMeta.attributes);
            final MonotonicBlockPackedReader blockPostingOffsetsReader = new MonotonicBlockPackedReader(bitpackedHandle, blockPostingsOffsetsMeta);
            this.ramBytesUsed += blockPostingOffsetsReader.memoryUsage();
            this.postingBlockFPs = blockPostingOffsetsReader.open();

            final SegmentMetadata.ComponentMetadata blockOrderMapCompMeta = componentMetadatas.get(IndexComponent.BLOCK_ORDERMAP);
            this.blockOrderMapMeta = new NumericValuesMeta(blockOrderMapCompMeta.attributes);
            final BlockPackedReader blockOrderMapFPReader = new BlockPackedReader(bitpackedHandle, blockOrderMapMeta);
            this.ramBytesUsed += blockOrderMapFPReader.memoryUsage();
            this.orderMapFPs = blockOrderMapFPReader.open();

            if (meta.numPostingBlocks != postingBlockFPs.length())
                throw new IllegalStateException();

            if (meta.numPostingBlocks != orderMapFPs.length())
                throw new IllegalStateException();

            this.orderMapReader = DirectReaders.getReaderForBitsPerValue((byte) DirectWriter.unsignedBitsRequired(meta.postingsBlockSize - 1));
            this.orderMapRandoInput = new SeekingRandomAccessInput(IndexInputReader.create(blockOrderMapFile));
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
            private PostingList postings;

            public PointsIterator(AutoCloseable toClose, long rowIdOffset)
            {
                this.toClose = toClose;
                this.rowIdOffset = rowIdOffset;
                bytesCursor = cursor();
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
                FileUtils.closeQuietly(postings);
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
                if (point >= meta.pointCount)
                    return false;

                final BytesRef term = bytesCursor.seekToPointId(point);

                if (term == null)
                    throw new IllegalStateException();

                final long block = point / meta.postingsBlockSize;
                if (block > currentPostingsBlock)
                {
                    final long fp = getPostingsFP(block);
                    this.currentPostingsBlock = block;
                    if (fp > currentPostingsBlockFP)
                    {
                        this.currentPostingsBlockFP = fp;
                        final IndexInput input = IndexInputReader.create(postingsHandle);

                        if (postings != null)
                            postings.close();

                        this.postings = new PostingsReader(input, currentPostingsBlockFP, QueryEventListener.PostingListEventListener.NO_OP);

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
            FileUtils.closeQuietly(termBlockFPs, postingBlockFPs, orderMapFPs, orderMapRandoInput);
        }

        /**
         * Iterator over the trie min terms and payload.
         */
        @VisibleForTesting
        @NotThreadSafe
        static class MinTermsIterator implements Iterator<Pair<ByteSource, Long>>, AutoCloseable
        {
            final Iterator<Pair<ByteSource, Long>> delegate;
            final AutoCloseable toClose;

            public MinTermsIterator(Iterator<Pair<ByteSource, Long>> delegate, AutoCloseable toClose)
            {
                this.delegate = delegate;
                this.toClose = toClose;
            }

            @Override
            public void close() throws Exception
            {
                toClose.close();
            }

            @Override
            public boolean hasNext()
            {
                return delegate.hasNext();
            }

            @Override
            public Pair<ByteSource, Long> next()
            {
                return delegate.next();
            }
        }

        @VisibleForTesting
        MinTermsIterator minTermsIterator()
        {
            TrieRangeIterator reader = new TrieRangeIterator(termsIndexHandle.instantiateRebufferer(),
                                                             meta.termsIndexFP,
                                                             null,
                                                             null,
                                                             true,
                                                             true);
            final Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
            return new MinTermsIterator(iterator, reader);
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
                byte[] startBytes = ByteSourceInverse.readBytes(start.asComparableBytes(ByteComparable.Version.OSS41));
                realStart = nudge(start, startBytes.length - 1);
            }
            if (endExclusive && end != null)
            {
                byte[] endBytes = ByteSourceInverse.readBytes(end.asComparableBytes(ByteComparable.Version.OSS41));
                realEnd = nudgeReverse(end, endBytes.length - 1);
            }

            if (start == null)
                realStart = minTerm();

            if (end == null)
                realEnd = maxTerm();

            final PostingList postingList = search(realStart, realEnd);
            return postingList;
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

        /**
         * Main functional search method.
         *
         * 1) The trie is queried for matching posting block min terms.
         * 2) Min max block ids from the trie payload are used to iterate on the posting file
         *    pointers in ascending order, non-duplicate posting file pointers and posting lists.
         * 3) The first block is always filtered, unless the block is all same values.
         * 4) Posting lists are merged in a MergePostingList
         *
         * @param startTerm Min range bytes inclusive; no null
         * @param endTerm Max range bytes inclusive; no null
         * @return Posting list of range matching rowids
         * @throws IOException
         */
        public PostingList search(BytesRef startTerm, BytesRef endTerm) throws IOException
        {
            if (startTerm == null)
                throw new IllegalArgumentException("startTerm="+startTerm);
            if (endTerm == null)
                throw new IllegalArgumentException();

            Pair<Integer, Integer> blockMinMax = searchTermsIndex(startTerm, endTerm);

            int blockMax = blockMinMax.right.intValue();

            // a negative blockMax means same values
            boolean lastBlockSameValues = false;
            if (blockMax < 0)
            {
                blockMax *= -1;
                lastBlockSameValues = true;
            }

            int minBlock = blockMinMax.left.intValue() == 0 ? 0 : blockMinMax.left.intValue() - 1;

            // filter 1 block only and return
            if (minBlock == blockMax)
            {
                return filterBlock(startTerm, endTerm, minBlock, new MutableValueLong());
            }

            int minBlockUse = blockMinMax.left.intValue();
            final PriorityQueue<PostingList.PeekablePostingList> postingLists = new PriorityQueue<>(100, Comparator.comparingLong(PostingList.PeekablePostingList::peek));

            long lastFP = -1;

            final MutableValueLong firstFilterFP = new MutableValueLong();
            final PostingList firstFilterPostings = filterBlock(startTerm, endTerm, minBlock, firstFilterFP);
            // the first block is always filtered the as there may be matching terms
            // in the block previous to the minBlock from the trie
            if (firstFilterPostings != null)
            {
                postingLists.add(firstFilterPostings.peekable());
                lastFP = firstFilterFP.value;
            }

            for (int block = minBlockUse; block < blockMax; block++)
            {
                final long fp = getPostingsFP(block);

                // avoid adding duplicate posting lists
                if (lastFP != fp)
                {
                    final IndexInput input = IndexInputReader.create(postingsHandle);
                    postingLists.add(new PostingsReader(input, fp, QueryEventListener.PostingListEventListener.NO_OP).peekable());
                }
                lastFP = fp;
            }

            // if the last block has same values there's no need to filter
            if (lastBlockSameValues)
            {
                final long fp = getPostingsFP(blockMax);

                // avoid adding duplicate posting lists
                if (lastFP != fp)
                {
                    final IndexInput input = IndexInputReader.create(postingsHandle);
                    postingLists.add(new PostingsReader(input, fp, QueryEventListener.PostingListEventListener.NO_OP).peekable());
                }
            }
            else
            {
                PostingList lastFilterPostings = filterBlock(startTerm, endTerm, blockMax, new MutableValueLong());
                if (lastFilterPostings != null)
                    postingLists.add(lastFilterPostings.peekable());
            }

            return MergePostingList.merge(postingLists);
        }

        /**
         * Obtain the min max points ids of a given range using the prefix encoded bytes.
         *
         * @param startTerm Start term inclusive
         * @param endTerm End term inclusive
         * @param block Block id to filter
         * @param postingsFP File pointer object
         * @return Filtered posting list
         * @throws IOException
         */
        PostingList filterBlock(final BytesRef startTerm,
                                final BytesRef endTerm,
                                final int block,
                                final MutableValueLong postingsFP) throws IOException
        {
            if (startTerm == null || endTerm == null)
                throw new IllegalArgumentException();

            final long start = (long)block * (long)meta.postingsBlockSize;

            long maxord = ((long)block + 1) * (long)meta.postingsBlockSize - 1;
            maxord = maxord >= meta.pointCount ? meta.pointCount - 1 : maxord;

            final Pair<Long, Long> minMaxPoints = filterPoints(startTerm, endTerm, start, maxord);

            if (minMaxPoints.left.longValue() == -1)
                return null;

            final OrdinalPostingList postings = openBlockPostings(block, postingsFP);

            if (postings.size() > meta.postingsBlockSize)
                return null;

            final long orderMapFP = getOrderMapFP(block);

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

        // TODO: for debugging, remove
        @VisibleForTesting
        IntArrayList openOrderMap(int block) throws IOException
        {
            long fp = getOrderMapFP(block);

            if (fp == -1)
                return null;

            IntArrayList list = new IntArrayList();
            for (int x = 0; x < meta.postingsBlockSize; x++)
            {
                int ord = (int) orderMapReader.get(orderMapRandoInput, fp, x);
                list.add(ord);
            }
            return list;
        }

        long getOrderMapFP(int block)
        {
            return orderMapFPs.get(block);
        }

        OrdinalPostingList openBlockPostings(long block) throws IOException
        {
            final long fp = getPostingsFP(block);
            IndexInput input = IndexInputReader.create(postingsHandle);
            return new PostingsReader(input, fp, QueryEventListener.PostingListEventListener.NO_OP);
        }

        OrdinalPostingList openBlockPostings(int block, MutableValueLong postingsFP) throws IOException
        {
            final long fp = getPostingsFP(block);
            postingsFP.value = fp;
            IndexInput input = IndexInputReader.create(postingsHandle);
            return new PostingsReader(input, fp, QueryEventListener.PostingListEventListener.NO_OP);
        }

        long getPostingsFP(long block)
        {
            return postingBlockFPs.get(block);
        }

        /**
         * Filter points by a start and end term in a given point id range using
         * @see BytesCursor
         *
         * Used to filter singular posting blocks
         *
         * @param startTerm Start term inclusive
         * @param endTerm End term inclusive
         * @param start Start point id
         * @param end End point id
         * @return Min and max point ids
         * @throws IOException
         */
        Pair<Long, Long> filterPoints(final BytesRef startTerm,
                                      final BytesRef endTerm,
                                      final long start,
                                      final long end) throws IOException
        {
            long idx = start;
            long startIdx = -1;
            long endIdx = end;

            try (final BytesCursor bytesCursor = cursor())
            {
                for (; idx <= end; idx++)
                {
                    final BytesRef term = bytesCursor.seekToPointId(idx);
                    if (term == null)
                        break;

                    if (startTerm != null && startIdx == -1 && term.compareTo(startTerm) >= 0)
                        startIdx = idx;

                    if (endTerm != null && term.compareTo(endTerm) > 0)
                    {
                        endIdx = idx - 1;
                        break;
                    }
                }
            }
            return Pair.create(startIdx, endIdx);
        }

        /**
         * Search the trie for min and max posting block ids
         * @param start Start term inclusive
         * @param end End term inclusive
         * @return Min and max posting block ids
         * @throws IOException
         */
        public Pair<Integer, Integer> searchTermsIndex(final BytesRef start, final BytesRef end) throws IOException
        {
            ByteComparable startComp = start != null ? fixedLength(start) : null;
            ByteComparable endComp = end != null ? fixedLength(end) : null;

            int minBlockOrdinal = 0, maxBlockOrdinal = this.meta.numPostingBlocks - 1;

            if (start != null)
            {
                try (final TrieRangeIterator reader = new TrieRangeIterator(termsIndexHandle.instantiateRebufferer(),
                                                                            meta.termsIndexFP,
                                                                            startComp,
                                                                            null,
                                                                            true,
                                                                            true))
                {
                    final Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
                    if (iterator.hasNext())
                    {
                        final Pair<ByteSource, Long> pair = iterator.next();
                        final long payloadValue = pair.right.longValue();
                        minBlockOrdinal = (int) (payloadValue >> 32);
                    }
                    else
                        minBlockOrdinal = this.meta.numPostingBlocks;
                }
            }

            if (end != null)
            {
                try (final TrieRangeIterator reader = new TrieRangeIterator(termsIndexHandle.instantiateRebufferer(),
                                                                            meta.termsIndexFP,
                                                                            endComp,
                                                                            null,
                                                                            true,
                                                                            true))
                {
                    final Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();
                    if (iterator.hasNext())
                    {
                        final Pair<ByteSource, Long> pair = iterator.next();

                        final long payloadValue = pair.right.longValue();
                        final int minBlock = (int) (payloadValue >> 32);
                        final int maxBlock = (int) payloadValue;

                        // negative max block means all same values

                        final byte[] bytes = ByteSourceInverse.readBytes(pair.left);
                        if (ByteComparable.compare(ByteComparable.fixedLength(bytes), endComp, ByteComparable.Version.OSS41) > 0)
                        {
                            // if the term found is greater than what we're looking for, use the previous leaf
                            maxBlockOrdinal = minBlock - 1;
                        }
                        else
                            maxBlockOrdinal = maxBlock;
                    }
                }
            }
            return Pair.create(minBlockOrdinal, maxBlockOrdinal);
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
            private long pointId;

            public BytesCursor()
            {
                this.currentTerm = new BytesRef(meta.maxTermLength);
                this.termsData = IndexInputReader.create(termsDataHandle);
            }

            @Override
            public void close() throws IOException
            {
                termsData.close();
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
                    termsData.seek(blockAddress + meta.termsDataFp);
                    pointId = (blockIndex << TERMS_DICT_BLOCK_SHIFT) - 1;
                    while (pointId < target)
                    {
                        boolean advanced = next();
                        assert advanced : "unexpected eof";   // must return true because target is in range
                    }
                }
                return currentTerm;
            }

            private boolean next() throws IOException
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

                assert prefixLength + suffixLength <= meta.maxTermLength : "prefixLength="+prefixLength+" suffixLength="+suffixLength;
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
        private final BytesRefArray minBlockTerms = new BytesRefArray(Counter.newCounter()); // per posting block min terms
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
        private final int[] orderMapBuffer;
        private final boolean segmented;
        private final long orderMapStartFP, termsStartFP, termsIndexStartFP;
        private int maxTermLength = 0;
        private int postingsBlockCount = 0;
        private long pointId = 0; // point count
        private long distinctTermCount = 0;
        private CachedBlock currentPostingsBlock = new CachedBlock(), previousPostingsBlock = new CachedBlock();
        private LongBitSet postingsBlockSameTerms = new LongBitSet(DEFAULT_POSTINGS_BLOCK_SIZE); // postings blocks where all terms are the same
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
            this.termsOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_TERMS_DATA, context, true, segmented);
            this.termsStartFP = this.termsOut.getFilePointer();
            this.termsIndexOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_TERMS_INDEX, context, true, segmented);
            this.termsIndexStartFP = this.termsIndexOut.getFilePointer();
            this.orderMapOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_ORDERMAP, context, true, segmented);
            this.orderMapStartFP = this.orderMapOut.getFilePointer();
            this.postingsOut = indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_POSTINGS, context, true, segmented);
            this.postingsWriter = new PostingsWriter(postingsOut, postingsBlockSize);
            this.orderMapBuffer = new int[postingsBlockSize];
            this.rowIDIndices = new RowIDIndex[postingsBlockSize];
            for (int x = 0; x < postingsBlockSize; x++)
            {
                this.rowIDIndices[x] = new RowIDIndex();
            }
        }

        /**
         * Writes merged segments.
         */
        public SegmentMetadata.ComponentMetadataMap writeAll(final MergePointsIterators iterator) throws IOException
        {
            while (iterator.next())
            {
                final BytesRef term = iterator.term();
                final long rowid = iterator.rowId();
                add(term, rowid);
            }

            final SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
            this.meta = finish(components);
            return components;
        }

        /**
         * Called by memtable index writer.
         */
        public SegmentMetadata.ComponentMetadataMap writeAll(final TermsIterator terms) throws IOException
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
                        add(term, rowid);
                    }
                }
            }

            SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
            this.meta = finish(components);
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
        public Meta finish(final SegmentMetadata.ComponentMetadataMap components) throws IOException
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
            for (int x = blockPostingsFPs.size() - 2; x >= 0; x--)
            {
                if (blockPostingsFPs.getLong(x) == -1)
                    blockPostingsFPs.setLong(x, blockPostingsFPs.getLong(x + 1));
            }

            // write the block min terms trie index
            final IncrementalTrieWriter termsIndexWriter = new IncrementalDeepTrieWriterPageAware<>(trieSerializer, termsIndexOut.asSequentialWriter());
            int start = 0;
            int blockIdx = 0;

            // write distinct min block terms and the min and max block id's encoded as a long
            final BytesRefBuilder lastTerm = new BytesRefBuilder();
            for (blockIdx = 0; blockIdx < minBlockTerms.size(); blockIdx++)
            {
                final BytesRef minValue = minBlockTerms.get(new BytesRefBuilder(), blockIdx);
                if (blockIdx > 0)
                {
                    final BytesRef prevMinValue = minBlockTerms.get(new BytesRefBuilder(),blockIdx - 1);
                    if (!minValue.equals(prevMinValue))
                    {
                        final int startBlock = start;
                        int endBlock = blockIdx - 1;

                        // same terms block signed negative
                        // TODO: maybe use bitshift instead to save payload space
                        if (postingsBlockSameTerms.get(endBlock))
                        {
                            endBlock = endBlock * -1;
                        }

                        long encodedLong = (((long)startBlock) << 32) | (endBlock & 0xffffffffL);
                        // TODO: when the start and end block's are the same encode a single int
                        termsIndexWriter.add(fixedLength(prevMinValue), new Long(encodedLong));
                        lastTerm.clear();
                        lastTerm.append(prevMinValue);
                        start = blockIdx;
                    }
                }
                if (blockIdx == minBlockTerms.size() - 1 && blockIdx > 0)
                {
                    int endBlock = blockIdx;

                    if (postingsBlockSameTerms.get(endBlock))
                    {
                        endBlock = endBlock * -1;
                    }

                    final long encodedLong = (((long)start) << 32) | (endBlock & 0xffffffffL);

                    if (!minValue.equals(lastTerm.get()))
                    {
                        assert minValue.compareTo(lastTerm.get()) > 0;

                        termsIndexWriter.add(fixedLength(minValue), new Long(encodedLong));
                        lastTerm.clear();
                        lastTerm.append(minValue);
                    }
                }
            }

            final long termsIndexFP = termsIndexWriter.complete();
            termsIndexOut.close();

            final long postingsOffset = postingsWriter.getStartOffset();

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
                                                                                                     true,
                                                                                                     MONOTONIC_BLOCK_SIZE))
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
            final FileValidation.Meta termsCRC = createCRCMeta(termsStartFP, IndexComponent.BLOCK_TERMS_DATA);
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
                            BytesRef.deepCopyOf(minTerm.get()).bytes,
                            BytesRef.deepCopyOf(prevTerm.get()).bytes,
                            termsIndexCRC,
                            termsCRC,
                            orderMapCRC,
                            postingsCRC,
                            bitpackedCRC,
                            minRowId,
                            maxRowId);

            final long postingsLength = postingsWriter.getFilePointer() - postingsOffset;
            final long termsLength = termsOut.getFilePointer() - termsStartFP;
            final long termsIndexLength = termsIndexOut.getFilePointer() - termsIndexStartFP;

            // Postings list file pointers are stored directly in TERMS_DATA, so a root is not needed.
            components.put(IndexComponent.BLOCK_TERMS_DATA, termsStartFP, termsStartFP, termsLength, meta.stringMap());
            components.put(IndexComponent.POSTING_LISTS, -1, postingsOffset, postingsLength, meta.stringMap());
            components.put(IndexComponent.BLOCK_TERMS_INDEX, meta.termsIndexFP, termsIndexStartFP, termsIndexLength, meta.stringMap());
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

        public BytesRef minBlockTerm(int block)
        {
            return minBlockTerms.get(new BytesRefBuilder(), block);
        }

        public int blockCount()
        {
            return postingsBlockCount;
        }

        public boolean blockSameValue(int block)
        {
            return postingsBlockSameTerms.get(block);
        }

        public void add(ByteComparable term, long rowid) throws IOException
        {
            copyBytes(term, spare);
            add(spare.get(), rowid);
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

        /**
         * Cached block is indexed into, flushed, and swapped with a 2nd cached block on full.
         * Adds a term and row id to the current block.
         *
         * @param term Bytes ascending order
         * @param rowid Row id in secondary ascending order
         * @throws IOException
         */
        public void add(BytesRef term, long rowid) throws IOException
        {
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

            if (!prevTerm.get().equals(term))
                distinctTermCount++;
            else if (rowid < lastRowID)
                throw new IllegalStateException("rowid=" + rowid + "less than lastRowID=" + lastRowID);

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
        }

        /**
         * Write a cached block's postings and order map if necessary.
         *
         * Order map is optional, the order map file pointer may be -1.
         */
        protected void writePostingsAndOrderMap(CachedBlock buffer) throws IOException
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
            if (!currentPostingsBlock.isEmpty() && currentPostingsBlock.sameTerms())
            {
                this.postingsBlockSameTerms = LongBitSet.ensureCapacity(this.postingsBlockSameTerms, postingsBlockCount + 1);
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

            if (!currentPostingsBlock.isEmpty() && currentPostingsBlock.sameTerms())
            {
                postingsBlockSameTerms = LongBitSet.ensureCapacity(postingsBlockSameTerms, postingsBlockCount + 1);
                postingsBlockSameTerms.set(postingsBlockCount);
            }

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
                writePostingsAndOrderMap(previousPostingsBlock);

                // if the previous buffer is all same terms as the current buffer
                // then the postings writer is kept open
                if (!currentPostingsBlock.isEmpty()
                    && currentPostingsBlock.sameTerms()
                    && previousPostingsBlock.sameTerms()
                    && previousPostingsBlock.minTerm().equals(currentPostingsBlock.minTerm()))
                {
                    // -1 demarcs a same terms block to be backfilled later with a real posting file pointer
                    blockPostingsFPs.addLong(-1);
                }
                else
                {
                    final long postingsFP = postingsWriter.finishPostings();
                    blockPostingsFPs.addLong(postingsFP);
                }
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
            private int blockId;
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
     * Arithmetic byte increment to a ByteComparable.
     */
    public static ByteComparable nudge(ByteComparable value, int nudgeAt)
    {
        return version -> new ByteSource()
        {
            private final ByteSource v = value.asComparableBytes(version);
            private int cur = 0;

            @Override
            public int next()
            {
                int b = ByteSource.END_OF_STREAM;
                if (cur <= nudgeAt)
                {
                    b = v.next();
                    if (cur == nudgeAt)
                    {
                        if (b < 255)
                            ++b;
                        else
                            return b;  // can't nudge here, increase next instead (eventually will be -1)
                    }
                }
                ++cur;
                return b;
            }
        };
    }

    /**
     * Arithmetic byte decrement to a ByteComparable.
     */
    public static ByteComparable nudgeReverse(ByteComparable value, int nudgeAt)
    {
        return version -> new ByteSource()
        {
            private final ByteSource v = value.asComparableBytes(version);
            private int cur = 0;

            @Override
            public int next()
            {
                int b = ByteSource.END_OF_STREAM;
                if (cur <= nudgeAt)
                {
                    b = v.next();
                    if (cur == nudgeAt)
                    {
                        if (b > 0)
                            --b;
                        else
                            return ByteSource.END_OF_STREAM;  // can't nudge here, increase next instead (eventually will be -1)
                    }
                }
                ++cur;
                return b;
            }
        };
    }
}
