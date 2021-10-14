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
package org.apache.cassandra.index.sai.disk.v2;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.disk.ByteSliceReader;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.RAMPostingSlices;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.Counter;

/**
 * Indexes strings into an on-heap inverted index to be flushed in an SSTable attached index later.
 * For flushing use the PostingTerms interface.
 */
public class RAMIndexer
{
    private final AbstractType<?> termComparator;
    private final BytesRefHash termsHash;
    private final RAMPostingSlices slices;
    private final Counter bytesUsed;
    private final BytesRefBuilder builder;

    // TODO This is pretty horrible
    public int rowCount = 0;

    private int[] lastSegmentRowID = new int[RAMPostingSlices.DEFAULT_TERM_DICT_SIZE];

    public RAMIndexer(AbstractType<?> termComparator)
    {
        this.termComparator = termComparator;
        bytesUsed = Counter.newCounter();

        ByteBlockPool termsPool = new ByteBlockPool(new ByteBlockPool.DirectTrackingAllocator(bytesUsed));

        termsHash = new BytesRefHash(termsPool);

        slices = new RAMPostingSlices(bytesUsed);
        builder = new BytesRefBuilder();
    }

    public long estimatedBytesUsed()
    {
        return bytesUsed.get();
    }

    /**
     * EXPENSIVE OPERATION due to sorting the terms, only call once.
     */
    // TODO: assert or throw and exception if getTermsWithPostings is called > 1
    public TermsIterator getTermsWithPostings()
    {
        final int[] sortedTermIDs = termsHash.sort();

        final int valueCount = termsHash.size();
        final ByteSliceReader sliceReader = new ByteSliceReader();

        return new TermsIterator()
        {
            private int ordUpto = 0;
            private final BytesRef br = new BytesRef();

            @Override
            public ByteBuffer getMinTerm()
            {
                BytesRef term = new BytesRef();
                int minTermID = sortedTermIDs[0];
                termsHash.get(minTermID, term);
                return ByteBuffer.wrap(term.bytes, term.offset, term.length);
            }

            @Override
            public ByteBuffer getMaxTerm()
            {
                BytesRef term = new BytesRef();
                int maxTermID = sortedTermIDs[valueCount-1];
                termsHash.get(maxTermID, term);
                return ByteBuffer.wrap(term.bytes, term.offset, term.length);
            }

            public void close() {}

            @Override
            public PostingList postings()
            {
                int termID = sortedTermIDs[ordUpto - 1];
                final int maxSegmentRowId = lastSegmentRowID[termID];
                return slices.postingList(termID, sliceReader, maxSegmentRowId);
            }

            @Override
            public boolean hasNext() {
                return ordUpto < valueCount;
            }

            @Override
            public ByteComparable next()
            {
                if (!hasNext())
                    throw new NoSuchElementException();

                termsHash.get(sortedTermIDs[ordUpto], br);
                ordUpto++;
                return asByteComparable(br.bytes, br.offset, br.length);
            }

            private ByteComparable asByteComparable(byte[] bytes, int offset, int length)
            {
                return v -> ByteSource.fixedLength(bytes, offset, length);
            }
        };
    }

    public long add(ByteBuffer term, int segmentRowId)
    {
        long startBytes = estimatedBytesUsed();
        ByteComparable byteComparable = v -> termComparator.asComparableBytes(term, v);
        builder.clear();
        BytesUtil.gatherBytes(byteComparable, builder);
        int termID = termsHash.add(builder.get());

        if (termID >= 0)
        {
            // firs time seeing this term, create the term's first slice !
            slices.createNewSlice(termID);
        }
        else
        {
            termID = (-termID) - 1;
        }

        if (termID >= lastSegmentRowID.length - 1)
        {
            lastSegmentRowID = ArrayUtil.grow(lastSegmentRowID, termID + 1);
        }

        int delta = segmentRowId - lastSegmentRowID[termID];

        lastSegmentRowID[termID] = segmentRowId;

        slices.writeVInt(termID, delta);

        long allocatedBytes = estimatedBytesUsed() - startBytes;

        rowCount++;

        return allocatedBytes;
    }
}
