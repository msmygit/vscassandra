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

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.PostingList;

import static org.apache.cassandra.index.sai.disk.v2.LuceneSkipWriter.MAX_SKIP_LEVELS;

@NotThreadSafe
public class LucenePostingsReader implements PostingList
{
    protected final Lucene8xIndexInput input;
    private final int blockSize;
    private final long numPostings;
    private final ForDeltaUtil forDeltaUtil;
    private final long[] docBuffer;

    private LuceneSkipReader skipReader;
    private int docBufferUpto = 0;
    private long nextSkipDoc;
    private long doc;
    private long accum;
    private boolean skipped;
    private long blockUpto;
    private final long docTermStartFP;
    private final int skipOffset;
    private final long skipFP;

    @VisibleForTesting
    public LucenePostingsReader(Lucene8xIndexInput input,
                                int blockSize,
                                long docTermStartFP) throws IOException
    {
        this.input = input;
        this.blockSize = blockSize;
        this.docTermStartFP = docTermStartFP;

        this.docBuffer = new long[blockSize + 1];

        input.seek(docTermStartFP);
        this.numPostings = input.readVLong();
        this.skipOffset = input.readVInt();

        skipFP = input.getFilePointer();

        // start of the FoR delta encoded blocks
        input.seek(docTermStartFP - skipOffset);

        final ForUtil forUtil = new ForUtil();
        this.forDeltaUtil = new ForDeltaUtil(forUtil);

        docBufferUpto = blockSize;
    }

    @Override
    public long nextPosting() throws IOException
    {
        //final long left = numPostings - blockUpto;

        if (docBufferUpto == blockSize)
        {
            refillDocs();
        }

        doc = docBuffer[docBufferUpto];
        docBufferUpto++;
        return doc;
    }

    private void refillDocs() throws IOException
    {
        final long left = numPostings - blockUpto;
        assert left >= 0;

        if (left == 0)
        {
            docBufferUpto = 0;
            docBuffer[0] = PostingList.END_OF_STREAM;
            return;
        }

        System.out.println("refillDocs left="+left);

        forDeltaUtil.decodeAndPrefixSum(input, accum, docBuffer);

        if (left < blockSize)
        {
            blockUpto += left;
            docBuffer[(int) left] = PostingList.END_OF_STREAM;
        }
        else
        {
            blockUpto += blockSize;
        }
        accum = docBuffer[blockSize - 1];
        docBufferUpto = 0;
    }

    @Override
    public long advance(long target) throws IOException
    {
        // current skip docID < docIDs generated from current buffer <= next skip docID
        // we don't need to skip if target is buffered already
        if (numPostings > blockSize && target > nextSkipDoc)
        {
            if (skipReader == null)
            {
                // Lazy init: first time this enum has ever been used for skipping
                skipReader = new LuceneSkipReader(input.clone(), MAX_SKIP_LEVELS, blockSize);
            }

            if (!skipped)
            {
                assert skipOffset != -1;
                // This is the first time this enum has skipped
                // since reset() was called; load the skip data:
                skipReader.init(skipFP, numPostings);
                skipped = true;
            }

            // always plus one to fix the result, since skip position in Lucene84SkipReader
            // is a little different from MultiLevelSkipListReader
            final long newDocUpto = skipReader.skipTo(target) + 1;

            if (newDocUpto >= blockUpto)
            {
                // Skipper moved
                assert newDocUpto % blockSize == 0 : "got " + newDocUpto;
                blockUpto = newDocUpto;

                // Force to read next block
                docBufferUpto = blockSize;
                accum = skipReader.getDoc();               // actually, this is just lastSkipEntry
                input.seek(skipReader.getDocPointer());    // now point to the block we want to search
            }
            // next time we call advance, this is used to
            // foresee whether skipper is necessary.
            nextSkipDoc = skipReader.getNextSkipDoc();
        }
        if (docBufferUpto == blockSize)
        {
            refillDocs();
        }

        // Now scan:
        long doc;
        while (true)
        {
            doc = docBuffer[docBufferUpto];
            docBufferUpto++;
            //docUpto++;

            if (doc >= target)
            {
                break;
            }

            if (docBufferUpto == blockSize)
            {
                return this.doc = PostingList.END_OF_STREAM;
            }
        }
//        position =

//        // Now scan... this is an inlined/pared down version
//        // of nextDoc():
//        long doc;
//        while (true)
//        {
//            doc = docBuffer[docBufferUpto];
//
//            if (doc >= target)
//            {
//                break;
//            }
//            ++docBufferUpto;
//        }

        docBufferUpto++;
        return this.doc = doc;
    }

//    @Override
//    public int advance(int target) throws IOException {
//        if (target > nextSkipDoc) {
//            advanceShallow(target);
//        }
//        if (docBufferUpto == BLOCK_SIZE) {
//            if (seekTo >= 0) {
//                docIn.seek(seekTo);
//                seekTo = -1;
//                isFreqsRead = true; // reset isFreqsRead
//            }
//            refillDocs();
//        }
//
//        // Now scan:
//        long doc;
//        while (true) {
//            doc = docBuffer[docBufferUpto];
//            docBufferUpto++;
//            docUpto++;
//
//            if (doc >= target) {
//                break;
//            }
//
//            if (docBufferUpto == BLOCK_SIZE) {
//                return this.doc = NO_MORE_DOCS;
//            }
//        }
//        position = 0;
//        lastStartOffset = 0;
//
//        return this.doc = (int) doc;
//    }

    @Override
    public void close() throws IOException
    {
        try
        {
            input.close();
        }
        finally
        {
        }
    }

    @Override
    public long size()
    {
        return numPostings;
    }

    @VisibleForTesting
    int getBlockSize()
    {
        return blockSize;
    }
}
