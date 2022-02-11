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
import java.util.HashMap;
import java.util.Map;

import com.google.common.primitives.UnsignedBytes;

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FutureArrays;

/**
 * Block terms is based on the version 1 index format kdtree with some major improvements.
 * <p>
 * Differences with the version 1 kdtree:
 * <p>
 * The binary tree index is read from disk rather than kept in heap as the kdtree does.
 * <p>
 * Variable bytes length binary tree index whereas the kdtree is fixed length bytes.
 * This obviates the need for {@link org.apache.cassandra.index.sai.utils.TypeUtil} truncating
 * big integer, big decimal, and enables range queries on strings.
 * <p>
 * When writing upper level postings, leaf postings are read from disk instead of kept in heap.
 * <p>
 * The random prefix bytes implementation is from SAI sorted terms/Lucene 8.x doc values.
 * <p>
 * The order map is in a separate file rather than inline with prefix encoded bytes.
 * <p>
 * Upper level postings are not written for segments for compaction.  Upper level postings
 * are only written for the final single merged segment.
 * Block terms + postings disk index data structure.
 * <p>
 * Raw bytes are prefix encoded in blocks of 16 for fast(er) random access than the posting block default size of 1024.
 * <p>
 * Block terms reader uses no significant heap space, all major data structures are disk based.
 * <p>
 * A point is defined as a term + rowid monotonic id.
 * <p>
 * The default postings block size is 1024.
 * <p>
 * All major data structures are on disk thereby enabling many indexes.
 * <p>
 * File validation is done via a CRC check.
 * <p>
 * kdtree and trie metrics are combined and reused.
 * <p>
 * Files:
 * <p>
 * BLOCK_BITPACKED - Bit packed maps of block id -> bytes block file pointer, and block id -> order map file pointer.
 *
 * BLOCK_TERMS_DATA - Raw bytes prefix encoded in blocks of 16 {@see SortedTermsReader}
 *
 * BLOCK_TERMS_INDEX - Variable byte length kdtree index on-disk implementation.  Based on Lucene 8.x.
 *
 * BLOCK_POSTINGS - Posting lists per 1024 sized block.
 *
 * BLOCK_UPPER_POSTINGS - Non-block level posting lists for greater than level 0 posting lists.
 *
 * BLOCK_ORDERMAP - Map of posting ordinals to the actual block ordinals.  Exists because posting lists
 *                  are in row id order.
 *
 */
// TODO: prefix compress the binary tree index in a jira that's not 770
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
        public final long upperPostingsIndexFP;
        public final byte[] minTerm;
        public final byte[] maxTerm;
        public final FileValidation.Meta termsIndexCRC,
        termsCRC,
        orderMapCRC,
        postingsCRC,
        bitpackedCRC,
        upperPostingsCRC;
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
                    long upperPostingsIndexFP,
                    byte[] minTerm,
                    byte[] maxTerm,
                    FileValidation.Meta termsIndexCRC,
                    FileValidation.Meta termsCRC,
                    FileValidation.Meta orderMapCRC,
                    FileValidation.Meta postingsCRC,
                    FileValidation.Meta bitpackedCRC,
                    FileValidation.Meta upperPostingsCRC,
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
            this.upperPostingsIndexFP = upperPostingsIndexFP;
            this.minTerm = minTerm;
            this.maxTerm = maxTerm;
            this.termsIndexCRC = termsIndexCRC;
            this.termsCRC = termsCRC;
            this.orderMapCRC = orderMapCRC;
            this.postingsCRC = postingsCRC;
            this.bitpackedCRC = bitpackedCRC;
            this.upperPostingsCRC = upperPostingsCRC;
            this.minRowid = minRowid;
            this.maxRowid = maxRowid;
        }
    }

    /**
     * Compare the bytes difference, return 0 if there is none.
     */
    public static int bytesDifference(BytesRef priorTerm, BytesRef currentTerm)
    {
        int mismatch = FutureArrays.mismatch(priorTerm.bytes, priorTerm.offset, priorTerm.offset + priorTerm.length, currentTerm.bytes, currentTerm.offset, currentTerm.offset + currentTerm.length);
        return Math.max(0, mismatch);
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
     * @param orig Prefix bytes
     * @param maxLength The correct ordering the absolute max bytes length of all compared bytes is specified.
     * @return Byte array that is the max of the given prefix.
     */
    public static byte[] prefixMaxTerm(byte[] orig, int maxLength)
    {
        assert orig.length > 0 : "Input byte array must be not empty";

        byte[] result = Arrays.copyOf(orig, maxLength);
        Arrays.fill(result, orig.length, result.length, (byte) 255);

        return result;
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

        // unused
        @Override
        public long findTokenRowID(long targetToken)
        {
            throw new UnsupportedOperationException();
        }
    }
}
