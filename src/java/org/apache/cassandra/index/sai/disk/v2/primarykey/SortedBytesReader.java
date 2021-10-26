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

package org.apache.cassandra.index.sai.disk.v2.primarykey;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil;
import org.apache.cassandra.index.sai.disk.v2.blockindex.TrieRangeIterator;
import org.apache.cassandra.index.sai.utils.SharedIndexInput2;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.BlockPackedReader;
import org.apache.lucene.util.packed.PackedInts;

import static org.apache.cassandra.index.sai.disk.v2.primarykey.SortedBytesWriter.BLOCK_SIZE;

/**
 * Reads a value based on a point id from an underlying set of bytes blocks.
 */
public class SortedBytesReader implements Closeable
{
    final FileHandle minTermsFile;
    final IndexInput blockFPInput;
    final SharedIndexInput2 bytesInput;
    final BlockPackedReader blockFPReader;
    final Meta meta;
    final BlockBytesReader blockBytesReader;
    private long currentBlock = -1;

    public SortedBytesReader(FileHandle minTermsFile,
                             IndexInput bytesInput,
                             IndexInput blockFPInput,
                             Meta meta) throws IOException
    {
        this.minTermsFile = minTermsFile;
        this.bytesInput = new SharedIndexInput2(bytesInput);
        this.blockBytesReader = new BlockBytesReader(this.bytesInput);
        this.blockFPInput = blockFPInput;
        this.meta = meta;
        this.blockFPReader = new BlockPackedReader(blockFPInput, PackedInts.VERSION_CURRENT, 128, meta.numBlocks, true);
    }

    /**
     * Looks up a term by point id
     */
    public BytesRef seekTo(long pointID) throws IOException
    {
        final long blockId = pointID / BLOCK_SIZE;
        final int idx = (int) (pointID % BLOCK_SIZE);

        if (blockId != currentBlock)
        {
            final long blockFP = blockFPReader.get(blockId);
            this.blockBytesReader.reset(blockFP);
            this.currentBlock = blockId;
        }
        return this.blockBytesReader.seek(idx);
    }

    /**
     * Looks up a point id by term
     */
    public long seekTo(BytesRef target) throws IOException
    {
        int block = -1;
        try (TrieRangeIterator reader = new TrieRangeIterator(minTermsFile.instantiateRebufferer(),
                                                              meta.minTermsFP,
                                                              BytesUtil.fixedLength(target),
                                                              null,
                                                              true,
                                                              true))
        {
            final Iterator<Pair<ByteSource, Long>> iterator = reader.iterator();

            if (iterator.hasNext())
            {
                final Pair<ByteSource, Long> pair = iterator.next();
                block = pair.right.intValue();
            }
            else
            {
                block = (int) meta.numBlocks - 1;
            }
        }

        // negate the block id as the target may be in the previous block
        final long useBlockId = block > 0 ? block - 1 : 0;

        long pointId = useBlockId * BLOCK_SIZE;
        while (true)
        {
            BytesRef term = seekTo(pointId);
            if (term.compareTo(target) >= 0)
            {
                break;
            }
            pointId++;
        }
        return pointId;
    }

    @Override
    public void close() throws IOException
    {
        this.blockBytesReader.close();
        this.bytesInput.close();
    }

    public static class Meta
    {
        public final long count;
        public final long numBlocks;
        public final long minTermsFP;

        public Meta(long count, long numBlocks, long minTermsFP)
        {
            this.count = count;
            this.numBlocks = numBlocks;
            this.minTermsFP = minTermsFP;
        }

        @Override
        public String toString()
        {
            return "Meta{" +
                   "count=" + count +
                   ", numBlocks=" + numBlocks +
                   ", minTermsFP=" + minTermsFP +
                   '}';
        }
    }
}
