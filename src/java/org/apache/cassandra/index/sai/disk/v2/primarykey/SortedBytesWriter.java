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

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil;
import org.apache.cassandra.io.tries.IncrementalDeepTrieWriterPageAware;
import org.apache.cassandra.io.tries.IncrementalTrieWriter;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.packed.BlockPackedWriter;

import static org.apache.cassandra.index.sai.disk.v1.TrieTermsDictionaryReader.trieSerializer;

/**
 * Writes bytes in blocks and a trie with the min block terms.
 */
public class SortedBytesWriter
{
    public static final int BLOCK_SIZE = 1024;
    private final IndexOutput bytesOutput;
    private final IndexOutputWriter minKeyWriter;
    private final IncrementalTrieWriter termsIndexWriter;
    private int block = 0;
    private int blockCount = 0;
    private int count = 0;
    private final BlockBytesWriter bytesWriter = new BlockBytesWriter();
    private final BlockPackedWriter blockFPWriter;
    private final BytesRefBuilder lastTerm = new BytesRefBuilder();

    public SortedBytesWriter(IndexOutputWriter minKeyWriter,
                             IndexOutput bytesOutput,
                             IndexOutput blockFPOutput)
    {
        this.minKeyWriter = minKeyWriter;
        this.bytesOutput = bytesOutput;
        termsIndexWriter = new IncrementalDeepTrieWriterPageAware<>(trieSerializer, minKeyWriter.asSequentialWriter());
        blockFPWriter = new BlockPackedWriter(blockFPOutput, 128);
    }

    public void add(BytesRef term) throws IOException
    {
        // check lexicographic order
        if (lastTerm.length() > 0 && lastTerm.get().compareTo(term) > 0)
        {
            throw new IllegalArgumentException("terms must be in lexicographic order");
        }

        if (blockCount == BLOCK_SIZE)
        {
            // block is completed, write the block
            flushBlock();
            blockCount = 0;
        }
        if (blockCount == 0)
        {
            // new block
            // min block term
            termsIndexWriter.add(BytesUtil.fixedLength(term), new Long(block));
        }
        // add bytes
        bytesWriter.add(term);

        lastTerm.copyBytes(term);

        blockCount++;
        count++;
    }

    public SortedBytesReader.Meta finish() throws IOException
    {
        if (blockCount > 0)
        {
            flushBlock();
        }
        final long minTermsTrieFP = termsIndexWriter.complete();

        // finish the data structures being written
        blockFPWriter.finish();
        termsIndexWriter.close();
        return new SortedBytesReader.Meta(count, block, minTermsTrieFP);
    }

    private void flushBlock() throws IOException
    {
        // block file pointer
        final long blockFP = bytesOutput.getFilePointer();
        blockFPWriter.add(blockFP);
        // write the block
        bytesWriter.finish(bytesOutput);
        block++;
    }
}
