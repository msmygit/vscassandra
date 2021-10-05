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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.disk.v2.PrefixBytesReader;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput2;
import org.apache.lucene.util.BytesRef;

import static org.apache.cassandra.index.sai.disk.v2.blockindex.PrefixBlockWriter.INDEX_INTERVAL;

public class PrefixBlockReader
{
    final SeekingRandomAccessInput seekInput;
    final DirectReaders.Reader reader;
    final long lowerBlockSizeDeltasFP;
    final PrefixBytesReader upperTermsReader;
    final SharedIndexInput input;
    final SharedIndexInput2 upperTermsInput, lowerTermsInput;
    final byte upperCount;
    final byte lastBlockCount;
    private int idx;
    private BytesRef upperTerm;
    private PrefixBytesReader lowerTermsReader;
    private long currentFP = -1;
    private final long lowerTermsStartFP;
    private long currentLowerTermsFP;

    public PrefixBlockReader(final long fp, final SharedIndexInput input) throws IOException
    {
        this.input = input;
        input.seek(fp);
        upperCount = input.readByte();
        lastBlockCount = input.readByte();
        final short upperTermsSize = input.readShort();
        final short lowerBlockSizeDeltasSize = input.readShort();

        final long fp2 = input.getFilePointer();

        System.out.println("PrefixBlockReader fp="+fp+" upperCount="+upperCount+" lastBlockCount="+lastBlockCount+" upperTermsSize="+upperTermsSize+" lowerBlockSizeDeltasSize="+lowerBlockSizeDeltasSize);

        upperTermsInput = new SharedIndexInput2(input.sharedCopy());
        upperTermsReader = new PrefixBytesReader(input.getFilePointer(), upperTermsInput);

        lowerTermsInput = new SharedIndexInput2(input.sharedCopy());

        input.seek(fp2 + upperTermsSize); // seek to the lowerBlockSizeDeltas
        final int bits = input.readByte();

        lowerBlockSizeDeltasFP = input.getFilePointer();
        seekInput = new SeekingRandomAccessInput(input);
        reader = DirectReaders.getReaderForBitsPerValue((byte)bits);

        lowerTermsStartFP = currentLowerTermsFP = fp2 + lowerBlockSizeDeltasSize + upperTermsSize;
    }

    public BytesRef next(final BytesRef target) throws IOException
    {
        final int upperIdx = idx / INDEX_INTERVAL;
        final int lowerIdx = idx % INDEX_INTERVAL;

        System.out.println("next upperIdx="+upperIdx+" upperCount="+upperCount+" lowerIdx="+lowerIdx+" lastBlockCount="+lastBlockCount);

        if (upperIdx == upperCount - 1 && lowerIdx == lastBlockCount)
        {
            return null;
        }

        if (idx % INDEX_INTERVAL == 0)
        {
            upperTerm = upperTermsReader.next();

            lowerTermsReader = new PrefixBytesReader(currentLowerTermsFP, lowerTermsInput);

            long lowerBlockSize = LeafOrderMap.getValue(seekInput, lowerBlockSizeDeltasFP, upperIdx, reader);
            currentLowerTermsFP += lowerBlockSize;
        }

        idx++;
        return lowerTermsReader.next();
    }
}
