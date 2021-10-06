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
import java.util.Arrays;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.disk.v2.PrefixBytesWriter;
import org.apache.cassandra.index.sai.disk.v2.SimpleFoR;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

public class PrefixBlockWriter
{
    public static final int INDEX_INTERVAL = 32;
    final PrefixBytesWriter upperTermsWriter, lowerTermsWriter;
    final RAMIndexOutput lowerTermsBuffer = new RAMIndexOutput("");
    final RAMIndexOutput upperTermsBuffer = new RAMIndexOutput("");
    final RAMIndexOutput lowerBlockSizesBuffer = new RAMIndexOutput("");
    final IntArrayList lowerBlockSizes = new IntArrayList();
    private int count = 0;

    public PrefixBlockWriter()
    {
        upperTermsWriter = new PrefixBytesWriter();
        lowerTermsWriter = new PrefixBytesWriter();
    }

    public long finish(IndexOutput output) throws IOException
    {
        final long startFP = output.getFilePointer();

        // write the upper bytes
        // write the lower block FPs
        int upperCount = upperTermsWriter.count();

        assert upperCount <= INDEX_INTERVAL;

        upperTermsWriter.finish(upperTermsBuffer);

        // flush the lower terms if there are any
        if (lowerTermsWriter.count() > 0)
        {
            final int size = (int)lowerTermsWriter.finish(lowerTermsBuffer);

            lowerBlockSizes.add(size);
        }

        SimpleFoR.write(lowerBlockSizes.toArray(), lowerBlockSizesBuffer);

        final long fp = output.getFilePointer();

        byte lastBlockCount = (byte)(count % INDEX_INTERVAL);
        if (lastBlockCount == 0)
        {
            upperCount++;
        }

        output.writeByte((byte)upperCount);
        output.writeByte(lastBlockCount);
        output.writeShort((short)upperTermsBuffer.getFilePointer()); // upper terms size to skip over
        output.writeShort((short)lowerBlockSizesBuffer.getFilePointer());

        upperTermsBuffer.writeTo(output);
        lowerBlockSizesBuffer.writeTo(output);
        lowerTermsBuffer.writeTo(output);

        long totalBytes = output.getFilePointer() - startFP;

        return fp;
    }

    public void add(BytesRef term) throws IOException
    {
        if (count % INDEX_INTERVAL == 0)
        {
            upperTermsWriter.add(term);

            // if there are lower terms flush
            if (lowerTermsWriter.count() > 0)
            {
                final int size = (int)lowerTermsWriter.finish(lowerTermsBuffer);

                lowerBlockSizes.add(size);
            }
            lowerTermsWriter.reset();
        }
        lowerTermsWriter.add(term);
        count++;
    }
}
