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

import java.io.Closeable;
import java.io.IOException;

import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput2;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

public class PrefixBytesReader implements Closeable
{
    final SharedIndexInput2 input;
    final SeekingRandomAccessInput lengthsSeeker;
    private final BytesRef bytesRef = new BytesRef();

    DirectReaders.Reader prefixLengthsReader, suffixLengthsReader;
    long prefixLengthsFP, suffixLengthsFP;
    private int idx = 0;
    private byte count;
    private long currentFP = -1;

    public int count()
    {
        return count;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(lengthsSeeker, input);
    }

    public PrefixBytesReader(SharedIndexInput2 input) throws IOException
    {
        this.input = input;
        this.lengthsSeeker = new SeekingRandomAccessInput(input.sharedCopy());
    }

    public void reset(long fp) throws IOException
    {
        input.seek(fp);

        this.count = input.readByte();

        final short prefixLengthsSize = input.readShort();
        final short suffixLengthsSize = input.readShort();

        final long rawFP = input.getFilePointer();

        final byte prefixLengthsBits = input.readByte();
        this.prefixLengthsFP = input.getFilePointer();
        this.prefixLengthsReader = DirectReaders.getReaderForBitsPerValue(prefixLengthsBits);

        // seek to the suffix lengths
        input.seek(rawFP + prefixLengthsSize);
        final byte suffixLengthsBits = input.readByte();
        this.suffixLengthsFP = input.getFilePointer();
        this.suffixLengthsReader = DirectReaders.getReaderForBitsPerValue(suffixLengthsBits);

        final long suffixBytesFP = rawFP + prefixLengthsSize + suffixLengthsSize;
        this.currentFP = suffixBytesFP;
    }

    public int getOrdinal()
    {
        return idx;
    }

    public BytesRef current()
    {
        return bytesRef;
    }

    public BytesRef next() throws IOException
    {
        if (idx >= count)
        {
            if (idx == count)
            {
                idx++;
            }
            return null;
        }

        final int prefixLength = LeafOrderMap.getValue(lengthsSeeker, prefixLengthsFP, idx, prefixLengthsReader);
        final int suffixLength = LeafOrderMap.getValue(lengthsSeeker, suffixLengthsFP, idx, suffixLengthsReader);

        bytesRef.bytes = ArrayUtil.grow(bytesRef.bytes, suffixLength + prefixLength);

        // TODO: with SharedIndexInput2, tracking the file pointer may not be necessary
        input.seek(this.currentFP);
        input.readBytes(bytesRef.bytes, prefixLength, suffixLength);
        this.currentFP += suffixLength;

        bytesRef.length = prefixLength + suffixLength;
        idx++;
        return bytesRef;
    }
}
