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

import org.apache.cassandra.index.sai.disk.v1.DirectReaders;
import org.apache.cassandra.index.sai.disk.v1.LeafOrderMap;
import org.apache.cassandra.index.sai.utils.SeekingRandomAccessInput;
import org.apache.cassandra.index.sai.utils.SharedIndexInput2;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Read a block of 32 first term prefix encoded lexicographic ordered bytes.
 */
public class PrefixBytesReader implements Closeable
{
    private final SharedIndexInput2 input;
    private final SeekingRandomAccessInput lengthsSeeker;
    private final BytesRef bytesRef = new BytesRef();
    private final BytesRef firstTerm = new BytesRef();

    private DirectReaders.Reader prefixLengthsReader, suffixLengthsReader;
    private long prefixLengthsFP, suffixLengthsFP;
    private int idx = 0;
    private byte count;

    private long suffixBytesFP = -1;

    public int count()
    {
        return count;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(lengthsSeeker, input);
    }

    public PrefixBytesReader(SharedIndexInput2 input) throws IOException
    {
        this.input = input;
        this.lengthsSeeker = new SeekingRandomAccessInput(input);
    }

    public void reset(long fp) throws IOException
    {
        input.seek(fp);

        this.count = input.readByte();

        final short prefixLengthsSize = input.readShort();
        final short suffixPositionsSize = input.readShort();

        final long rawFP = input.getFilePointer();

        final byte prefixLengthsBits = input.readByte();
        this.prefixLengthsFP = input.getFilePointer();
        this.prefixLengthsReader = DirectReaders.getReaderForBitsPerValue(prefixLengthsBits);

        // seek to the suffix lengths
        input.seek(rawFP + prefixLengthsSize);
        final byte suffixLengthsBits = input.readByte();
        this.suffixLengthsFP = input.getFilePointer();
        this.suffixLengthsReader = DirectReaders.getReaderForBitsPerValue(suffixLengthsBits);

        suffixBytesFP = rawFP + prefixLengthsSize + suffixPositionsSize;

        this.idx = 0;

        // read the first term
        final int secondPosition = LeafOrderMap.getValue(lengthsSeeker, suffixLengthsFP, 1, suffixLengthsReader);
        input.seek(suffixBytesFP);
        firstTerm.offset = 0;
        firstTerm.bytes = ArrayUtil.grow(firstTerm.bytes, secondPosition);
        firstTerm.length = secondPosition;

        input.readBytes(firstTerm.bytes, 0, secondPosition);
    }

    public int getOrdinal()
    {
        return idx;
    }

    public BytesRef current()
    {
        return bytesRef;
    }

    public BytesRef seek(int targetIndex) throws IOException
    {
        // read the suffix bytes of the targetIndex
        final int suffixPosition = LeafOrderMap.getValue(lengthsSeeker, suffixLengthsFP, targetIndex, suffixLengthsReader);
        final int suffixPosition2 = LeafOrderMap.getValue(lengthsSeeker, suffixLengthsFP, targetIndex + 1, suffixLengthsReader);
        final int prefixLength = LeafOrderMap.getValue(lengthsSeeker, prefixLengthsFP, targetIndex, prefixLengthsReader);

        int suffixLength = suffixPosition2 - suffixPosition;

        bytesRef.bytes = ArrayUtil.grow(bytesRef.bytes, suffixLength + prefixLength);

        System.arraycopy(firstTerm.bytes, 0, bytesRef.bytes, 0, prefixLength);

        input.seek(suffixBytesFP + suffixPosition);
        input.readBytes(bytesRef.bytes, prefixLength, suffixLength);

        bytesRef.length = prefixLength + suffixLength;

        return bytesRef;
    }
}
