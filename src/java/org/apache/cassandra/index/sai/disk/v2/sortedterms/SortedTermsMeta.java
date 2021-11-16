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

package org.apache.cassandra.index.sai.disk.v2.sortedterms;

import java.io.IOException;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Metadata produced by {@link SortedTermsWriter}, needed by {@link SortedTermsReader}.
 */
public class SortedTermsMeta
{
    public final long trieFP;
    /** Number of terms */
    public final long count;
    public final int maxTermLength;
    public final byte[] offsetMetaBytes;
    public final long offsetBlockCount;

    public SortedTermsMeta(IndexInput input) throws IOException
    {
        this.trieFP = input.readLong();
        this.count = input.readLong();
        this.maxTermLength = input.readInt();
        int length = input.readVInt();
        byte[] bytes = new byte[length];
        input.readBytes(bytes, 0, length);
        this.offsetMetaBytes = bytes;
        this.offsetBlockCount = input.readLong();
    }

    public SortedTermsMeta(long trieFP, long count, int maxTermLength, byte[] offsetMetaBytes, long offsetBlockCount)
    {
        this.trieFP = trieFP;
        this.count = count;
        this.maxTermLength = maxTermLength;
        this.offsetMetaBytes = offsetMetaBytes;
        this.offsetBlockCount = offsetBlockCount;
    }

    public void write(IndexOutput output) throws IOException
    {
        output.writeLong(trieFP);
        output.writeLong(count);
        output.writeInt(maxTermLength);
        output.writeVInt(offsetMetaBytes.length);
        output.writeBytes(offsetMetaBytes, 0, offsetMetaBytes.length);
        output.writeLong(offsetBlockCount);
    }
}
