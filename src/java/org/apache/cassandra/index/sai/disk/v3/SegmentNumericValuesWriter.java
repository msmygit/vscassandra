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

import java.io.Closeable;
import java.io.IOException;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.bitpack.AbstractBlockPackedWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.lucene.store.IndexOutput;

/**
 * Writes monotonic or regular bitpacked long values to a segment.
 */
public class SegmentNumericValuesWriter implements Closeable
{
    public static final int MONOTONIC_BLOCK_SIZE = 16384;
    public static final int BLOCK_SIZE = 128;

    private final IndexOutput output;
    private final AbstractBlockPackedWriter writer;
    private final SegmentMetadata.ComponentMetadataMap components;
    private final IndexComponent component;
    private final int blockSize;
    private final long startFP;
    private long count = 0;

    public SegmentNumericValuesWriter(IndexComponent component,
                                      IndexOutput indexOutput,
                                      SegmentMetadata.ComponentMetadataMap components,
                                      boolean monotonic,
                                      int blockSize) throws IOException
    {
        this.startFP = indexOutput.getFilePointer();
        this.component = component;
        SAICodecUtils.writeHeader(indexOutput);
        this.writer = monotonic ? new MonotonicBlockPackedWriter(indexOutput, blockSize)
                                : new BlockPackedWriter(indexOutput, blockSize);
        this.output = indexOutput;
        this.components = components;
        this.blockSize = blockSize;
    }

    @Override
    public void close() throws IOException
    {
        final long fp = writer.finish();
        SAICodecUtils.writeFooter(output);

        long length = output.getFilePointer() - startFP;

        NumericValuesMeta meta = new NumericValuesMeta(count, blockSize, fp);
        components.put(component, -1, fp, length, meta.stringMap());
    }

    public void add(long value) throws IOException
    {
        writer.add(value);
        count++;
    }
}
