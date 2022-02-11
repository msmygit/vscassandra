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

import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexOutput;

public class SegmentNumericValuesTest extends SaiRandomizedTest
{
    @Test
    public void test() throws Exception
    {
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);
        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();

        IndexDescriptor descriptor = newIndexDescriptor();

        try (IndexOutput out = descriptor.openPerIndexOutput(IndexComponent.BLOCK_TERMS_OFFSETS, indexContext);
             SegmentNumericValuesWriter writer = new SegmentNumericValuesWriter(IndexComponent.BLOCK_TERMS_OFFSETS,
                                                                                out,
                                                                                components,
                                                                                true,
                                                                                SegmentNumericValuesWriter.MONOTONIC_BLOCK_SIZE))
        {
            for (int x = 0; x < 200; x++)
            {
                writer.add(x);
            }
        }

        try (FileHandle file = descriptor.createPerIndexFileHandle(IndexComponent.BLOCK_TERMS_OFFSETS, indexContext))
        {
            final SegmentMetadata.ComponentMetadata blockPostingsOffsetsCompMeta = components.get(IndexComponent.BLOCK_TERMS_OFFSETS);
            NumericValuesMeta blockPostingsOffsetsMeta = new NumericValuesMeta(blockPostingsOffsetsCompMeta.attributes);
            try (LongArray postingBlockFPs = new MonotonicBlockPackedReader(file, blockPostingsOffsetsMeta).open())
            {
                for (int x = 0; x < 200; x++)
                {
                    assertEquals(x, postingBlockFPs.get(x));
                }
            }
        }
    }
}
