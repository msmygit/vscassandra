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

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.bitpack.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;

public class MultiBitpackedTest extends SaiRandomizedTest
{
    @Test
    public void test() throws Exception
    {
        IndexDescriptor indexDescriptor = newIndexDescriptor();
        try (MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerSSTableOutput(IndexComponent.GROUP_META)))
        {
            try (final NumericValuesWriter numericWriter = new NumericValuesWriter(indexDescriptor.componentName(IndexComponent.TOKEN_VALUES),
                                                                                   indexDescriptor.openPerSSTableOutput(IndexComponent.TOKEN_VALUES, true, false),
                                                                                   metadataWriter, false))
            {
                for (int x = 0; x < 1000; x++)
                {
                    numericWriter.add(x);
                }
            }

            try (final NumericValuesWriter numericWriter = new NumericValuesWriter(indexDescriptor.componentName(IndexComponent.BLOCK_TERMS_OFFSETS),
                                                                                   indexDescriptor.openPerSSTableOutput(IndexComponent.TOKEN_VALUES, true, false),
                                                                                   metadataWriter, false))
            {
                for (int x = 1000; x < 2000; x++)
                {
                    numericWriter.add(x);
                }
            }
        }

        final MetadataSource source = MetadataSource.loadGroupMetadata(indexDescriptor);
        NumericValuesMeta tokensMeta = new NumericValuesMeta(source.get(indexDescriptor.componentName(IndexComponent.TOKEN_VALUES)));

        try (FileHandle fileHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES);
             LongArray reader = new BlockPackedReader(fileHandle, tokensMeta).open())
        {
            assertEquals(1000, reader.length());

            for (int x = 0; x < 1000; x++)
            {
                assertEquals(x, reader.get(x));
            }
        }

        NumericValuesMeta offsetsMeta = new NumericValuesMeta(source.get(indexDescriptor.componentName(IndexComponent.BLOCK_TERMS_OFFSETS)));

        try (FileHandle fileHandle = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES);
             LongArray reader = new BlockPackedReader(fileHandle, offsetsMeta).open())
        {
            assertEquals(1000, reader.length());

            for (int x = 0; x < 1000; x++)
            {
                assertEquals(x + 1000, reader.get(x));
            }
        }
    }
}
