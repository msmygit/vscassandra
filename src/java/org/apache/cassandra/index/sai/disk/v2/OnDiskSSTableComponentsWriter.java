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

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.SSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.numerics.NumericValuesWriter;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.lucene.store.IndexOutput;

public class OnDiskSSTableComponentsWriter implements SSTableComponentsWriter
{
    private final IndexDescriptor indexDescriptor;

    private final MetadataWriter metadataWriter;
    private final IndexOutput primaryKeysOut;
    private final NumericValuesWriter primaryKeyOffsets;

    public OnDiskSSTableComponentsWriter(IndexDescriptor indexDescriptor,
                                         CompressionParams compressionParams) throws IOException
    {
        this.indexDescriptor = indexDescriptor;

        primaryKeysOut = indexDescriptor.openOutput(IndexComponent.PRIMARY_KEYS);

        SAICodecUtils.writeHeader(this.primaryKeysOut);

        this.metadataWriter = new MetadataWriter(indexDescriptor.openOutput(IndexComponent.GROUP_META, false, false));

        this.primaryKeyOffsets = new NumericValuesWriter(IndexComponent.PRIMARY_KEY_OFFSETS,
                                                         indexDescriptor.openOutput(IndexComponent.PRIMARY_KEY_OFFSETS, false, false),
                                                         metadataWriter, true);
    }

    public void nextRow(PrimaryKey key) throws IOException
    {
        long offset = primaryKeysOut.getFilePointer();
        byte[] rawKey = key.asBytes();
        primaryKeysOut.writeBytes(rawKey, rawKey.length);
        primaryKeyOffsets.add(offset);
    }

    public void complete() throws IOException
    {
        SAICodecUtils.writeFooter(primaryKeysOut);
        FileUtils.close(primaryKeysOut, primaryKeyOffsets, metadataWriter);
        indexDescriptor.createComponentOnDisk(IndexComponent.GROUP_COMPLETION_MARKER);
    }

    public void abort(Throwable accumulator)
    {
        logger.debug(indexDescriptor.logMessage("Aborting token/offset writer for {}..."), indexDescriptor.descriptor);
        indexDescriptor.deletePerSSTableIndexComponents();
    }
}
