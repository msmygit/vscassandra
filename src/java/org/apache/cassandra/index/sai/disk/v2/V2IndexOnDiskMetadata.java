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

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.IndexOnDiskMetadata;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

// TODO: this class is superceded by BlockIndexMeta
public class V2IndexOnDiskMetadata implements IndexOnDiskMetadata
{
    public static final V2IndexOnDiskMetadata.Serializer serializer = new V2IndexOnDiskMetadata.Serializer();

    public final BlockIndexMeta segment;

    public V2IndexOnDiskMetadata(BlockIndexMeta segment)
    {
        this.segment = segment;
    }

    public static class Serializer implements IndexMetadataSerializer
    {
        @Override
        public void serialize(IndexOnDiskMetadata indexMetadata, IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException
        {
            final BlockIndexMeta meta = (BlockIndexMeta)indexMetadata;

            try (final IndexOutput out = indexDescriptor.openPerIndexOutput(IndexComponent.META, indexContext))
            {
                try
                {
                    meta.write(out);
                }
                catch (Exception ex)
                {
                     throw new IOException(ex);
                }
            }
        }

        @Override
        public IndexOnDiskMetadata deserialize(IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException
        {
            try (IndexInput input = indexDescriptor.openPerIndexInput(IndexComponent.META, indexContext))
            {
                try
                {
                    return new BlockIndexMeta(input);
                }
                catch (Exception ex)
                {
                    throw new IOException(ex);
                }
            }
        }
    }
}
