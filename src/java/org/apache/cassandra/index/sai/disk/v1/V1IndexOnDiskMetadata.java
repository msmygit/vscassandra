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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.IndexOnDiskMetadata;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class V1IndexOnDiskMetadata implements IndexOnDiskMetadata
{
    private static final String NAME = "SegmentMetadata";

    public static final Serializer serializer = new Serializer();

    public final List<SegmentMetadata> segments;

    public V1IndexOnDiskMetadata(List<SegmentMetadata> segments)
    {
        this.segments = segments;
    }

    private static class Serializer implements IndexMetadataSerializer
    {
        @Override
        public void serialize(IndexOnDiskMetadata indexMetadata, IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException
        {
            List<SegmentMetadata> segments = ((V1IndexOnDiskMetadata)indexMetadata).segments;

            try (MetadataWriter writer = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, indexContext));
                 IndexOutput output = writer.builder(NAME))
            {
                output.writeVInt(segments.size());

                for (SegmentMetadata metadata : segments)
                {
                    output.writeLong(metadata.segmentRowIdOffset);
                    output.writeLong(metadata.numRows);
                    output.writeLong(metadata.minSSTableRowId);
                    output.writeLong(metadata.maxSSTableRowId);

                    Stream.of(metadata.minKey.partitionKey().getKey(),
                              metadata.maxKey.partitionKey().getKey(),
                              metadata.minTerm, metadata.maxTerm).forEach(bb -> writeBytes(bb, output));

                    metadata.componentMetadatas.write(output);
                }
            }
        }

        @Override
        public IndexOnDiskMetadata deserialize(IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException
        {
            PrimaryKey.PrimaryKeyFactory primaryKeyFactory = indexDescriptor.primaryKeyFactory;
            MetadataSource source = MetadataSource.load(indexDescriptor.openPerIndexInput(IndexComponent.META, indexContext));

            IndexInput input = source.get(NAME);

            int segmentCount = input.readVInt();

            List<SegmentMetadata> segmentMetadata = new ArrayList<>(segmentCount);

            for (int i = 0; i < segmentCount; i++)
            {
                long segmentRowIdOffset = input.readLong();

                long numRows = input.readLong();
                long minSSTableRowId = input.readLong();
                long maxSSTableRowId = input.readLong();
                PrimaryKey minKey = primaryKeyFactory.createKey(DatabaseDescriptor.getPartitioner().decorateKey(readBytes(input)));
                PrimaryKey maxKey = primaryKeyFactory.createKey(DatabaseDescriptor.getPartitioner().decorateKey(readBytes(input)));

                ByteBuffer minTerm = readBytes(input);
                ByteBuffer maxTerm = readBytes(input);
                SegmentMetadata.ComponentMetadataMap componentMetadatas = new SegmentMetadata.ComponentMetadataMap(input);

                segmentMetadata.add(new SegmentMetadata(segmentRowIdOffset,
                                                        numRows,
                                                        minSSTableRowId,
                                                        maxSSTableRowId,
                                                        minKey,
                                                        maxKey,
                                                        minTerm,
                                                        maxTerm,
                                                        componentMetadatas));
            }

            return new V1IndexOnDiskMetadata(segmentMetadata);
        }

        private ByteBuffer readBytes(IndexInput input) throws IOException
        {
            int len = input.readVInt();
            byte[] bytes = new byte[len];
            input.readBytes(bytes, 0, len);
            return ByteBuffer.wrap(bytes);
        }

        private void writeBytes(ByteBuffer buf, IndexOutput out)
        {
            try
            {
                byte[] bytes = ByteBufferUtil.getArray(buf);
                out.writeVInt(bytes.length);
                out.writeBytes(bytes, 0, bytes.length);
            }
            catch (IOException ioe)
            {
                throw new RuntimeException(ioe);
            }
        }
    }
}
