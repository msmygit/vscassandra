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
import java.nio.ByteBuffer;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.IndexOnDiskMetadata;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;

public class V2IndexOnDiskMetadata implements IndexOnDiskMetadata
{
    private static final String NAME = "SegmentMetadata";

    public static final V2IndexOnDiskMetadata.Serializer serializer = new V2IndexOnDiskMetadata.Serializer();

    public final SegmentMetadata segment;

    public V2IndexOnDiskMetadata(SegmentMetadata segment)
    {
        this.segment = segment;
    }

    public static class Serializer implements IndexMetadataSerializer
    {
        @Override
        public void serialize(IndexOnDiskMetadata indexMetadata, IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException
        {
            SegmentMetadata segment = ((V2IndexOnDiskMetadata) indexMetadata).segment;

            try (MetadataWriter writer = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, indexContext));
                 MetadataWriter.Builder output = writer.builder(NAME))
            {
                output.writeLong(segment.segmentRowIdOffset);
                output.writeLong(segment.numRows);
                output.writeLong(segment.minSSTableRowId);
                output.writeLong(segment.maxSSTableRowId);
                PrimaryKey.serializer.serialize(output, 0, segment.minKey);
                PrimaryKey.serializer.serialize(output, 0, segment.maxKey);
                writeBytes(ByteBufferUtil.getArray(segment.minTerm), output);
                writeBytes(ByteBufferUtil.getArray(segment.maxTerm), output);
                segment.componentMetadatas.write(output);
            }
        }

        @Override
        public IndexOnDiskMetadata deserialize(IndexDescriptor indexDescriptor, IndexContext indexContext) throws IOException
        {
            PrimaryKey.PrimaryKeyFactory primaryKeyFactory = indexDescriptor.primaryKeyFactory;
            MetadataSource source = MetadataSource.load(indexDescriptor.openPerIndexInput(IndexComponent.META, indexContext));

            DataInputPlus input = source.getDataInput(NAME);

            long segmentRowIdOffset = input.readLong();

            long numRows = input.readLong();
            long minSSTableRowId = input.readLong();
            long maxSSTableRowId = input.readLong();


            PrimaryKey minKey = primaryKeyFactory.createKey(input, -1);
            PrimaryKey maxKey = primaryKeyFactory.createKey(input, -1);

            ByteBuffer minTerm = ByteBuffer.wrap(readBytes(input));
            ByteBuffer maxTerm = ByteBuffer.wrap(readBytes(input));
            SegmentMetadata.ComponentMetadataMap componentMetadatas = new SegmentMetadata.ComponentMetadataMap(input);

            return new V2IndexOnDiskMetadata(new SegmentMetadata(segmentRowIdOffset,
                                                                 numRows,
                                                                 minSSTableRowId,
                                                                 maxSSTableRowId,
                                                                 minKey,
                                                                 maxKey,
                                                                 minTerm,
                                                                 maxTerm,
                                                                 componentMetadatas));
        }

        private byte[] readBytes(DataInputPlus input) throws IOException
        {
            int len = input.readInt();
            byte[] bytes = new byte[len];
            input.readFully(bytes, 0, len);
            return bytes;
        }

        private void writeBytes(byte[] bytes, DataOutputPlus out)
        {
            try
            {
                out.writeInt(bytes.length);
                out.write(bytes, 0, bytes.length);
            }
            catch (IOException ioe)
            {
                throw new RuntimeException(ioe);
            }
        }
    }
}
