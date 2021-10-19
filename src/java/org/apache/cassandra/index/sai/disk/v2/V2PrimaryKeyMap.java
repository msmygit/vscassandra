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

import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

public class V2PrimaryKeyMap implements PrimaryKeyMap
{
    public static class V2PrimaryKeyMapFactory implements PrimaryKeyMap.Factory
    {
        private final BlockIndexFileProvider fileProvider;
        private final BlockIndexMeta metadata;
        private final BlockIndexReader reader;
        private final PrimaryKey.PrimaryKeyFactory keyFactory;
        private final long size;

        public V2PrimaryKeyMapFactory(IndexDescriptor indexDescriptor) throws IOException
        {
            fileProvider = new PerSSTableFileProvider(indexDescriptor);
            this.keyFactory = indexDescriptor.primaryKeyFactory;
            if (indexDescriptor.isSSTableEmpty())
            {
                metadata = null;
                size = 0;
                reader = null;
            }
            else
            {
                try (IndexInput input = fileProvider.openMetadataInput())
                {
                    metadata = new BlockIndexMeta(input);
                }
                size = metadata.numRows;
                reader = new BlockIndexReader(fileProvider, false, metadata);
            }
        }

        @Override
        public PrimaryKeyMap newPerSSTablePrimaryKeyMap(SSTableQueryContext context) throws IOException
        {
            //TODO We need a test for this condition with an empty SSTable (not sure it's possible)
            return (size == 0) ? EMPTY : new V2PrimaryKeyMap(reader, keyFactory, size);
        }

        @Override
        public void close() throws IOException
        {
            FileUtils.closeQuietly(fileProvider, reader);
        }
    }

    private final BlockIndexReader reader;
    private final BlockIndexReader.BlockIndexReaderContext context;
    private final PrimaryKey.PrimaryKeyFactory keyFactory;
    private final long size;

    private V2PrimaryKeyMap(BlockIndexReader reader,
                            PrimaryKey.PrimaryKeyFactory keyFactory,
                            long size)
    {
        this.reader = reader;
        this.context = reader.initContext();
        this.keyFactory = keyFactory;
        this.size = size;
    }

    @Override
    public boolean maybeContains(PrimaryKey key)
    {
        return false;
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId) throws IOException
    {
        BytesRef term = reader.seekTo(sstableRowId, context, true);
        return keyFactory.createKey(new ByteSource.Peekable(ByteSource.fixedLength(term.bytes, 0, term.length)), sstableRowId);
    }

    @Override
    public long rowIdFromPrimaryKey(PrimaryKey key) throws IOException
    {
        BytesRef bytesRef = new BytesRef(ByteSourceInverse.readBytes(key.asComparableBytes(ByteComparable.Version.OSS41)));
        return reader.seekTo(bytesRef, context);
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(context);
    }
}
