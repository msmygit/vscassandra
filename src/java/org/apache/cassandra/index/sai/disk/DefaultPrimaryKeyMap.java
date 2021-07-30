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

package org.apache.cassandra.index.sai.disk;

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.numerics.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;

public class DefaultPrimaryKeyMap implements PrimaryKeyMap
{
    private final long size;
    private final FileHandle primaryKeysFile;
    private final FileHandle primaryKeyOffsetsFile;
    private final RandomAccessReader primaryKeys;
    private final LongArray primaryKeyOffsets;
    private final NumericValuesMeta meta;
    private final MonotonicBlockPackedReader primaryKeyOffsetsReader;

    private final PrimaryKey.PrimaryKeyFactory keyFactory;

    public DefaultPrimaryKeyMap(IndexDescriptor indexDescriptor, TableMetadata tableMetadata) throws IOException
    {
        final MetadataSource metadataSource = MetadataSource.load(indexDescriptor.openInput(IndexComponent.create(IndexComponent.Type.GROUP_META)));
        meta = new NumericValuesMeta(metadataSource.get(IndexComponent.PRIMARY_KEY_OFFSETS.type.name()));
        this.size = meta.valueCount;

        this.primaryKeyOffsetsFile = indexDescriptor.createFileHandle(IndexComponent.PRIMARY_KEY_OFFSETS);
        this.primaryKeyOffsetsReader = new MonotonicBlockPackedReader(primaryKeyOffsetsFile, meta);
        this.primaryKeyOffsets = primaryKeyOffsetsReader.open();

        this.primaryKeysFile = indexDescriptor.createFileHandle(IndexComponent.PRIMARY_KEYS);
        this.primaryKeys = primaryKeysFile.createReader();

        this.keyFactory = PrimaryKey.factory(tableMetadata);
    }

    private DefaultPrimaryKeyMap(DefaultPrimaryKeyMap orig) throws IOException
    {
        this.primaryKeysFile = orig.primaryKeysFile.sharedCopy();
        this.primaryKeyOffsetsFile = orig.primaryKeyOffsetsFile.sharedCopy();
        this.meta = orig.meta;
        this.primaryKeyOffsetsReader = orig.primaryKeyOffsetsReader;

        this.primaryKeys = primaryKeysFile.createReader();
        this.primaryKeyOffsets = orig.primaryKeyOffsetsReader.open();
        this.keyFactory = orig.keyFactory.copyOf();
        this.size = orig.size();
    }

    @Override
    public PrimaryKeyMap copyOf()
    {
        try
        {
            return new DefaultPrimaryKeyMap(this);
        }
        catch (Throwable e)
        {
            throw Throwables.unchecked(e);
        }
    }

    @Override
    public long size()
    {
        return size;
    }

    @Override
    public PrimaryKey primaryKeyFromRowId(long sstableRowId) throws IOException
    {
        long startOffset = primaryKeyOffsets.get(sstableRowId);
        long endOffset = (sstableRowId + 1) < size
                         ? primaryKeyOffsets.get(sstableRowId + 1)
                         : primaryKeys.length();

        if (endOffset - startOffset <= 0)
            throw new IOException(
            "Primary key length <= 0 for row " + sstableRowId + " of " + primaryKeyOffsetsFile.path());
        if (endOffset - startOffset > Integer.MAX_VALUE)
            throw new IOException(
            "Primary key length too large for row " + sstableRowId + " of " + primaryKeyOffsetsFile.path());

        int length = (int) (endOffset - startOffset);
        byte[] serializedKeyBuf = new byte[length];

        primaryKeys.seek(startOffset);
        primaryKeys.readFully(serializedKeyBuf);
        return keyFactory.createKey(serializedKeyBuf, sstableRowId);
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(primaryKeyOffsets, primaryKeyOffsetsFile, primaryKeys, primaryKeysFile);
    }
}
