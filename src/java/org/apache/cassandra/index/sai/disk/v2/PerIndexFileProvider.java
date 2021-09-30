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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Set;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.FileValidator;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.index.sai.disk.format.IndexComponent.COMPRESSED_TERMS_DATA;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.KD_TREE_POSTING_LISTS;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.ORDER_MAP;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.POSTING_LISTS;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.ROW_ID_POINT_ID_MAP;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.TERMS_DATA;
import static org.apache.cassandra.index.sai.disk.format.IndexComponent.TERMS_INDEX;

public class PerIndexFileProvider implements BlockIndexFileProvider
{
    private static final Set<IndexComponent> components = EnumSet.of(TERMS_DATA, TERMS_INDEX, POSTING_LISTS, KD_TREE_POSTING_LISTS);

    private final IndexDescriptor indexDescriptor;
    private final IndexContext indexContext;

    public PerIndexFileProvider(IndexDescriptor indexDescriptor, IndexContext indexContext)
    {
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
    }

    @Override
    public IndexOutputWriter openValuesOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerIndexOutput(TERMS_DATA, indexContext, true, temporary);
    }

    @Override
    public IndexOutputWriter openIndexOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerIndexOutput(TERMS_INDEX, indexContext, true, temporary);
    }

    @Override
    public IndexOutputWriter openLeafPostingsOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerIndexOutput(POSTING_LISTS, indexContext, true, temporary);
    }

    @Override
    public IndexOutputWriter openOrderMapOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerIndexOutput(ORDER_MAP, indexContext, true, temporary);
    }

    @Override
    public IndexOutputWriter openCompressedValuesOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerIndexOutput(COMPRESSED_TERMS_DATA, indexContext, true, temporary);
    }

    @Override
    public IndexOutputWriter openPointIdMapOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerIndexOutput(ROW_ID_POINT_ID_MAP, indexContext, true, temporary);
    }

    @Override
    public IndexOutputWriter openMultiPostingsOutput(boolean temporary) throws IOException
    {
        return indexDescriptor.openPerIndexOutput(KD_TREE_POSTING_LISTS, indexContext, true, temporary);
    }

    @Override
    public IndexInput openComponentInput(IndexComponent indexComponent)
    {
        return indexDescriptor.openPerIndexInput(ORDER_MAP, indexContext);
    }

    @Override
    public SharedIndexInput openLeafPostingsInput(boolean temporary)
    {
        return new SharedIndexInput(indexDescriptor.openPerIndexInput(POSTING_LISTS, indexContext, temporary));
    }

    @Override
    public SharedIndexInput openValuesInput(boolean temporary)
    {
        return new SharedIndexInput(indexDescriptor.openPerIndexInput(TERMS_DATA, indexContext, temporary));
    }

    @Override
    public SharedIndexInput openIndexInput(boolean temporary)
    {
        return new SharedIndexInput(indexDescriptor.openPerIndexInput(TERMS_INDEX, indexContext, temporary));
    }

    @Override
    public SharedIndexInput openOrderMapInput(boolean temporary)
    {
        return new SharedIndexInput(indexDescriptor.openPerIndexInput(ORDER_MAP, indexContext, temporary));
    }

    @Override
    public SharedIndexInput openMultiPostingsInput(boolean temporary)
    {
        return new SharedIndexInput(indexDescriptor.openPerIndexInput(KD_TREE_POSTING_LISTS, indexContext, temporary));
    }

    @Override
    public SharedIndexInput openCompressedValuesInput(boolean temporary)
    {
        return new SharedIndexInput(indexDescriptor.openPerIndexInput(COMPRESSED_TERMS_DATA, indexContext, temporary));
    }

    @Override
    public FileHandle getIndexFileHandle(boolean temporary)
    {
        return null;
    }

    @Override
    public HashMap<IndexComponent, FileValidator.FileInfo> fileInfoMap(boolean temporary) throws IOException
    {
        return new HashMap<>();
    }

    @Override
    public void close() throws Exception
    {
        //TODO Close stuff here
    }
}
