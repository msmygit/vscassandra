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

import java.io.Closeable;
import java.util.EnumMap;
import java.util.Map;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;

public class PerIndexFiles implements Closeable
{
    protected final Map<IndexComponent, FileHandle> files = new EnumMap<>(IndexComponent.class);
    protected final IndexDescriptor indexDescriptor;
    protected final IndexContext indexContext;
    protected final boolean temporary;

    public PerIndexFiles(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean temporary)
    {
        this.indexDescriptor = indexDescriptor;
        this.indexContext = indexContext;
        this.temporary = temporary;
    }

    public boolean isTemporary()
    {
        return temporary;
    }

    public IndexDescriptor indexDescriptor()
    {
        return indexDescriptor;
    }

    public FileHandle termsData()
    {
        return getFile(IndexComponent.TERMS_DATA);
    }

    public FileHandle postingLists()
    {
        return getFile(IndexComponent.POSTING_LISTS);
    }

    public FileHandle kdtree()
    {
        return getFile(IndexComponent.KD_TREE);
    }

    public FileHandle kdtreePostingLists()
    {
        return getFile(IndexComponent.KD_TREE_POSTING_LISTS);
    }

    protected FileHandle getFile(IndexComponent indexComponent)
    {
        return files.computeIfAbsent(indexComponent, (comp) -> indexDescriptor.createPerIndexFileHandle(indexComponent, indexContext, temporary));
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(files.values());
    }
}
