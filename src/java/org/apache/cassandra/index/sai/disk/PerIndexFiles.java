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

import java.io.Closeable;
import java.util.Map;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;

public abstract class PerIndexFiles implements Closeable
{
    private final IndexDescriptor indexDescriptor;
    private final IndexContext columnContext;
    private final Map<IndexComponent.Type, FileHandle> files;

    public PerIndexFiles(IndexDescriptor indexDescriptor, IndexContext columnContext)
    {
        this(indexDescriptor, columnContext, false);
    }

    public PerIndexFiles(IndexDescriptor indexDescriptor, IndexContext columnContext, boolean temporary)
    {
        this.indexDescriptor = indexDescriptor;
        this.columnContext = columnContext;
        files = populate(indexDescriptor, columnContext, temporary);
    }

    public FileHandle get(IndexComponent.Type type)
    {
        FileHandle file = files.get(type);
        if (file == null)
            throw new IllegalArgumentException(String.format(columnContext.logMessage("Component for %s not found for SSTable %s"),
                                                             type.representation,
                                                             indexDescriptor.descriptor));

        return file;
    }

    protected abstract Map<IndexComponent.Type, FileHandle> populate(IndexDescriptor indexDescriptor, IndexContext columnContext, boolean temporary);

    protected void putFile(IndexComponent.Type type, boolean temporary)
    {
        files.put(type, indexDescriptor.createFileHandle(IndexComponent.create(type, columnContext.getIndexName()), temporary));
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(files.values());
    }
}
