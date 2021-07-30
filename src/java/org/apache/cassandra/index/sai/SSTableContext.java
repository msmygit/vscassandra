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
package org.apache.cassandra.index.sai;

import com.google.common.base.Objects;

import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.V1SSTableContext;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.utils.concurrent.RefCounted;
import org.apache.cassandra.utils.concurrent.SharedCloseableImpl;

/**
 * SSTableContext is created for individual sstable shared across indexes to track per-sstable index files.
 *
 * SSTableContext itself will be released when receiving sstable removed notification, but its shared copies in individual
 * SSTableIndex will be released when in-flight read requests complete.
 */
public abstract class SSTableContext extends SharedCloseableImpl
{
    public final SSTableReader sstable;
    public final IndexDescriptor indexDescriptor;
    public final PrimaryKeyMap primaryKeyMap;

    protected SSTableContext(SSTableReader sstable, PrimaryKeyMap primaryKeyMap, IndexDescriptor indexDescriptor, RefCounted.Tidy tidy)
    {
        super(tidy);
        this.sstable = sstable;
        this.primaryKeyMap = primaryKeyMap;
        this.indexDescriptor = indexDescriptor;
    }

    protected SSTableContext(SSTableReader sstable, IndexDescriptor indexDescriptor, SSTableContext copy)
    {
        super(copy);
        this.primaryKeyMap = copy.primaryKeyMap;
        this.sstable = sstable;
        this.indexDescriptor = indexDescriptor;
    }

    @SuppressWarnings("resource")
    public static SSTableContext create(SSTableReader sstable, IndexDescriptor indexDescriptor)
    {
        // We currently only support the V1 per-sstable format but in future selection of
        // per-sstable format will be here
        return V1SSTableContext.create(sstable, indexDescriptor);
    }

    @Override
    public abstract SSTableContext sharedCopy();
    /**
     * @return number of open files per {@link SSTableContext} instance
     */

    /**
     * @return descriptor of attached sstable
     */
    public Descriptor descriptor()
    {
        return sstable.descriptor;
    }

    public SSTableReader sstable()
    {
        return sstable;
    }

    public abstract int openFilesPerSSTable();

    /**
     * @return disk usage of per-sstable index files
     */
    public abstract long diskUsage();

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sstable.descriptor.hashCode());
    }

    public abstract PerIndexFiles perIndexFiles(IndexContext columnContext);
}
