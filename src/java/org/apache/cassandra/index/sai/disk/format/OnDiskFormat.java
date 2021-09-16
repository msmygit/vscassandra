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

package org.apache.cassandra.index.sai.disk.format;

import java.io.IOException;
import java.util.Set;

import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.IndexOnDiskMetadata;
import org.apache.cassandra.index.sai.disk.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Represents the on-disk format of an index. This determines how the per-sstable and
 * per-index files are written to and read from disk.
 *
 * The methods on this interface can be logically mapped into the following groups
 * based on their method parameters:
 * <ul>
 *     <li>Methods taking no parameters. These methods return static information about the
 *     format. This can include static information about the per-sstable components</li>
 *     <li>Methods taking just an {@link IndexContext}. These methods return static information
 *     specific to the index. This can the information relating to the type of index being used</li>
 *     <li>Methods taking an {@link IndexDescriptor}. These methods interact with the on-disk components or
 *     return objects that will interact with the on-disk components or return information about the on-disk
 *     components. If they take an {@link IndexContext} as well they will be interacting with per-index files
 *     otherwise they will be interacting with per-sstable files</li>
 *     <li>Methods taking an {@link IndexComponent}. This methods only interact with a single component or
 *     set of components</li>
 *
 * </ul>
 */
public interface OnDiskFormat
{
    /**
     * Returns the {@link IndexFeatureSet} for the on-disk format
     *
     * @return the index feature set
     */
    public IndexFeatureSet indexFeatureSet();

    /**
     * Returns true if the per-sstable index components have been built and are valid.
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable SAI index
     * @return true if the per-sstable index components have been built and are complete
     */
    public boolean isPerSSTableBuildComplete(IndexDescriptor indexDescriptor);

    /**
     * Returns true if the per-index index components have been built and are valid.
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable SAI Index
     * @param indexContext The {@link IndexContext} for the index
     * @return true if the per-index index components have been built and are complete
     */
    public boolean isPerIndexBuildComplete(IndexDescriptor indexDescriptor, IndexContext indexContext);

    /**
     * Returns true if the component is used to indicate that the index files are
     * built and complete.
     *
     * @param indexComponent The {@link IndexComponent} to be tested
     * @return true if the component is a build completion marker
     */
    public boolean isBuildCompletionMarker(IndexComponent indexComponent);

    /**
     * Returns true if the {@link IndexComponent} can be encrypted on disk
     *
     * @param indexComponent The {@link IndexComponent} to be tested
     * @return true if the component can be encrypted
     */
    public boolean isEncryptable(IndexComponent indexComponent);

    /**
     * Returns a {@link PrimaryKeyMap.Factory} for the SSTable
     *
     * @param indexDescriptor The {@link IndexDescriptor} for the SSTable
     * @param sstable
     * @return
     * @throws IOException
     */
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable) throws IOException;

    public SearchableIndex newSearchableIndex(SSTableContext ssTableContext, IndexContext indexContext);

    public PerSSTableWriter newPerSSTableWriter(IndexDescriptor indexDescriptor) throws IOException;

    public PerIndexWriter newPerIndexWriter(StorageAttachedIndex index,
                                            IndexDescriptor indexDescriptor,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping);

    public IndexOnDiskMetadata.IndexMetadataSerializer newIndexMetadataSerializer();

    public void validatePerSSTableComponent(IndexDescriptor indexDescriptor, IndexComponent indexComponent, boolean checksum) throws IOException;

    public void validatePerIndexComponent(IndexDescriptor indexDescriptor, IndexComponent indexComponent, IndexContext indexContext, boolean checksum) throws IOException;

    public Set<IndexComponent> perSSTableComponents();

    public Set<IndexComponent> perIndexComponents(IndexContext indexContext);

    public PerIndexFiles perIndexFiles(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean temporary);

    public int openFilesPerSSTable();

    public int openFilesPerIndex(IndexContext indexContext);
}
