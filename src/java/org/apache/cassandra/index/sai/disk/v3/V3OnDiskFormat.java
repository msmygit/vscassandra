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

package org.apache.cassandra.index.sai.disk.v3;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;

import static org.apache.cassandra.utils.FBUtilities.prettyPrintMemory;

public class V3OnDiskFormat extends V2OnDiskFormat
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final V3OnDiskFormat instance = new V3OnDiskFormat();

    private static final IndexFeatureSet v3IndexFeatureSet = () -> true;

    private static final Set<IndexComponent> V3_INDEX_COMPONENTS = EnumSet.of(IndexComponent.COLUMN_COMPLETION_MARKER,
                                                                              IndexComponent.META,
                                                                              IndexComponent.BLOCK_BITPACKED,
                                                                              IndexComponent.BLOCK_ORDERMAP,
                                                                              IndexComponent.BLOCK_TERMS_DATA,
                                                                              IndexComponent.BLOCK_TERMS_INDEX,
                                                                              IndexComponent.BLOCK_POSTINGS,
                                                                              IndexComponent.BLOCK_UPPER_POSTINGS,
                                                                              // IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS,
                                                                              IndexComponent.TERMS_DATA,
                                                                              IndexComponent.POSTING_LISTS);

    protected V3OnDiskFormat()
    {
    }

    @Override
    public Set<IndexComponent> perIndexComponents(IndexContext indexContext)
    {
        return V3_INDEX_COMPONENTS;
    }

    @Override
    public SearchableIndex newSearchableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        return new V3SearchableIndex(sstableContext, indexContext);
    }

    private void validateComponent(FileValidation.Meta meta,
                                   IndexComponent indexComponent,
                                   V3PerIndexFiles indexFiles) throws IOException
    {
        final FileHandle handle = indexFiles.getFileAndCache(indexComponent);
        validateCRC(handle, meta);
    }

    private void validateCRC(FileHandle handle, FileValidation.Meta meta) throws IOException
    {
        try (IndexInput input = IndexInputReader.create(handle))
        {
            validateCRC(input, meta);
        }
    }

    private void validateCRC(IndexInput input, FileValidation.Meta meta) throws IOException
    {
        if (!FileValidation.validate(input, meta))
            throw new IOException("corrupt");
    }

    @Override
    public boolean validatePerIndexComponents(IndexDescriptor indexDescriptor,
                                              IndexContext indexContext,
                                              boolean checksum)
    {
        try
        {
            try (final V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false))
            {
                // load each segment reader which validates the files
                final MetadataSource source = MetadataSource.loadColumnMetadata(indexDescriptor, indexContext);

                final List<SegmentMetadata> metadatas = SegmentMetadata.load(source, indexDescriptor.primaryKeyFactory);

                for (SegmentMetadata metadata : metadatas)
                {
                    final SegmentMetadata.ComponentMetadata termsDataCompMeta = metadata.componentMetadatas.get(IndexComponent.BLOCK_TERMS_DATA);
                    final BlockTerms.Meta meta = new BlockTerms.Meta(termsDataCompMeta.attributes);

                    validateComponent(meta.termsCRC,
                                      IndexComponent.BLOCK_TERMS_DATA,
                                      indexFiles);
                    validateComponent(meta.termsIndexCRC,
                                      IndexComponent.BLOCK_TERMS_INDEX,
                                      indexFiles);
                    validateComponent(meta.bitpackedCRC,
                                      IndexComponent.BLOCK_BITPACKED,
                                      indexFiles);
                    validateComponent(meta.postingsCRC,
                                      IndexComponent.BLOCK_POSTINGS,
                                      indexFiles);
                    validateComponent(meta.orderMapCRC,
                                      IndexComponent.BLOCK_ORDERMAP,
                                      indexFiles);

                    if (indexFiles.exists(IndexComponent.BLOCK_UPPER_POSTINGS, indexFiles.isTemporary()))
                    {
                        validateComponent(meta.upperPostingsCRC,
                                          IndexComponent.BLOCK_UPPER_POSTINGS,
                                          indexFiles);
                    }
//                    validateComponent(meta.upperPostingsOffsetsCRC,
//                                      IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS,
//                                      indexFiles);
                }
            }
        }
        catch (Throwable th)
        {
            logger.error("File validation failed", th);
            return false;
        }
        return true;
    }

    @Override
    public PerIndexWriter newPerIndexWriter(StorageAttachedIndex index,
                                            IndexDescriptor indexDescriptor,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping)
    {
        // If we're not flushing or we haven't yet started the initialization build, flush from SSTable contents.
        if (tracker.opType() != OperationType.FLUSH || !index.isInitBuildStarted())
        {
            NamedMemoryLimiter limiter = SEGMENT_BUILD_MEMORY_LIMITER;
            logger.info(index.getIndexContext().logMessage("Starting a compaction index build. Global segment memory usage: {}"),
                        prettyPrintMemory(limiter.currentBytesUsed()));

            return new V3SSTableIndexWriter(indexDescriptor, index.getIndexContext(), limiter, index.isIndexValid());
        }

        return new V3MemtableIndexWriter(index.getIndexContext().getPendingMemtableIndex(tracker),
                                         indexDescriptor,
                                         index.getIndexContext(),
                                         rowMapping);
    }

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v3IndexFeatureSet;
    }

    @Override
    public int openFilesPerSSTable()
    {
        return 8;
    }
}
