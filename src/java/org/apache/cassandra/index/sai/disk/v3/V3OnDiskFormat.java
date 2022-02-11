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

import java.lang.invoke.MethodHandles;
import java.util.EnumSet;
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
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.TypeUtil;

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
                                                                              IndexComponent.BLOCK_UPPER_POSTINGS);

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

    @Override
    public boolean validatePerIndexComponents(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean checksum)
    {
        // TODO: validate here?
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
    public boolean validatePerSSTableComponents(IndexDescriptor indexDescriptor, boolean checksum)
    {
//        for (IndexComponent indexComponent : perSSTableComponents())
//        {
//            if (isBuildCompletionMarker(indexComponent))
//                continue;
//            try (IndexInput input = indexDescriptor.openPerSSTableInput(indexComponent))
//            {
//                if (checksum)
//                    SAICodecUtils.validateChecksum(input);
//                else
//                    SAICodecUtils.validate(input);
//            }
//            catch (Throwable e)
//            {
//                if (logger.isDebugEnabled())
//                {
//                    logger.debug(indexDescriptor.logMessage("{} failed for index component {} on SSTable {}"),
//                                 (checksum ? "Checksum validation" : "Validation"),
//                                 indexComponent,
//                                 indexDescriptor.descriptor);
//                }
//                return false;
//            }
//        }
        return true;
    }

    @Override
    // TODO: correct?
    public int openFilesPerSSTable()
    {
        return 4;
    }
}
