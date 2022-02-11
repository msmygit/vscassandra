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
import java.nio.ByteBuffer;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.BytesRefUtil;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SSTableIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.v1.SegmentMerger;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.lucene.util.BytesRef;

public class V3SSTableIndexWriter extends SSTableIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(V3SSTableIndexWriter.class);

    public V3SSTableIndexWriter(IndexDescriptor indexDescriptor, IndexContext indexContext, NamedMemoryLimiter limiter, BooleanSupplier isIndexValid)
    {
        super(indexDescriptor, indexContext, limiter, isIndexValid);
    }

    public static class V3RAMStringSegmentBuilder extends SegmentBuilder.RAMStringSegmentBuilder
    {
        protected byte[] buffer = null;

        public V3RAMStringSegmentBuilder(AbstractType<?> termComparator, NamedMemoryLimiter limiter)
        {
            super(termComparator, limiter);
        }

        @Override
        protected long addInternal(ByteBuffer term, int segmentRowId)
        {
            if (TypeUtil.isLiteral(termComparator))
            {
                BytesRefUtil.copyBufferToBytesRef(term, stringBuffer);
                return ramIndexer.add(stringBuffer.get(), segmentRowId);
            }
            else
            {
                if (this.buffer == null)
                {
                    int typeSize = TypeUtil.fixedSizeOf(termComparator);
                    this.buffer = new byte[typeSize];
                }
                TypeUtil.toComparableBytes(term, termComparator, buffer);
                return ramIndexer.add(new BytesRef(buffer), segmentRowId);
            }
        }

        @Override
        protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor,
                                                                     IndexContext indexContext) throws IOException
        {
            final BlockTerms.Writer writer = new BlockTerms.Writer(indexDescriptor, indexContext, true);
            return writer.writeAll(ramIndexer.getTermsWithPostings());
        }
    }

    @Override
    protected SegmentMerger newSegmentMerger(boolean literal)
    {
        return new BlockTermsSegmentMerger();
    }

    @Override
    protected PerIndexFiles newPerIndexFiles(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean temporary)
    {
        return new V3PerIndexFiles(indexDescriptor, indexContext, temporary);
    }

    @Override
    protected SegmentBuilder newSegmentBuilder()
    {
        final SegmentBuilder builder = new V3RAMStringSegmentBuilder(indexContext.getValidator(), limiter);

        long globalBytesUsed = limiter.increment(builder.totalBytesAllocated());
        logger.debug(indexContext.logMessage("Created new segment builder while flushing SSTable {}. Global segment memory usage now at {}."),
                     indexDescriptor.descriptor,
                     FBUtilities.prettyPrintMemory(globalBytesUsed));

        return builder;
    }
}
