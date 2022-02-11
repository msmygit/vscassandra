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
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMerger;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileUtils;

public class BlockTermsSegmentMerger implements SegmentMerger
{
    private ByteBuffer minTerm = null, maxTerm = null;
    private long maxRowid = Long.MIN_VALUE;
    private long minRowId = Long.MAX_VALUE;
    private final List<BlockTerms.Reader.PointsIterator> segmentIterators = new ArrayList<>();

    @Override
    public void addSegment(IndexContext context, SegmentMetadata segment, PerIndexFiles indexFiles) throws IOException
    {
        if (!(indexFiles instanceof V3PerIndexFiles))
            throw new IllegalArgumentException();

        minTerm = TypeUtil.min(segment.minTerm, minTerm, context.getValidator());
        maxTerm = TypeUtil.max(segment.maxTerm, maxTerm, context.getValidator());
        minRowId = Math.min(segment.minSSTableRowId, minRowId);
        maxRowid = Math.max(segment.maxSSTableRowId, maxRowid);

        segmentIterators.add(createIterator(context, segment, (V3PerIndexFiles) indexFiles));
    }

    private BlockTerms.Reader.PointsIterator createIterator(IndexContext indexContext,
                                                            SegmentMetadata segment,
                                                            V3PerIndexFiles indexFiles) throws IOException
    {
        final BlockTerms.Reader reader = new BlockTerms.Reader(indexFiles.indexDescriptor(),
                                                               indexContext,
                                                               indexFiles,
                                                               segment.componentMetadatas);
        return reader.pointsIterator(reader, segment.segmentRowIdOffset);
    }

    @Override
    public boolean isEmpty()
    {
        return segmentIterators.isEmpty();
    }

    @Override
    public SegmentMetadata merge(IndexDescriptor indexDescriptor,
                                 IndexContext indexContext,
                                 PrimaryKey minKey,
                                 PrimaryKey maxKey,
                                 long maxSSTableRowId) throws IOException
    {
        try
        {
            try (final MergePointsIterators merger = new MergePointsIterators(segmentIterators))
            {
                final BlockTerms.Writer writer = new BlockTerms.Writer(indexDescriptor, indexContext, false);
                final SegmentMetadata.ComponentMetadataMap indexMetas = writer.writeAll(merger);
                return new SegmentMetadata(0,
                                           writer.meta.pointCount,
                                           minRowId,
                                           maxRowid,
                                           minKey,
                                           maxKey,
                                           minTerm,
                                           maxTerm,
                                           indexMetas);
            }
        }
        catch (Exception ex)
        {
             throw new IOException(ex);
        }
    }

    @Override
    public void close() throws IOException
    {
        for (BlockTerms.Reader.PointsIterator iterator : segmentIterators)
            FileUtils.closeQuietly(iterator);
    }
}
