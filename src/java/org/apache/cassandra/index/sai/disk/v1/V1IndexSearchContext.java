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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.IndexSearchContext;
import org.apache.cassandra.index.sai.disk.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeConcatIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

public class V1IndexSearchContext extends IndexSearchContext
{
    private final ImmutableList<Segment> segments;
    private PerIndexFiles indexFiles;

    private final List<SegmentMetadata> metadatas;
    private final DecoratedKey minKey, maxKey; // in token order
    private final ByteBuffer minTerm, maxTerm;
    private final long minSSTableRowId, maxSSTableRowId;
    private final long numRows;

    public V1IndexSearchContext(V1SSTableContext sstableContext, IndexContext indexContext)
    {
        try
        {
            this.indexFiles = sstableContext.perIndexFiles(indexContext);

            ImmutableList.Builder<Segment> segmentsBuilder = ImmutableList.builder();

            final MetadataSource source = MetadataSource.load(sstableContext.indexDescriptor.openPerIndexInput(IndexComponent.META,
                                                                                                               indexContext.getIndexName()));
            metadatas = SegmentMetadata.load(source, null);

            for (SegmentMetadata metadata : metadatas)
            {
                segmentsBuilder.add(new Segment(indexContext, sstableContext, indexFiles, metadata));
            }

            segments = segmentsBuilder.build();
            assert !segments.isEmpty();

            this.minKey = metadatas.get(0).minKey;
            this.maxKey = metadatas.get(metadatas.size() - 1).maxKey;

            this.minTerm = metadatas.stream().map(m -> m.minTerm).min(TypeUtil.comparator(indexContext.getValidator())).orElse(null);
            this.maxTerm = metadatas.stream().map(m -> m.maxTerm).max(TypeUtil.comparator(indexContext.getValidator())).orElse(null);

            this.numRows = metadatas.stream().mapToLong(m -> m.numRows).sum();

            this.minSSTableRowId = metadatas.get(0).minSSTableRowId;
            this.maxSSTableRowId = metadatas.get(metadatas.size() - 1).maxSSTableRowId;
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(indexFiles);
            FileUtils.closeQuietly(sstableContext);
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public long indexFileCacheSize()
    {
        return segments.stream().mapToLong(Segment::indexFileCacheSize).sum();
    }

    @Override
    public long getRowCount()
    {
        return numRows;
    }

    @Override
    public long minSSTableRowId()
    {
        return minSSTableRowId;
    }

    @Override
    public long maxSSTableRowId()
    {
        return maxSSTableRowId;
    }

    @Override
    public ByteBuffer minTerm()
    {
        return minTerm;
    }

    @Override
    public ByteBuffer maxTerm()
    {
        return maxTerm;
    }

    @Override
    public DecoratedKey minKey()
    {
        return minKey;
    }

    @Override
    public DecoratedKey maxKey()
    {
        return maxKey;
    }

    @Override
    public RangeIterator search(Expression expression, AbstractBounds<PartitionPosition> keyRange, SSTableQueryContext context, boolean defer)
    {
        RangeConcatIterator.Builder builder = RangeConcatIterator.builder();

        for (Segment segment : segments)
        {
            if (segment.intersects(keyRange))
            {
                builder.add(segment.search(expression, context, defer));
            }
        }

        return builder.build();
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(indexFiles);
        FileUtils.closeQuietly(segments);
    }
}
