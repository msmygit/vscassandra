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
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

public class V1SearchableIndex extends SearchableIndex
{
    private final ImmutableList<Segment> segments;
    private PerIndexFiles indexFiles;

    private final List<SegmentMetadata> metadatas;
    private final PrimaryKey minKey, maxKey; // in token order
    private final ByteBuffer minTerm, maxTerm;
    private final long minSSTableRowId, maxSSTableRowId;
    private final long numRows;

    public V1SearchableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        try
        {
            this.indexFiles = sstableContext.perIndexFiles(indexContext);

            ImmutableList.Builder<Segment> segmentsBuilder = ImmutableList.builder();

            metadatas = ((V1IndexOnDiskMetadata)sstableContext.indexDescriptor.newIndexMetadataSerializer()
                                                                              .deserialize(sstableContext.indexDescriptor, indexContext)).segments;

            for (SegmentMetadata metadata : metadatas)
            {
                segmentsBuilder.add(new Segment(indexContext, sstableContext, indexFiles, metadata));
            }

            segments = segmentsBuilder.build();
            assert !segments.isEmpty();

            this.minKey = metadatas.get(0).minKey;
            this.maxKey = metadatas.get(metadatas.size() - 1).maxKey;

            this.minTerm = metadatas.stream().map(m -> m.minTerm).min(TypeUtil.instance.comparator(indexContext.getValidator())).orElse(null);
            this.maxTerm = metadatas.stream().map(m -> m.maxTerm).max(TypeUtil.instance.comparator(indexContext.getValidator())).orElse(null);

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
    public PostingList missingValuesPostings() throws IOException
    {
        throw new UnsupportedOperationException();
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
    public PrimaryKey minKey()
    {
        return minKey;
    }

    @Override
    public PrimaryKey maxKey()
    {
        return maxKey;
    }

    @Override
    public List<RangeIterator> search(Expression expression, AbstractBounds<PartitionPosition> keyRange, SSTableQueryContext context) throws IOException
    {
        List<RangeIterator> iterators = new ArrayList<>();

        for (Segment segment : segments)
        {
            if (segment.intersects(keyRange))
            {
                iterators.add(segment.search(expression, context));
            }
        }

        return iterators;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(indexFiles);
        FileUtils.closeQuietly(segments);
    }
}
