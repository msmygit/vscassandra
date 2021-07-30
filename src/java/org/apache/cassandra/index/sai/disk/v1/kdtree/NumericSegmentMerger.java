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

package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.v1.SegmentMerger;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.TypeUtil;

public class NumericSegmentMerger implements SegmentMerger
{
    final List<BKDReader.IteratorState> segmentIterators = new ArrayList<>();
    final List<BKDReader> readers = new ArrayList<>();

    ByteBuffer minTerm = null, maxTerm = null;

    @Override
    public void addSegment(IndexContext context, SegmentMetadata segment, PerIndexFiles indexFiles) throws IOException
    {
        minTerm = TypeUtil.min(segment.minTerm, minTerm, context.getValidator());
        maxTerm = TypeUtil.max(segment.maxTerm, maxTerm, context.getValidator());

        segmentIterators.add(createIteratorState(context, segment, indexFiles));
    }

    @Override
    public boolean isEmpty()
    {
        return segmentIterators.isEmpty();
    }

    @Override
    public SegmentMetadata merge(IndexDescriptor indexDescriptor, IndexContext context, PrimaryKey minKey, PrimaryKey maxKey, long maxSSTableRowId) throws IOException
    {
        final MergeOneDimPointValues merger = new MergeOneDimPointValues(segmentIterators, context.getValidator());

        final SegmentMetadata.ComponentMetadataMap componentMetadataMap;
        try (NumericIndexWriter indexWriter = new NumericIndexWriter(indexDescriptor,
                                                                     context,
                                                                     TypeUtil.fixedSizeOf(context.getValidator()),
                                                                     maxSSTableRowId,
                                                                     Integer.MAX_VALUE,
                                                                     context.getIndexWriterConfig(),
                                                                     false))
        {
            componentMetadataMap = indexWriter.writeAll(merger);
        }
        return new SegmentMetadata(0,
                                   merger.getNumRows(),
                                   merger.getMinRowID(),
                                   merger.getMaxRowID(),
                                   minKey,
                                   maxKey,
                                   minTerm,
                                   maxTerm,
                                   componentMetadataMap);
    }

    @Override
    public void close() throws IOException
    {
        segmentIterators.forEach(BKDReader.IteratorState::close);
        readers.forEach(BKDReader::close);
    }

    @SuppressWarnings("resource")
    private BKDReader.IteratorState createIteratorState(IndexContext indexContext, SegmentMetadata segment, PerIndexFiles indexFiles) throws IOException
    {
        final long bkdPosition = segment.getIndexRoot(IndexComponent.Type.KD_TREE);
        assert bkdPosition >= 0;
        final long postingsPosition = segment.getIndexRoot(IndexComponent.Type.KD_TREE_POSTING_LISTS);
        assert postingsPosition >= 0;

        final BKDReader bkdReader = new BKDReader(indexContext,
                                                  indexFiles.get(IndexComponent.Type.KD_TREE).sharedCopy(),
                                                  bkdPosition,
                                                  indexFiles.get(IndexComponent.Type.KD_TREE_POSTING_LISTS).sharedCopy(),
                                                  postingsPosition,
                                                  null);
        readers.add(bkdReader);
        return bkdReader.iteratorState(rowid -> rowid + segment.segmentRowIdOffset);
    }
}
