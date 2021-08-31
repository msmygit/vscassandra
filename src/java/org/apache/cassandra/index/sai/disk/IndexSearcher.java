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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.v1.kdtree.KDTreeIndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.trie.InvertedIndexSearcher;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.disk.v1.Segment;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.V1SSTableContext;
import org.apache.cassandra.index.sai.disk.v1.PerIndexFiles;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;

/**
 * Abstract reader for individual segments of an on-disk index.
 *
 * Accepts shared resources (token/offset file readers), and uses them to perform lookups against on-disk data
 * structures.
 */
public abstract class IndexSearcher implements Closeable
{
    private final PrimaryKeyMap primaryKeyMap;
    private final V1SSTableContext.KeyFetcher keyFetcher;
    protected final PerIndexFiles indexFiles;
    protected final SegmentMetadata metadata;
    protected final IndexContext indexContext;

    public IndexSearcher(Segment segment, IndexContext indexContext)
    {
        this.keyFetcher = segment.keyFetcher;
        this.indexFiles = segment.indexFiles;
        this.metadata = segment.metadata;
        this.indexContext = indexContext;
        this.primaryKeyMap = segment.primaryKeyMap;
    }

    public static IndexSearcher open(Segment segment, IndexContext columnContext) throws IOException
    {
        return segment.indexFiles.indexDescriptor.version.onDiskFormat().newIndexSearcher(segment, columnContext);
    }

    /**
     * @return number of per-index open files attached to a sstable
     */
    // TODO: needs to be per OnDiskFormat
    public static int openPerIndexFiles(AbstractType<?> columnType)
    {
        return TypeUtil.isLiteral(columnType) ? InvertedIndexSearcher.openPerIndexFiles() : KDTreeIndexSearcher.openPerIndexFiles();
    }

    /**
     * @return memory usage of underlying on-disk data structure
     */
    public abstract long indexFileCacheSize();

    /**
     * Search on-disk index synchronously.
     *
     * @param expression to filter on disk index
     * @param queryContext to track per sstable cache and per query metrics
     *
     * @return {@link RangeIterator} that matches given expression
     */
    public abstract List<RangeIterator> search(Expression expression, SSTableQueryContext queryContext) throws IOException;

    public List<RangeIterator> toIterators(List<PostingList.PeekablePostingList> postingLists, SSTableQueryContext queryContext) throws IOException
    {
        if (postingLists == null || postingLists.isEmpty())
            return Collections.EMPTY_LIST;

        List<RangeIterator> iterators = new ArrayList<>();

        for (PostingList.PeekablePostingList postingList : postingLists)
        {
            SearcherContext searcherContext = new SearcherContext(queryContext, postingList);
            if (!searcherContext.noOverlap)
            {
                RangeIterator iterator = new PostingListRangeIterator(indexContext, searcherContext);
                iterators.add(iterator);
            }
        }
        return iterators;
    }

    public class SearcherContext
    {
        PrimaryKey minimumKey;
        PrimaryKey maximumKey;
        long maxPartitionOffset;
        boolean noOverlap;
        final SSTableQueryContext context;
        final PostingList.PeekablePostingList postingList;
        final PrimaryKeyMap primaryKeyMap;

        public SearcherContext(SSTableQueryContext context, PostingList.PeekablePostingList postingList) throws IOException
        {
            this.context = context;
            this.postingList = postingList;
            this.primaryKeyMap = IndexSearcher.this.primaryKeyMap;

            minimumKey = this.primaryKeyMap.primaryKeyFromRowId(postingList.peek());

            // use segment's metadata for the range iterator, may not be accurate, but should not matter to performance.
            maximumKey = metadata.maxKey;

            maxPartitionOffset = Long.MAX_VALUE;
        }

        long count()
        {
            return postingList.size();
        }
    }
}
