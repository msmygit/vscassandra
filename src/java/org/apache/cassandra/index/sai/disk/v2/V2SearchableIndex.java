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

package org.apache.cassandra.index.sai.disk.v2;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.IndexSearcherContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PostingListRangeIterator;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

public class V2SearchableIndex extends SearchableIndex
{
    private V2PerIndexFiles indexFiles;

    private final Token minToken;
    private final Token maxToken;

    private final Token.KeyBound minKeyBound;
    private final Token.KeyBound maxKeyBound;

    final PrimaryKey.PrimaryKeyFactory keyFactory;
    final BlockIndexMeta meta;
    final BlockIndexReader reader;
    final IndexContext indexContext;
    final SSTableContext sstableContext;
    final PrimaryKey minKey, maxKey;
    final V2PrimaryKeyMap.V2PrimaryKeyMapFactory primaryKeyMapFactory;

    public V2SearchableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        this.indexContext = indexContext;
        this.sstableContext = sstableContext;
        try
        {
            keyFactory = indexContext.keyFactory();
            this.indexFiles = (V2PerIndexFiles) sstableContext.perIndexFiles(indexContext);

            meta = (BlockIndexMeta) sstableContext.indexDescriptor.newIndexMetadataSerializer().deserialize(sstableContext.indexDescriptor, indexContext);

            // load the min and max primary keys
            primaryKeyMapFactory = new V2PrimaryKeyMap.V2PrimaryKeyMapFactory(sstableContext.indexDescriptor);
            SSTableQueryContext ssTableQueryContext = new SSTableQueryContext(new QueryContext());
            final PrimaryKeyMap primaryKeyMap = primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(ssTableQueryContext);
            
            reader = new BlockIndexReader(sstableContext.indexDescriptor,
                                          indexContext.getIndexName(),
                                          meta,
                                          indexFiles);

            minKey = primaryKeyMap.primaryKeyFromRowId(meta.minRowID);
            maxKey = primaryKeyMap.primaryKeyFromRowId(meta.maxRowID);

            primaryKeyMap.close();

            this.minToken = minKey.partitionKey().getToken();
            this.maxToken = maxKey.partitionKey().getToken();
            this.minKeyBound = minToken.minKeyBound();
            this.maxKeyBound = maxToken.maxKeyBound();
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(indexFiles);
            FileUtils.closeQuietly(sstableContext);
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public long heapMemoryUsed()
    {
        return reader.heapMemoryUsed();
    }

    @Override
    public long getRowCount()
    {
        return meta.numRows;
    }

    @Override
    public long minSSTableRowId()
    {
        return meta.minRowID;
    }

    @Override
    public long maxSSTableRowId()
    {
        return meta.maxRowID;
    }

    @Override
    public ByteBuffer minTerm()
    {
        return ByteBuffer.wrap(meta.minTerm.bytes, meta.minTerm.offset, meta.minTerm.length);
    }

    @Override
    public ByteBuffer maxTerm()
    {
        return ByteBuffer.wrap(meta.maxTerm.bytes, meta.maxTerm.offset, meta.maxTerm.length);
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
    public List<RangeIterator> search(Expression expression,
                                      AbstractBounds<PartitionPosition> keyRange,
                                      SSTableQueryContext context) throws IOException
    {
        final ByteBuffer lower = expression.lower.value.encoded;
        final ByteBuffer upper = expression.upper != null ? expression.upper.value.encoded : null;

        if (intersects(keyRange))
        {
            ByteSource lowerSource = this.indexContext.getValidator().asComparableBytes(lower.duplicate(), ByteComparable.Version.OSS41);
            ByteSource upperSource = upper != null ? this.indexContext.getValidator().asComparableBytes(upper.duplicate(), ByteComparable.Version.OSS41) : null;

            byte[] lowerBytes = ByteSourceInverse.readBytes(lowerSource);
            byte[] upperBytes = upperSource != null ? ByteSourceInverse.readBytes(upperSource) : null;

            boolean upperExclusive = expression.upper != null ? !expression.upper.inclusive : false;

            if (expression.getOp().equals(Expression.Op.PREFIX))
            {
                // there's an extra byte with LIKE prefix
                lowerBytes = Arrays.copyOf(lowerBytes, lowerBytes.length - 1);
                upperBytes = Arrays.copyOf(upperBytes, upperBytes.length - 1);

                // TODO: +10 needs to be replaced with the diff of the actual max length of the index values
                byte[] upperBytesBig = Arrays.copyOf(upperBytes, upperBytes.length + 10);
                // set the greater bytes to the highest byte value to match
                for (int x = upperBytes.length; x < upperBytesBig.length; x++)
                {
                    upperBytesBig[x] = 127; // max byte?
                }

                upperBytes = upperBytesBig;

                upperExclusive = false;
            }

            final List<PostingList.PeekablePostingList> postingLists = reader.traverse(ByteComparable.fixedLength(lowerBytes),
                                                                                       !expression.lower.inclusive,
                                                                                       upperBytes != null ? ByteComparable.fixedLength(upperBytes) : null,
                                                                                       upperExclusive);

            return toIterators(postingLists, context);
        }
        return Collections.emptyList();
    }

    List<RangeIterator> toIterators(List<PostingList.PeekablePostingList> postingLists, SSTableQueryContext queryContext) throws IOException
    {
        if (postingLists == null || postingLists.isEmpty())
            return Collections.emptyList();

        List<RangeIterator> iterators = new ArrayList<>();

        for (PostingList.PeekablePostingList postingList : postingLists)
        {
            if (postingList.size() == 0 || postingList.peek() == PostingList.END_OF_STREAM)
            {
                continue;
            }
            IndexSearcherContext searcherContext = new IndexSearcherContext(minKey,
                                                                            maxKey,
                                                                            queryContext,
                                                                            postingList);

            iterators.add(new PostingListRangeIterator(indexContext, searcherContext));
        }

        return iterators;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(indexFiles, reader, primaryKeyMapFactory);
    }

    /**
     * @return true if current segment intersects with query key range
     */
    private boolean intersects(AbstractBounds<PartitionPosition> keyRange)
    {
        if (keyRange instanceof Range && ((Range<?>) keyRange).isWrapAround())
            return keyRange.contains(minKeyBound) || keyRange.contains(maxKeyBound);

        int cmp = keyRange.right.getToken().compareTo(minToken);
        // if right is minimum, it means right is the max token and bigger than maxKey.
        // if right bound is less than minKey, no intersection
        if (!keyRange.right.isMinimum() && (!keyRange.inclusiveRight() && cmp == 0 || cmp < 0))
            return false;

        cmp = keyRange.left.getToken().compareTo(maxToken);
        // if left bound is bigger than maxKey, no intersection
        if (!keyRange.isStartInclusive() && cmp == 0 || cmp > 0)
            return false;

        return true;
    }
}
