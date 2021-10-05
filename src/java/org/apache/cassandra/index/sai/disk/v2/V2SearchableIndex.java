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
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexReader;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Throwables;

public class V2SearchableIndex extends SearchableIndex
{
    private BlockIndexFileProvider fileProvider;

    private final BlockIndexMeta metadata;
    private final BlockIndexReader reader;

    private final ByteBuffer minTerm;
    private final ByteBuffer maxTerm;
    private final PrimaryKey minKey;
    private final PrimaryKey maxKey;
    private final Token minToken;
    private final Token maxToken;

    private final Token.KeyBound minKeyBound;
    private final Token.KeyBound maxKeyBound;

    public V2SearchableIndex(SSTableContext sstableContext, IndexContext indexContext)
    {
        try
        {
            this.fileProvider = new PerIndexFileProvider(sstableContext.indexDescriptor, indexContext);

            this.metadata = new BlockIndexMeta(fileProvider.openMetadataInput());

            this.reader = new BlockIndexReader(fileProvider, false, metadata);

            PrimaryKeyMap primaryKeyMap = sstableContext.primaryKeyMapFactory.newPerSSTablePrimaryKeyMap(null);

            this.minTerm = ByteBuffer.wrap(metadata.minTerm.bytes, metadata.minTerm.offset, metadata.minTerm.length);
            this.maxTerm = ByteBuffer.wrap(metadata.maxTerm.bytes, metadata.maxTerm.offset, metadata.maxTerm.length);
            this.minKey = primaryKeyMap.primaryKeyFromRowId(metadata.minRowID);
            this.maxKey = primaryKeyMap.primaryKeyFromRowId(metadata.maxRowID);
            this.minToken = minKey.partitionKey().getToken();
            this.maxToken = maxKey.partitionKey().getToken();
            this.minKeyBound = minToken.minKeyBound();
            this.maxKeyBound = maxToken.maxKeyBound();
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(fileProvider);
            FileUtils.closeQuietly(sstableContext);
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public long indexFileCacheSize()
    {
        //TODO Need to work this one out
        return 0;
    }

    @Override
    public long getRowCount()
    {
        return metadata.numRows;
    }

    @Override
    public long minSSTableRowId()
    {
        return metadata.minRowID;
    }

    @Override
    public long maxSSTableRowId()
    {
        return metadata.maxRowID;
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
        if (intersects(keyRange))
        {
            return Collections.emptyList();
//            return Collections.singletonList(index.search(expression, context));
        }

        return Collections.emptyList();
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(fileProvider, reader);
    }

    /**
     * @return true if current segment intersects with query key range
     */
    private boolean intersects(AbstractBounds<PartitionPosition> keyRange)
    {
        if (keyRange instanceof Range && ((Range<?>)keyRange).isWrapAround())
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
