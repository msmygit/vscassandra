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

package org.apache.cassandra.index.sai;

import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 */
@NotThreadSafe
public class ConcurrentQueryContext extends QueryContext
{
    private final LongAdder sstablesHit = new LongAdder();
    private final LongAdder segmentsHit = new LongAdder();
    private final LongAdder partitionsRead = new LongAdder();
    private final LongAdder rowsFiltered = new LongAdder();

    private final LongAdder trieSegmentsHit = new LongAdder();

    private final LongAdder bkdPostingListsHit = new LongAdder();
    private final LongAdder bkdSegmentsHit = new LongAdder();

    private final LongAdder bkdPostingsSkips = new LongAdder();
    private final LongAdder bkdPostingsDecodes = new LongAdder();

    private final LongAdder triePostingsSkips = new LongAdder();
    private final LongAdder triePostingsDecodes = new LongAdder();

    private final LongAdder tokenSkippingCacheHits = new LongAdder();
    private final LongAdder tokenSkippingLookups = new LongAdder();

    private final LongAdder queryTimeouts = new LongAdder();

    private final LongAdder hnswVectorsAccessed = new LongAdder();
    private final LongAdder hnswVectorCacheHits = new LongAdder();

    private final NavigableSet<PrimaryKey> shadowedPrimaryKeys = new ConcurrentSkipListSet<>();

    @VisibleForTesting
    public ConcurrentQueryContext(QueryContext original)
    {
        super(original.executionQuotaNano, original.queryStartTimeNanos);
        addFrom(original);
    }
    
    @Override
    public void recordShadowedPrimaryKey(PrimaryKey primaryKey)
    {
        shadowedPrimaryKeys.add(primaryKey);
    }

    // Returns true if the row ID will be included or false if the row ID will be shadowed
    @Override
    public boolean shouldInclude(long sstableRowId, PrimaryKeyMap primaryKeyMap)
    {
        return !shadowedPrimaryKeys.contains(primaryKeyMap.primaryKeyFromRowId(sstableRowId));
    }

    @Override
    public boolean containsShadowedPrimaryKey(PrimaryKey primaryKey)
    {
        return shadowedPrimaryKeys.contains(primaryKey);
    }

    /**
     * @return shadowed primary keys, in ascending order
     */
    @Override
    public NavigableSet<PrimaryKey> getShadowedPrimaryKeys()
    {
        return shadowedPrimaryKeys;
    }


    // setters
    @Override
    public void addSstablesHit(long val)
    {
        sstablesHit.add(val);
    }
    @Override
    public void addSegmentsHit(long val) {
        segmentsHit.add(val);
    }
    @Override
    public void addPartitionsRead(long val)
    {
        partitionsRead.add(val);
    }
    @Override
    public void addRowsFiltered(long val)
    {
        rowsFiltered.add(val);
    }
    @Override
    public void addTrieSegmentsHit(long val)
    {
        trieSegmentsHit.add(val);
    }
    @Override
    public void addBkdPostingListsHit(long val)
    {
        bkdPostingListsHit.add(val);
    }
    @Override
    public void addBkdSegmentsHit(long val)
    {
        bkdSegmentsHit.add(val);
    }
    @Override
    public void addBkdPostingsSkips(long val)
    {
        bkdPostingsSkips.add(val);
    }
    @Override
    public void addBkdPostingsDecodes(long val)
    {
        bkdPostingsDecodes.add(val);
    }
    @Override
    public void addTriePostingsSkips(long val)
    {
        triePostingsSkips.add(val);
    }
    @Override
    public void addTriePostingsDecodes(long val)
    {
        triePostingsDecodes.add(val);
    }

    @Override
    public void addTokenSkippingCacheHits(long val)
    {
        tokenSkippingCacheHits.add(val);
    }
    @Override
    public void addTokenSkippingLookups(long val)
    {
        tokenSkippingLookups.add(val);
    }
    @Override
    public void addQueryTimeouts(long val)
    {
        queryTimeouts.add(val);
    }
    @Override
    public void addHnswVectorsAccessed(long val)
    {
        hnswVectorsAccessed.add(val);
    }
    @Override
    public void addHnswVectorCacheHits(long val)
    {
        hnswVectorCacheHits.add(val);
    }

    // getters

    @Override
    public long sstablesHit()
    {
        return sstablesHit.longValue();
    }

    @Override
    public long segmentsHit() {
        return segmentsHit.longValue();
    }
    @Override
    public long partitionsRead()
    {
        return partitionsRead.longValue();
    }
    @Override
    public long rowsFiltered()
    {
        return rowsFiltered.longValue();
    }
    @Override
    public long trieSegmentsHit()
    {
        return trieSegmentsHit.longValue();
    }
    @Override
    public long bkdPostingListsHit()
    {
        return bkdPostingListsHit.longValue();
    }
    @Override
    public long bkdSegmentsHit()
    {
        return bkdSegmentsHit.longValue();
    }
    @Override
    public long bkdPostingsSkips()
    {
        return bkdPostingsSkips.longValue();
    }
    @Override
    public long bkdPostingsDecodes()
    {
        return bkdPostingsDecodes.longValue();
    }
    @Override
    public long triePostingsSkips()
    {
        return triePostingsSkips.longValue();
    }
    @Override
    public long triePostingsDecodes()
    {
        return triePostingsDecodes.longValue();
    }
    @Override
    public long tokenSkippingCacheHits()
    {
        return tokenSkippingCacheHits.longValue();
    }
    @Override
    public long tokenSkippingLookups()
    {
        return tokenSkippingLookups.longValue();
    }
    @Override
    public long queryTimeouts()
    {
        return queryTimeouts.longValue();
    }
    @Override
    public long hnswVectorsAccessed()
    {
        return hnswVectorsAccessed.longValue();
    }
    @Override
    public long hnswVectorCacheHits()
    {
        return hnswVectorCacheHits.longValue();
    }
}
