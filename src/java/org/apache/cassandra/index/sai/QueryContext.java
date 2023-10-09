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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import io.github.jbellis.jvector.util.Bits;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.vector.CassandraOnHeapGraph;
import org.apache.cassandra.index.sai.disk.vector.JVectorLuceneOnDiskGraph;
import org.apache.cassandra.index.sai.utils.AbortedOperationException;
import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * Tracks state relevant to the execution of a single query, including metrics and timeout monitoring.
 *
 * Fields here are non-volatile, as they are accessed from a single thread.
 */
@NotThreadSafe
public class QueryContext
{
    private static final boolean DISABLE_TIMEOUT = Boolean.getBoolean("cassandra.sai.test.disable.timeout");

    protected final long queryStartTimeNanos;

    public final long executionQuotaNano;

    private long sstablesHit = 0;
    private long segmentsHit = 0;
    private long partitionsRead = 0;
    private long rowsFiltered = 0;

    private long trieSegmentsHit = 0;

    private long bkdPostingListsHit = 0;
    private long bkdSegmentsHit = 0;

    private long bkdPostingsSkips = 0;
    private long bkdPostingsDecodes = 0;

    private long triePostingsSkips = 0;
    private long triePostingsDecodes = 0;

    private long tokenSkippingCacheHits = 0;
    private long tokenSkippingLookups = 0;

    private long queryTimeouts = 0;

    private long hnswVectorsAccessed;
    private long hnswVectorCacheHits;

    private NavigableSet<PrimaryKey> shadowedPrimaryKeys; // allocate when needed

    @VisibleForTesting
    public QueryContext()
    {
        this(DatabaseDescriptor.getRangeRpcTimeout(TimeUnit.MILLISECONDS));
    }

    public QueryContext(long executionQuotaMs)
    {
        this(TimeUnit.MILLISECONDS.toNanos(executionQuotaMs), System.nanoTime());
    }

    public QueryContext(long executionQuotaNanos, long queryStartTimeNanos)
    {
        this.executionQuotaNano = executionQuotaNanos;
        this.queryStartTimeNanos = queryStartTimeNanos;
    }

    public void addFrom(QueryContext other)
    {
        addSstablesHit(other.sstablesHit());
        addSegmentsHit(other.segmentsHit());
        addPartitionsRead(other.partitionsRead());
        addRowsFiltered(other.rowsFiltered());
        addTrieSegmentsHit(other.trieSegmentsHit());
        addBkdPostingListsHit(other.bkdPostingListsHit());
        addBkdSegmentsHit(other.bkdSegmentsHit());
        addBkdPostingsSkips(other.bkdPostingsSkips());
        addBkdPostingsDecodes(other.bkdPostingsDecodes());
        addTriePostingsSkips(other.triePostingsSkips());
        addTriePostingsDecodes(other.triePostingsDecodes());
        addTokenSkippingCacheHits(other.tokenSkippingCacheHits());
        addTokenSkippingLookups(other.tokenSkippingLookups());
        addQueryTimeouts(other.queryTimeouts());
        addHnswVectorsAccessed(other.hnswVectorsAccessed());
        addHnswVectorCacheHits(other.hnswVectorCacheHits());

        var shadowed = other.getShadowedPrimaryKeys();
        if (!shadowed.isEmpty()) {
            shadowed.forEach(this::recordShadowedPrimaryKey);
        }
    }

    public long totalQueryTimeNs()
    {
        return System.nanoTime() - queryStartTimeNanos;
    }

    // setters
    public void addSstablesHit(long val)
    {
        sstablesHit += val;
    }
    public void addSegmentsHit(long val) {
        segmentsHit += val;
    }
    public void addPartitionsRead(long val)
    {
        partitionsRead += val;
    }
    public void addRowsFiltered(long val)
    {
        rowsFiltered += val;
    }
    public void addTrieSegmentsHit(long val)
    {
        trieSegmentsHit += val;
    }
    public void addBkdPostingListsHit(long val)
    {
        bkdPostingListsHit += val;
    }
    public void addBkdSegmentsHit(long val)
    {
        bkdSegmentsHit += val;
    }
    public void addBkdPostingsSkips(long val)
    {
        bkdPostingsSkips += val;
    }
    public void addBkdPostingsDecodes(long val)
    {
        bkdPostingsDecodes += val;
    }
    public void addTriePostingsSkips(long val)
    {
        triePostingsSkips += val;
    }
    public void addTriePostingsDecodes(long val)
    {
        triePostingsDecodes += val;
    }

    public void addTokenSkippingCacheHits(long val)
    {
        tokenSkippingCacheHits += val;
    }
    public void addTokenSkippingLookups(long val)
    {
        tokenSkippingLookups += val;
    }
    public void addQueryTimeouts(long val)
    {
        queryTimeouts += val;
    }
    public void addHnswVectorsAccessed(long val)
    {
        hnswVectorsAccessed += val;
    }
    public void addHnswVectorCacheHits(long val)
    {
        hnswVectorCacheHits += val;
    }
    
    // getters

    public long sstablesHit()
    {
        return sstablesHit;
    }

    public long segmentsHit() {
        return segmentsHit;
    }
    public long partitionsRead()
    {
        return partitionsRead;
    }
    public long rowsFiltered()
    {
        return rowsFiltered;
    }

    public long trieSegmentsHit()
    {
        return trieSegmentsHit;
    }

    public long bkdPostingListsHit()
    {
        return bkdPostingListsHit;
    }
    public long bkdSegmentsHit()
    {
        return bkdSegmentsHit;
    }
    public long bkdPostingsSkips()
    {
        return bkdPostingsSkips;
    }
    public long bkdPostingsDecodes()
    {
        return bkdPostingsDecodes;
    }
    public long triePostingsSkips()
    {
        return triePostingsSkips;
    }
    public long triePostingsDecodes()
    {
        return triePostingsDecodes;
    }
    public long tokenSkippingCacheHits()
    {
        return tokenSkippingCacheHits;
    }
    public long tokenSkippingLookups()
    {
        return tokenSkippingLookups;
    }

    public long queryTimeouts()
    {
        return queryTimeouts;
    }

    public long hnswVectorsAccessed()
    {
        return hnswVectorsAccessed;
    }
    public long hnswVectorCacheHits()
    {
        return hnswVectorCacheHits;
    }
    
    public void checkpoint()
    {
        if (totalQueryTimeNs() >= executionQuotaNano && !DISABLE_TIMEOUT)
        {
            addQueryTimeouts(1);
            throw new AbortedOperationException();
        }
    }

    public void recordShadowedPrimaryKey(PrimaryKey primaryKey)
    {
        if (shadowedPrimaryKeys == null)
            shadowedPrimaryKeys = new TreeSet<>();
        shadowedPrimaryKeys.add(primaryKey);
    }

    // Returns true if the row ID will be included or false if the row ID will be shadowed
    public boolean shouldInclude(long sstableRowId, PrimaryKeyMap primaryKeyMap)
    {
        return shadowedPrimaryKeys == null || !shadowedPrimaryKeys.contains(primaryKeyMap.primaryKeyFromRowId(sstableRowId));
    }

    public boolean containsShadowedPrimaryKey(PrimaryKey primaryKey)
    {
        return shadowedPrimaryKeys != null && shadowedPrimaryKeys.contains(primaryKey);
    }

    /**
     * @return shadowed primary keys, in ascending order
     */
    public NavigableSet<PrimaryKey> getShadowedPrimaryKeys()
    {
        if (shadowedPrimaryKeys == null)
            return Collections.emptyNavigableSet();
        return shadowedPrimaryKeys;
    }

    public Bits bitsetForShadowedPrimaryKeys(CassandraOnHeapGraph<PrimaryKey> graph)
    {
        if (getShadowedPrimaryKeys().isEmpty())
            return null;

        return new IgnoredKeysBits(graph, shadowedPrimaryKeys);
    }

    public Bits bitsetForShadowedPrimaryKeys(SegmentMetadata metadata, PrimaryKeyMap primaryKeyMap, JVectorLuceneOnDiskGraph graph) throws IOException
    {
        Set<Integer> ignoredOrdinals = null;
        try (var ordinalsView = graph.getOrdinalsView())
        {
            for (PrimaryKey primaryKey : getShadowedPrimaryKeys())
            {
                // not in current segment
                if (primaryKey.compareTo(metadata.minKey) < 0 || primaryKey.compareTo(metadata.maxKey) > 0)
                    continue;

                long sstableRowId = primaryKeyMap.exactRowIdForPrimaryKey(primaryKey);
                if (primaryKeyMap.isNotFound(sstableRowId)) // not found
                    continue;

                int segmentRowId = metadata.toSegmentRowId(sstableRowId);
                // not in segment yet
                if (segmentRowId < 0)
                    continue;
                // end of segment
                if (segmentRowId > metadata.maxSSTableRowId)
                    break;

                int ordinal = ordinalsView.getOrdinalForRowId(segmentRowId);
                if (ordinal >= 0)
                {
                    if (ignoredOrdinals == null)
                        ignoredOrdinals = new HashSet<>();
                    ignoredOrdinals.add(ordinal);
                }
            }
        }

        if (ignoredOrdinals == null)
            return null;

        return new IgnoringBits(ignoredOrdinals, graph.size());
    }

    private static class IgnoringBits implements Bits
    {
        private final Set<Integer> ignoredOrdinals;
        private final int maxOrdinal;

        public IgnoringBits(Set<Integer> ignoredOrdinals, int maxOrdinal)
        {
            this.ignoredOrdinals = ignoredOrdinals;
            this.maxOrdinal = maxOrdinal;
        }

        @Override
        public boolean get(int index)
        {
            return !ignoredOrdinals.contains(index);
        }

        @Override
        public int length()
        {
            return maxOrdinal;
        }
    }

    private static class IgnoredKeysBits implements Bits
    {
        private final CassandraOnHeapGraph<PrimaryKey> graph;
        private final NavigableSet<PrimaryKey> ignored;

        public IgnoredKeysBits(CassandraOnHeapGraph<PrimaryKey> graph, NavigableSet<PrimaryKey> ignored)
        {
            this.graph = graph;
            this.ignored = ignored;
        }

        @Override
        public boolean get(int ordinal)
        {
            var keys = graph.keysFromOrdinal(ordinal);
            return keys.stream().anyMatch(k -> !ignored.contains(k));
        }

        @Override
        public int length()
        {
            return graph.size();
        }
    }
}
