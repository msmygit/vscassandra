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
package org.apache.cassandra.db.compaction;

import java.util.*;
import java.util.function.LongPredicate;

import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.io.sstable.RowIndexEntry;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.schema.CompactionParams.TombstoneOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.*;
import org.apache.cassandra.utils.OverlapIterator;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.db.lifecycle.SSTableIntervalTree.buildIntervals;

/**
 * Manage compaction options.
 */
public class CompactionController implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionController.class);
    static final boolean NEVER_PURGE_TOMBSTONES = Boolean.getBoolean("cassandra.never_purge_tombstones");

    public final ColumnFamilyStore cfs;
    private final boolean compactingRepaired;
    // note that overlapIterator and overlappingSSTables will be null if NEVER_PURGE_TOMBSTONES is set - this is a
    // good thing so that noone starts using them and thinks that if overlappingSSTables is empty, there
    // is no overlap.
    private Refs<SSTableReader> overlappingSSTables;
    private OverlapIterator<PartitionPosition, SSTableReader> overlapIterator;
    private final Iterable<SSTableReader> compacting;
    private final boolean ignoreOverlaps;
    private final RateLimiter limiter;
    private final long minTimestamp;
    final TombstoneOption tombstoneOption;
    final Map<SSTableReader, FileDataInput> openDataFiles = new HashMap<>();

    public final int gcBefore;

    protected CompactionController(ColumnFamilyStore cfs, int maxValue)
    {
        this(cfs, null, maxValue);
    }

    public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore)
    {
        this(cfs, compacting, gcBefore, false);
    }

    public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore, boolean ignoreOverlaps)
    {
        this(cfs, compacting, gcBefore, null,
             cfs.getCompactionStrategyManager().getCompactionParams().tombstoneOption(), ignoreOverlaps);
    }

    public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore, RateLimiter limiter, TombstoneOption tombstoneOption)
    {
        this(cfs, compacting, gcBefore, limiter, tombstoneOption, false);
    }

    public CompactionController(ColumnFamilyStore cfs, Set<SSTableReader> compacting, int gcBefore, RateLimiter limiter, TombstoneOption tombstoneOption, boolean ignoreOverlaps)
    {
        assert cfs != null;
        this.cfs = cfs;
        this.gcBefore = gcBefore;
        this.compacting = compacting;
        this.limiter = limiter;
        compactingRepaired = compacting != null && compacting.stream().allMatch(SSTableReader::isRepaired);
        this.tombstoneOption = tombstoneOption;
        this.minTimestamp = compacting != null && !compacting.isEmpty()       // check needed for test
                          ? compacting.stream().mapToLong(SSTableReader::getMinTimestamp).min().getAsLong()
                          : 0;
        this.ignoreOverlaps = ignoreOverlaps;
        refreshOverlaps();
        if (NEVER_PURGE_TOMBSTONES)
            logger.warn("You are running with -Dcassandra.never_purge_tombstones=true, this is dangerous!");
    }

    public void maybeRefreshOverlaps()
    {
        if (NEVER_PURGE_TOMBSTONES)
        {
            logger.trace("not refreshing overlaps - running with -Dcassandra.never_purge_tombstones=true");
            return;
        }

        if (ignoreOverlaps())
        {
            logger.trace("not refreshing overlaps - running with ignoreOverlaps activated");
            return;
        }

        for (SSTableReader reader : overlappingSSTables)
        {
            if (reader.isMarkedCompacted())
            {
                refreshOverlaps();
                return;
            }
        }
    }

    private void refreshOverlaps()
    {
        if (NEVER_PURGE_TOMBSTONES || ignoreOverlaps())
            return;

        if (this.overlappingSSTables != null)
            close();

        if (compacting == null)
            overlappingSSTables = Refs.tryRef(Collections.<SSTableReader>emptyList());
        else
            overlappingSSTables = cfs.getAndReferenceOverlappingLiveSSTables(compacting);
        this.overlapIterator = new OverlapIterator<>(buildIntervals(overlappingSSTables));
    }

    public Set<SSTableReader> getFullyExpiredSSTables()
    {
        return getFullyExpiredSSTables(cfs, compacting, overlappingSSTables, gcBefore, ignoreOverlaps());
    }

    public static Set<SSTableReader> getFullyExpiredSSTables(ColumnFamilyStore cfStore, Iterable<SSTableReader> compacting, Iterable<SSTableReader> overlapping, int gcBefore)
    {
        return getFullyExpiredSSTables(cfStore, compacting, overlapping, gcBefore, false);
    }

    /**
     * Finds expired sstables
     *
     * works something like this;
     * 1. find "global" minTimestamp of overlapping sstables, compacting sstables and memtables containing any non-expired data
     * 2. build a list of fully expired candidates
     * 3. check if the candidates to be dropped actually can be dropped {@code (maxTimestamp < global minTimestamp)}
     *    - if not droppable, remove from candidates
     * 4. return candidates.
     *
     * @param cfStore current cf store
     * @param compacting we take the drop-candidates from this set, it is usually the sstables included in the compaction
     * @param overlappingByKey the sstables that overlap the ones in compacting in terms of key ranges.
     * @param gcBefore time period to consider sstables fully expired
     * @param ignoreOverlaps whether or not to ignore the overlaps (both time and key range)
     * @return fully expired sstables
     */
    public static Set<SSTableReader> getFullyExpiredSSTables(ColumnFamilyStore cfStore, Iterable<SSTableReader> compacting, Iterable<SSTableReader> overlappingByKey, int gcBefore, boolean ignoreOverlaps)
    {
        logger.trace("Checking droppable sstables in {}", cfStore);

        if (NEVER_PURGE_TOMBSTONES || compacting == null)
            return Collections.<SSTableReader>emptySet();

        if (cfStore.getCompactionStrategyManager().onlyPurgeRepairedTombstones() && !Iterables.all(compacting, SSTableReader::isRepaired))
            return Collections.emptySet();

        Set<SSTableReader> candidates = new HashSet<>();
        long minTimestamp = Long.MAX_VALUE;

        for (SSTableReader candidate : compacting)
        {
            if (candidate.getSSTableMetadata().maxLocalDeletionTime < gcBefore)
                candidates.add(candidate);
            else
                minTimestamp = Math.min(minTimestamp, candidate.getMinTimestamp());
        }

        // When ignoring overlaps, we judge solely based on the fact whether or not the sstable has to be expired
        if (ignoreOverlaps)
            return candidates;

        for (SSTableReader sstable : overlappingByKey)
        {
            // Overlapping might include fully expired sstables. What we care about here is
            // the min timestamp of the overlapping sstables that actually contain live data.
            if (sstable.getSSTableMetadata().maxLocalDeletionTime >= gcBefore)
                minTimestamp = Math.min(minTimestamp, sstable.getMinTimestamp());
        }

        for (Memtable memtable : cfStore.getTracker().getView().getAllMemtables())
            minTimestamp = Math.min(minTimestamp, memtable.getMinTimestamp());

        // At this point, minTimestamp denotes the lowest timestamp of any relevant
        // SSTable or Memtable that contains a constructive value. candidates contains all the
        // candidates with no constructive values. The ones out of these that have
        // (getMaxTimestamp() < minTimestamp) serve no purpose anymore.

        Iterator<SSTableReader> iterator = candidates.iterator();
        while (iterator.hasNext())
        {
            SSTableReader candidate = iterator.next();
            if (candidate.getMaxTimestamp() >= minTimestamp)
            {
                iterator.remove();
            }
            else
            {
               logger.trace("Dropping expired SSTable {} (maxLocalDeletionTime={}, gcBefore={})",
                        candidate, candidate.getSSTableMetadata().maxLocalDeletionTime, gcBefore);
            }
        }
        return candidates;
    }

    public String getKeyspace()
    {
        return cfs.keyspace.getName();
    }

    public String getColumnFamily()
    {
        return cfs.name;
    }

    /**
     * @param key
     * @return a predicate for whether tombstones marked for deletion at the given time for the given partition are
     * purgeable; we calculate this by checking whether the deletion time is less than the min timestamp of all SSTables
     * containing his partition and not participating in the compaction. This means there isn't any data in those
     * sstables that might still need to be suppressed by a tombstone at this timestamp.
     */
    public LongPredicate getPurgeEvaluator(DecoratedKey key)
    {
        if (NEVER_PURGE_TOMBSTONES || !compactingRepaired())
            return time -> false;

        overlapIterator.update(key);
        Set<SSTableReader> filteredSSTables = overlapIterator.overlaps();
        Iterable<Memtable> memtables = cfs.getTracker().getView().getAllMemtables();
        long minTimestampSeen = Long.MAX_VALUE;
        boolean hasTimestamp = false;

        for (SSTableReader sstable: filteredSSTables)
        {
            // if we don't have bloom filter(bf_fp_chance=1.0 or filter file is missing),
            // we check index file instead.
            if (sstable.couldContain(key))
            {
                minTimestampSeen = Math.min(minTimestampSeen, sstable.getMinTimestamp());
                hasTimestamp = true;
            }
        }

        for (Memtable memtable : memtables)
        {
            try
            {
                Partition partition = memtable.getPartition(key).blockingLast(null);
                if (partition != null)
                {
                    minTimestampSeen = Math.min(minTimestampSeen, partition.stats().minTimestamp);
                    hasTimestamp = true;
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        if (!hasTimestamp)
            return time -> true;
        else
        {
            final long finalTimestamp = minTimestampSeen;
            return time -> time < finalTimestamp;
        }
    }

    public void close()
    {
        if (overlappingSSTables != null)
            overlappingSSTables.release();

        FileUtils.closeQuietly(openDataFiles.values());
        openDataFiles.clear();
    }

    public boolean compactingRepaired()
    {
        return !cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones() || compactingRepaired;
    }

    boolean provideTombstoneSources()
    {
        return tombstoneOption != TombstoneOption.NONE;
    }

    // caller must close iterators
    public Iterable<UnfilteredRowIterator> shadowSources(DecoratedKey key, boolean tombstoneOnly)
    {
        if (!provideTombstoneSources() || !compactingRepaired() || NEVER_PURGE_TOMBSTONES)
            return null;
        overlapIterator.update(key);
        return Iterables.filter(Iterables.transform(overlapIterator.overlaps(),
                                                    reader -> getShadowIterator(reader, key, tombstoneOnly)),
                                Predicates.notNull());
    }

    @SuppressWarnings("resource") // caller to close
    private UnfilteredRowIterator getShadowIterator(SSTableReader reader, DecoratedKey key, boolean tombstoneOnly)
    {
        if (reader.isMarkedSuspect() ||
            reader.getMaxTimestamp() <= minTimestamp ||
            tombstoneOnly && !reader.mayHaveTombstones())
            return null;
        RowIndexEntry position = reader.getExactPosition(key, Rebufferer.ReaderConstraint.NONE);
        if (position == null)
            return null;
        FileDataInput dfile = openDataFiles.computeIfAbsent(reader, this::openDataFile);
        return reader.simpleIterator(dfile, key, position, tombstoneOnly);
    }

    private FileDataInput openDataFile(SSTableReader reader)
    {
        return limiter != null ? reader.openDataReader(limiter) : reader.openDataReader();
    }

    /**
     * Is overlapped sstables ignored
     *
     * Control whether or not we are taking into account overlapping sstables when looking for fully expired sstables.
     * In order to reduce the amount of work needed, we look for sstables that can be dropped instead of compacted.
     * As a safeguard mechanism, for each time range of data in a sstable, we are checking globally to see if all data
     * of this time range is fully expired before considering to drop the sstable.
     * This strategy can retain for a long time a lot of sstables on disk (see CASSANDRA-13418) so this option
     * control whether or not this check should be ignored.
     *
     * @return false by default
     */
    protected boolean ignoreOverlaps()
    {
        return ignoreOverlaps;
    }

    public Set<SSTableReader> getCompacting()
    {
        return compacting != null ? Sets.newHashSet(compacting) : Collections.emptySet();
    }
}
