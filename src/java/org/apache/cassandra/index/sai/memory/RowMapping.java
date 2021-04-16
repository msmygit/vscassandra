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
package org.apache.cassandra.index.sai.memory;

import java.util.Collections;
import java.util.Iterator;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.index.sai.utils.AbstractIterator;
import org.apache.cassandra.index.sai.utils.SortedRow;
import org.apache.cassandra.index.sai.utils.SortedRows;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * In memory representation of {@link SortedRow} to row ID mappings which only contains
 * {@link Row} regardless it's live or deleted. ({@link RangeTombstoneMarker} is not included.)
 *
 * For JBOD, we can make use of sstable min/max partition key to filter irrelevant {@link MemtableIndex} subranges.
 * For Tiered Storage, in most cases, it flushes to tiered 0.
 */
public class RowMapping
{
    public static final RowMapping DUMMY = new RowMapping()
    {
        @Override
        public Iterator<Pair<ByteComparable, LongArrayList>> merge(MemtableIndex index) { return Collections.emptyIterator(); }

        @Override
        public void complete() {}

        @Override
        public void add(SortedRow key) {}
    };

    private final MemtableTrie<Long> rowMapping = new MemtableTrie<>(BufferType.OFF_HEAP);

    private volatile boolean complete = false;

    public SortedRow minKey;
    public SortedRow maxKey;

    public long maxSegmentRowId = -1;

    private RowMapping()
    {}

    /**
     * Create row mapping for FLUSH operation only.
     */
    public static RowMapping create(OperationType opType)
    {
        if (opType == OperationType.FLUSH)
            return new RowMapping();
        return DUMMY;
    }

    /**
     * Merge IndexMemtable(index term to PrimaryKeys mappings) with row mapping of a sstable
     * (PrimaryKey to RowId mappings).
     *
     * @param index a Memtable-attached column index
     *
     * @return iterator of index term to postings mapping exists in the sstable
     */
    public Iterator<Pair<ByteComparable, LongArrayList>> merge(MemtableIndex index)
    {
        assert complete : "RowMapping is not built.";

        Iterator<Pair<ByteComparable, SortedRows>> iterator = index.iterator();
        return new AbstractIterator<Pair<ByteComparable, LongArrayList>>()
        {
            @Override
            protected Pair<ByteComparable, LongArrayList> computeNext()
            {
                while (iterator.hasNext())
                {
                    Pair<ByteComparable, SortedRows> pair = iterator.next();

                    LongArrayList postings = null;
                    Iterator<SortedRow> primaryKeys = pair.right.iterator();

                    while (primaryKeys.hasNext())
                    {
                        SortedRow sortedRow = primaryKeys.next();
                        ByteComparable byteComparable = v -> sortedRow.primaryKeyAsComparableBytes(v);
                        Long segmentRowId = rowMapping.get(byteComparable);

                        if (segmentRowId != null)
                        {
                            postings = postings == null ? new LongArrayList() : postings;
                            postings.add(segmentRowId);
                        }
                    }
                    if (postings != null && !postings.isEmpty())
                        return Pair.create(pair.left, postings);
                }
                return endOfData();
            }
        };
    }

    /**
     * Complete building in memory RowMapping, mark it as immutable.
     */
    public void complete()
    {
        assert !complete : "RowMapping can only be built once.";
        this.complete = true;
    }

    /**
     * Include PrimaryKey to RowId mapping
     */
    public void add(SortedRow key) throws MemtableTrie.SpaceExhaustedException
    {
        assert !complete : "Cannot modify built RowMapping.";

        ByteComparable byteComparable = v -> key.primaryKeyAsComparableBytes(v);
        rowMapping.apply(Trie.singleton(byteComparable, key.sstableRowId()), (existing, neww) -> neww);

        maxSegmentRowId = Math.max(maxSegmentRowId, key.sstableRowId());

        // data is written in token sorted order
        if (minKey == null)
            minKey = key;
        maxKey = key;
    }

    public boolean hasRows()
    {
        return maxSegmentRowId >= 0;
    }
}
