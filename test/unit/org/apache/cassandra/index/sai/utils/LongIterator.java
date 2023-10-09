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
package org.apache.cassandra.index.sai.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.LongFunction;

import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.io.sstable.SequenceBasedSSTableId;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LongIterator extends RangeIterator
{
    private final List<PrimaryKey> keys;
    private int currentIdx = 0;

    private static long tokenToRowIdShift(long rowId)
    {
        return rowId + 100;
    }

    /**
     * whether LongIterator should throw exception during iteration.
     */
    private boolean shouldThrow = false;
    private final Random random = new Random();

    public LongIterator(long[] tokens)
    {
        this(tokens, 0);
    }

    public LongIterator(long[] tokens, int sstableId)
    {
        this(tokens, sstableId, LongIterator::tokenToRowIdShift);
    }

    public LongIterator(long[] tokens, int sstableId, LongFunction<Long> toOffset)
    {
        super(tokens.length == 0 ? null : fromToken(tokens[0]), tokens.length == 0 ? null : fromToken(tokens[tokens.length - 1]), tokens.length);

        this.keys = new ArrayList<>(tokens.length);
        for (long token : tokens)
            this.keys.add(fromTokenAndRowId(token, sstableId, toOffset.apply(token)));
    }

    public LongIterator throwsException()
    {
        this.shouldThrow = true;
        return this;
    }

    @Override
    protected PrimaryKey computeNext()
    {
        // throws exception if it's last element or chosen 1 out of n
        if (shouldThrow && (currentIdx >= keys.size() - 1 || random.nextInt(keys.size()) == 0))
            throw new RuntimeException("injected exception");

        if (currentIdx >= keys.size())
            return endOfData();

        return keys.get(currentIdx++);
    }

    @Override
    protected void performSkipTo(PrimaryKey nextToken)
    {
        for (int i = currentIdx == 0 ? 0 : currentIdx - 1; i < keys.size(); i++)
        {
            PrimaryKey token = keys.get(i);
            if (token.compareTo(nextToken) >= 0)
            {
                currentIdx = i;
                break;
            }
        }
    }

    @Override
    public void close()
    {}

    public static PrimaryKey fromToken(long token)
    {
        return SAITester.TEST_FACTORY.createTokenOnly(new Murmur3Partitioner.LongToken(token));
    }

    /**
     * A convenience method to assert that the given iterator contains the expected tokens and sstable id/row id pairs.
     * @param expectedMap a map from token id to a list of sstable ids associated with each token.
     * @param actual the iterator to be validated
     */
    public static void assertEqual(Map<Long, List<Integer>> expectedMap, RangeIterator actual)
    {
        var expected = expectedMap.entrySet().iterator();
        while (actual.hasNext())
        {
            assertTrue(expected.hasNext());
            PrimaryKey next = actual.next();
            Map.Entry<Long, List<Integer>> entry = expected.next();
            long expectedToken = entry.getKey();
            assertEquals(expectedToken, next.token().getLongValue());
            for (int sstableId : entry.getValue())
            {
                long rowId = tokenToRowIdShift(expectedToken);
                assertEquals(rowId, next.sstableRowId(new SequenceBasedSSTableId(sstableId)));
            }
        }
        assertFalse(expected.hasNext());
    }

    public static void updateExpectedMap(Map<Long, List<Integer>> expectedMap, long[] tokens, List<Integer> sstableIds)
    {
        for (int i = 0; i < tokens.length; i++)
        {
            long token = tokens[i];
            var overwritten = expectedMap.put(token, sstableIds);
            // Test is not meant to overwrite values since that might be confusing
            assertNull(overwritten);
        }
    }

    public static List<Long> convert(RangeIterator tokens)
    {
        List<Long> results = new ArrayList<>();
        while (tokens.hasNext())
            results.add(tokens.next().token().getLongValue());

        return results;
    }

    public static List<Long> convert(final long... nums)
    {
        return new ArrayList<Long>(nums.length)
        {{
            for (long n : nums)
                add(n);
        }};
    }

    private PrimaryKey fromTokenAndRowId(long token, int sstableId, long rowId)
    {
        DecoratedKey key = new BufferDecoratedKey(new Murmur3Partitioner.LongToken(token), ByteBufferUtil.bytes(token));
        return SAITester.TEST_FACTORY.create(key, Clustering.EMPTY, new SequenceBasedSSTableId(sstableId), rowId);
    }
}
