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

package org.apache.cassandra.index.sai.disk.v3;

import java.util.concurrent.TimeUnit;

import org.apache.cassandra.index.sai.metrics.ColumnQueryMetrics;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Combination of kdtree and trie index metrics for the version 3 index format that has both index types
 */
public class V3ColumnQueryMetrics extends ColumnQueryMetrics implements QueryEventListener.TrieIndexEventListener, QueryEventListener.BKDIndexEventListener
{
    private final BKDIndexMetrics bkdIndexMetrics;
    private final TrieIndexMetrics trieIndexMetrics;

    public V3ColumnQueryMetrics(String keyspace, String table, String indexName)
    {
        super(keyspace, table, indexName);

        bkdIndexMetrics = new BKDIndexMetrics(keyspace, table, indexName);
        trieIndexMetrics = new TrieIndexMetrics(keyspace, table, indexName);
    }

    @Override
    public void onIntersectionComplete(long intersectionTotalTime, TimeUnit unit)
    {
        bkdIndexMetrics.onIntersectionComplete(intersectionTotalTime, unit);
    }

    @Override
    public void onIntersectionEarlyExit()
    {
        bkdIndexMetrics.onIntersectionEarlyExit();
    }

    @Override
    public void postingListsHit(int count)
    {
        bkdIndexMetrics.postingListsHit(count);
    }

    @Override
    public void onSegmentHit()
    {
    }

    @Override
    public void onTraversalComplete(long traversalTotalTime, TimeUnit unit)
    {
        trieIndexMetrics.onTraversalComplete(traversalTotalTime, unit);
    }

    @Override
    public QueryEventListener.PostingListEventListener bkdPostingListEventListener()
    {
        return bkdIndexMetrics.bkdPostingListEventListener();
    }

    @Override
    public QueryEventListener.PostingListEventListener triePostingListEventListener()
    {
        return trieIndexMetrics.triePostingListEventListener();
    }
}
