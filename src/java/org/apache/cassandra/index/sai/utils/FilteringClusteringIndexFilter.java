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

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.CachedPartition;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.TableMetadata;

public class FilteringClusteringIndexFilter implements ClusteringIndexFilter
{
    private final ClusteringIndexFilter wrapped;
    private final Clustering<?> clustering;

    public FilteringClusteringIndexFilter(ClusteringIndexFilter filter, Clustering<?> clustering)
    {
        this.wrapped = filter;
        this.clustering = clustering;
    }

    @Override
    public boolean isReversed()
    {
        return wrapped.isReversed();
    }

    @Override
    public boolean isEmpty(ClusteringComparator comparator)
    {
        return wrapped.isEmpty(comparator);
    }

    @Override
    public ClusteringIndexFilter forPaging(ClusteringComparator comparator, Clustering<?> lastReturned, boolean inclusive)
    {
        return new FilteringClusteringIndexFilter(wrapped.forPaging(comparator, lastReturned, inclusive), clustering);
    }

    @Override
    public boolean isFullyCoveredBy(CachedPartition partition)
    {
        return isFullyCoveredBy(partition);
    }

    @Override
    public boolean isHeadFilter()
    {
        return wrapped.isHeadFilter();
    }

    @Override
    public boolean selectsAllPartition()
    {
        return wrapped.selectsAllPartition();
    }

    @Override
    public boolean selects(Clustering<?> clustering)
    {
        return this.clustering.equals(clustering);
    }

    @Override
    public UnfilteredRowIterator filterNotIndexed(ColumnFilter columnFilter, UnfilteredRowIterator iterator)
    {
        return wrapped.filterNotIndexed(columnFilter, iterator);
    }

    @Override
    public Slices getSlices(TableMetadata metadata)
    {
        return wrapped.getSlices(metadata);
    }

    @Override
    public UnfilteredRowIterator getUnfilteredRowIterator(ColumnFilter columnFilter, Partition partition)
    {
        return wrapped.getUnfilteredRowIterator(columnFilter, partition);
    }

    @Override
    public boolean intersects(ClusteringComparator comparator, Slice slice)
    {
        return wrapped.intersects(comparator, slice);
    }

    @Override
    public Kind kind()
    {
        return wrapped.kind();
    }

    @Override
    public String toString(TableMetadata metadata)
    {
        return wrapped.toString(metadata);
    }

    @Override
    public String toCQLString(TableMetadata metadata)
    {
        return wrapped.toCQLString(metadata);
    }
}
