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

import java.io.IOException;
import java.util.Set;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PerSSTableWriter;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.format.IndexFeatureSet;
import org.apache.cassandra.index.sai.disk.v1.SSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;
import org.apache.cassandra.index.sai.disk.v1.V1PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.v2.V2OnDiskFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class V3OnDiskFormat extends V2OnDiskFormat
{
    public static final V3OnDiskFormat instance = new V3OnDiskFormat();

    private static final IndexFeatureSet v3IndexFeatureSet = new IndexFeatureSet()
    {
        @Override
        public boolean isRowAware()
        {
            return false;
        }

        @Override
        public boolean usesNonStandardEncoding()
        {
            return false;
        }

        @Override
        public boolean supportsRounding()
        {
            return false;
        }
    };

    protected V3OnDiskFormat()
    {}

    @Override
    public IndexFeatureSet indexFeatureSet()
    {
        return v3IndexFeatureSet;
    }

    @Override
    public PrimaryKeyMap.Factory newPrimaryKeyMapFactory(IndexDescriptor indexDescriptor, SSTableReader sstable) throws IOException
    {
        return new V1PrimaryKeyMap.V1PrimaryKeyMapFactory(indexDescriptor, sstable);
    }

    @Override
    public Set<IndexComponent> perSSTableComponents()
    {
        return V1OnDiskFormat.PER_SSTABLE_COMPONENTS;
    }

    @Override
    public Set<IndexComponent> perIndexComponents(IndexContext indexContext)
    {
        return PER_INDEX_COMPONENTS;
    }

    @Override
    public PerSSTableWriter newPerSSTableWriter(IndexDescriptor indexDescriptor) throws IOException
    {
        return new SSTableComponentsWriter(indexDescriptor);
    }

    @Override
    public int openFilesPerSSTable()
    {
        return 2;
    }
}
