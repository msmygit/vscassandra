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
package org.apache.cassandra.index.sai.disk;

import java.io.Closeable;
import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MetadataSource;
import org.apache.cassandra.index.sai.disk.v1.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.numerics.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;

public interface PrimaryKeyMap extends Closeable
{
    PrimaryKey primaryKeyFromRowId(long sstableRowId) throws IOException;

    default PrimaryKeyMap copyOf()
    {
        return IDENTITY;
    }

    @VisibleForTesting
    default long size()
    {
        return 0;
    }

    default void close() throws IOException
    {}

    PrimaryKeyMap IDENTITY = sstableRowId -> PrimaryKey.factory().createKey(new Murmur3Partitioner.LongToken(sstableRowId), sstableRowId);
}
