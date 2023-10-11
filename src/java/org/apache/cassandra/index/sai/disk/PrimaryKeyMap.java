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

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.utils.PrimaryKey;

/**
 * A bidirectional map of {@link PrimaryKey} to row Id. Implementations of this interface
 * are not expected to be threadsafe.
 */
@NotThreadSafe
public interface PrimaryKeyMap extends Closeable
{
    /**
     * A factory for creating {@link PrimaryKeyMap} instances. Implementations of this
     * interface are expected to be threadsafe.
     */
    public interface Factory extends Closeable
    {
        /**
         * Creates a new {@link PrimaryKeyMap} instance
         *
         * @return a {@link PrimaryKeyMap}
         * @throws IOException
         */
        PrimaryKeyMap newPerSSTablePrimaryKeyMap();

        @Override
        default void close() throws IOException
        {
        }
    }

    /**
     * Returns a {@link PrimaryKey} for a row Id
     *
     * @param sstableRowId the row Id to lookup
     * @return the {@link PrimaryKey} associated with the row Id
     */
    PrimaryKey primaryKeyFromRowId(long sstableRowId);

    /**
     * Returns a row Id for a {@link PrimaryKey}. If there is no such term, {@link #isNotFound(long)} will return true.
     *
     * @param key the {@link PrimaryKey} to lookup
     * @return the row Id associated with the {@link PrimaryKey}
     */
    long exactRowIdForPrimaryKey(PrimaryKey key);

    /**
     * Returns the sstable row id associated with the least {@link PrimaryKey} greater than or equal to the given
     * {@link PrimaryKey}. If there is no such term, {@link #isNotFound(long)} will return true.
     *
     * @param key the {@link PrimaryKey} to lookup
     * @return the first row Id associated with the {@link PrimaryKey}
     */
    long ceiling(PrimaryKey key);

    /**
     * Returns the sstable row id associated with the greatest {@link PrimaryKey} less than or equal to the given
     * {@link PrimaryKey}. If there is no such term, {@link #isNotFound(long)} will return true.
     *
     * @param key the {@link PrimaryKey} to lookup
     * @return an sstable row id
     */
    long floor(PrimaryKey key);

    /**
     * This method is necessary because the two implementations for this interface have divergent behavior for
     * indicating that a {@link PrimaryKey} was not found.
     * @param sstableRowId
     * @return true if the passed row id is not found by the {@link PrimaryKeyMap}
     */
    boolean isNotFound(long sstableRowId);

    /**
     * Returns the number of primary keys in the map
     */
    long count();

    @Override
    default void close() throws IOException
    {
    }
}
