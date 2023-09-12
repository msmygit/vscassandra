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

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;

/**
 * Essentially a Function<Long, PrimaryKey>, but avoids the boxing of the long.
 */
public class RowIdToPrimaryKeyMapper implements AutoCloseable
{
    private final PrimaryKeyMap primaryKeyMap;
    private final long segmentRowIdOffset;

    /**
     *
     * @param factory - primary key map factory
     * @param segmentRowIdOffset - the offset to add to the rowId to get the SS Table row id
     */
    public RowIdToPrimaryKeyMapper(PrimaryKeyMap.Factory factory, long segmentRowIdOffset)
    {
        this.primaryKeyMap = factory.newPerSSTablePrimaryKeyMap();
        this.segmentRowIdOffset = segmentRowIdOffset;
    }

    public PrimaryKey getPrimaryKeyForRowId(long rowId)
    {
        System.out.println("### getPrimaryKeyForRowId " + rowId + " - " + segmentRowIdOffset);
        long ssTableRowId = rowId + segmentRowIdOffset;
        // Special case where we wouldn't want to attempt to retreive a PK
        if (ssTableRowId == PostingList.END_OF_STREAM)
            return null;

        // Need to load eagerly to ensure we can close the primaryKeyMap safely
        return primaryKeyMap.primaryKeyFromRowId(ssTableRowId).loadDeferred();
    }

    @Override
    public void close() throws IOException
    {
        primaryKeyMap.close();
    }
}
