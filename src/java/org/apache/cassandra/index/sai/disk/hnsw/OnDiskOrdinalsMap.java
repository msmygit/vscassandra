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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.IOException;
import java.util.ArrayList;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.Pair;

public class OnDiskOrdinalsMap
{
    private static final Logger logger = LoggerFactory.getLogger(OnDiskOrdinalsMap.class);

    private final FileHandle fh;
    private final int size;
    // the offset where we switch from recording ordinal -> rows, to row -> ordinal
    private final long rowOrdinalOffset;

    public OnDiskOrdinalsMap(FileHandle fh)
    {
        this.fh = fh;
        try (var reader = fh.createReader())
        {
            logger.debug("file length is {}", reader.length());
            this.size = reader.readInt();
            reader.seek(reader.length() - 8);
            this.rowOrdinalOffset = reader.readLong();

            // ordinal -> row offsets
            reader.seek(4);
            for (int j = 0; j < size; j++)
                reader.readLong();

            // ordinal -> rows
            int ordinal = 0;
            for (int j = 0; j < size; j++)
            {
                var count = reader.readInt();
                var L = new ArrayList<Integer>();
                for (int i = 0; i < count; i++)
                    L.add(reader.readInt());
                logger.debug(String.format("ordinal %s -> rows %s", ordinal++, L));
            }
            assert reader.getFilePointer() == rowOrdinalOffset : String.format("reader.getFilePointer() %s != rowOrdinalOffset %s", reader.getFilePointer(), rowOrdinalOffset);

            assert (reader.length() - rowOrdinalOffset) % 8 == 0;
            reader.seek(rowOrdinalOffset);
            var L = new ArrayList<Pair<Integer, Integer>>();
            while (reader.getFilePointer() < reader.length() - 8)
            {
                var rowId = reader.readInt();
                var vectorOrdinal = reader.readInt();
                L.add(Pair.create(rowId, vectorOrdinal));
            }
            for (int n = 0; n < L.size(); n++)
            {
                var p = L.get(n);
                assert p.left < size;
                var vectorOrdinal = p.right;
                assert vectorOrdinal < size : String.format("vectorOrdinal %s is out of bounds %s at entry %s", vectorOrdinal, size, n);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    public int[] getSegmentRowIdsMatching(int vectorOrdinal) throws IOException
    {
        Preconditions.checkArgument(vectorOrdinal < size, "vectorOrdinal %s is out of bounds %s", vectorOrdinal, size);

        try (var reader = fh.createReader())
        {
            // read index entry
            reader.seek(4L + vectorOrdinal * 8L);
            var offset = reader.readLong();
            // seek to and read ordinals
            reader.seek(offset);
            var postingsSize = reader.readInt();
            var ordinals = new int[postingsSize];
            for (var i = 0; i < ordinals.length; i++)
            {
                ordinals[i] = reader.readInt();
            }
            return ordinals;
        }
    }

    /**
     * @return order if given row id is found; otherwise return -1
     */
    public int getOrdinalForRowId(int rowId) throws IOException
    {
        try (var reader = fh.createReader())
        {
            // Compute the offset of the start of the rowId to vectorOrdinal mapping
            var high = (reader.length() - 8 - rowOrdinalOffset) / 8;
            long index = DiskBinarySearch.searchInt(0, Math.toIntExact(high), rowId, i -> {
                try
                {
                    long offset = rowOrdinalOffset + i * 8;
                    reader.seek(offset);
                    return reader.readInt();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            });

            // not found
            if (index < 0)
                return -1;
            reader.seek(rowOrdinalOffset + index * 8);
            var id = reader.readInt();
            assert id == rowId : "id mismatch: " + id + " != " + rowId;

            return reader.readInt();
        }
    }

    public void close()
    {
        fh.close();
    }
}
