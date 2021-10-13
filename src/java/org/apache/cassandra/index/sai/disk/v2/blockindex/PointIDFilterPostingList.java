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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.IOException;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.lucene.util.packed.BlockPackedReader;

public class PointIDFilterPostingList implements PostingList
{
    final long minPointID;
    final long maxPointID;
    final BlockPackedReader rowIDPointIDMap;
    final long numRows;
    private long currentRowID = 0;

    public PointIDFilterPostingList(long minPointID, long maxPointID, long numRows, BlockPackedReader rowIDPointIDMap)
    {
        this.minPointID = minPointID;
        this.maxPointID = maxPointID;
        this.numRows = numRows;
        this.rowIDPointIDMap = rowIDPointIDMap;
    }

    @Override
    public long nextPosting() throws IOException
    {
        return advance(currentRowID + 1);
    }

    @Override
    public long size()
    {
        return maxPointID - minPointID;
    }

    @Override
    public long advance(long targetRowId) throws IOException
    {
        while (true)
        {
            if (targetRowId >= numRows)
            {
                return PostingList.END_OF_STREAM;
            }

            assert targetRowId >= currentRowID;

            this.currentRowID = targetRowId;

            final long pointID = rowIDPointIDMap.get(currentRowID);
            if (pointID == -1 || (pointID < minPointID || pointID > maxPointID))
            {
                // skip to the next valid or matching row
                //advance(targetRowId + 1);
                targetRowId++;
            }
            else
            {
                return currentRowID;
            }
        }
    }
}
