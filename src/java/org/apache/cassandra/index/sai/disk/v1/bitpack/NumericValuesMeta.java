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
package org.apache.cassandra.index.sai.disk.v1.bitpack;

import java.io.IOException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class NumericValuesMeta
{
    public final long valueCount;
    public final int blockSize;
    public final long blockMetaOffset;

    public NumericValuesMeta(Map<String, String> map)
    {
        valueCount = Long.parseLong(map.get("valueCount"));
        blockSize = Integer.parseInt(map.get("blockSize"));
        blockMetaOffset = Long.parseLong(map.get("blockMetaOffset"));
    }

    public Map<String, String> stringMap()
    {
        Map<String, String> map = new HashMap<>();
        map.put("valueCount", Long.toString(valueCount));
        map.put("blockSize", Integer.toString(blockSize));
        map.put("blockMetaOffset", Long.toString(blockMetaOffset));
        return map;
    }

    public NumericValuesMeta(IndexInput input) throws IOException
    {
        valueCount = input.readLong();
        blockSize = input.readInt();
        blockMetaOffset = input.readVLong();
    }

    public NumericValuesMeta(long valueCount, int blockSize, long blockMetaOffset)
    {
        this.valueCount = valueCount;
        this.blockSize = blockSize;
        this.blockMetaOffset = blockMetaOffset;
    }

    public void write(IndexOutput out) throws IOException
    {
        out.writeLong(valueCount);
        out.writeInt(blockSize);
        out.writeVLong(blockMetaOffset);
    }
}
