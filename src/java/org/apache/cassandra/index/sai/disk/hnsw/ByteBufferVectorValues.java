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
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

//TODO Could potentially move these offheap
public class ByteBufferVectorValues implements RandomAccessVectorValues<float[]>
{
    private final AtomicInteger cachedDimensions = new AtomicInteger();
    private final TypeSerializer<float[]> serializer;
    private final Map<Integer, ByteBuffer> values = new ConcurrentSkipListMap<>();
    private final long emptySize;

    public ByteBufferVectorValues(TypeSerializer<float[]> serializer)
    {
        this.serializer = serializer;
        this.emptySize = ObjectSizes.measureDeep(values);
    }

    @Override
    public int size()
    {
        return values.size();
    }

    @Override
    public int dimension()
    {
        int i = cachedDimensions.get();
        if (i == 0)
        {
            i = vectorValue(0).length;
            cachedDimensions.set(i);
        }
        return i;
    }

    @Override
    public float[] vectorValue(int i)
    {
        return serializer.deserialize(values.get(i));
    }

    public void add(int ordinal, ByteBuffer buffer)
    {
        values.put(ordinal, buffer);
    }

    @Override
    public RandomAccessVectorValues<float[]> copy()
    {
        return this;
    }

    public ByteBuffer bufferValue(int node)
    {
        return values.get(node);
    }

    public long ramBytesUsed()
    {
        if (values.isEmpty())
            return emptySize;

        return emptySize +
               (dimension() * 4 + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY) * values.size() +
               values.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
    }

    public void write(SequentialWriter writer) throws IOException
    {
        writer.writeInt(size());
        writer.writeInt(dimension());

        for (var i = 0; i < size(); i++) {
            var buffer = bufferValue(i);
            writer.write(buffer);
        }
    }
}
