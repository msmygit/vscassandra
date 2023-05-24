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
import java.util.Collection;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.serializers.TypeSerializer;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.hnsw.ConcurrentHnswGraphBuilder;
import org.apache.lucene.util.hnsw.HnswGraphSearcher;
import org.apache.lucene.util.hnsw.NeighborQueue;

public class CassandraOnHeapHnsw<T>
{
    private final VectorSimilarityFunction similarityFunction;
    private final ByteBufferVectorValues vectorValues;
    private final ConcurrentHnswGraphBuilder<float[]> builder;
    final Map<ByteBuffer, VectorPostings<T>> postingsMap;
    private final AtomicInteger nextOrdinal = new AtomicInteger();
    private final LongAdder writeCount = new LongAdder();
    private final Counter bytesUsed = Counter.newCounter();

    private long lastGraphRamUsage;

    public CassandraOnHeapHnsw(AbstractType<?> termComparator, IndexWriterConfig indexWriterConfig)
    {
        similarityFunction = indexWriterConfig.getSimilarityFunction();
        vectorValues = new ByteBufferVectorValues((TypeSerializer<float[]>) termComparator.getSerializer());
        postingsMap = new ConcurrentSkipListMap<>((left, right) -> ValueAccessor.compare(left, ByteBufferAccessor.instance, right, ByteBufferAccessor.instance));

        try
        {
            builder = ConcurrentHnswGraphBuilder.create(vectorValues,
                                                        VectorEncoding.FLOAT32,
                                                        similarityFunction,
                                                        indexWriterConfig.getMaximumNodeConnections(),
                                                        indexWriterConfig.getConstructionBeamWidth());
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        bytesUsed.addAndGet(ObjectSizes.measureDeep(postingsMap) + ObjectSizes.measureDeep(vectorValues) + ObjectSizes.measureDeep(builder));
        lastGraphRamUsage = builder.getGraph().ramBytesUsed();
    }

    public int size()
    {
        return vectorValues.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public long add(ByteBuffer term, T key)
    {
        long initialSize = bytesUsed.get();

        var postings = postingsMap.computeIfAbsent(term, v -> {
            var ordinal = nextOrdinal.getAndIncrement();
            bytesUsed.addAndGet(vectorValues.add(ordinal, term));
            try
            {
                builder.addGraphNode(ordinal, vectorValues);
                long graphRamBytesUsed = builder.getGraph().ramBytesUsed();
                bytesUsed.addAndGet(graphRamBytesUsed - lastGraphRamUsage);
                lastGraphRamUsage = graphRamBytesUsed;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            bytesUsed.addAndGet(VectorPostings.EMPTY_SIZE);
            return new VectorPostings<>(ordinal);
        });
        bytesUsed.addAndGet(postings.append(key));
        return bytesUsed.get() - initialSize;
    }

    public PriorityQueue<T> search(float[] queryVector, int limit, Bits toAccept)
    {
        NeighborQueue queue;
        try
        {
            queue = HnswGraphSearcher.search(queryVector,
                                             limit,
                                             vectorValues,
                                             VectorEncoding.FLOAT32,
                                             similarityFunction,
                                             builder.getGraph().getView(),
                                             toAccept,
                                             Integer.MAX_VALUE);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        PriorityQueue<T> keyQueue = new PriorityQueue<>();
        while (queue.size() > 0)
        {
            for (var pk : keysFromOrdinal(queue.pop()))
            {
                keyQueue.add(pk);
            }
        }
        return keyQueue;
    }

    public void writeData(IndexDescriptor indexDescriptor, IndexContext indexContext, Function<T, Integer> postingTransformer) throws IOException
    {
        try (var vectorsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.VECTOR, indexContext));
             var postingsOutput = IndexFileUtils.instance.openOutput(indexDescriptor.fileFor(IndexComponent.POSTING_LISTS, indexContext)))
        {
            vectorValues.write(vectorsOutput.asSequentialWriter());
            new VectorPostingsWriter<T>().writePostings(postingsOutput.asSequentialWriter(), vectorValues, postingsMap, postingTransformer);
            new HnswGraphWriter(new ExtendedConcurrentHnswGraph(builder.getGraph())).write(indexDescriptor.fileFor(IndexComponent.TERMS_DATA, indexContext));
        }
    }

    public Collection<T> keysFromOrdinal(int node)
    {
        return postingsMap.get(vectorValues.bufferValue(node)).postings;
    }

    private long expensiveRamUsage()
    {
        return ObjectSizes.measureDeep(postingsMap) + ObjectSizes.measureDeep(vectorValues) + ObjectSizes.measureDeep(builder);
    }
}
