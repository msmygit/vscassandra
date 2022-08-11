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

package org.apache.cassandra.test.microbench.index.sai.v1;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.sux4j.util.EliasFanoIndexedMonotoneLongBigList;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.utils.ArrayPostingList;
import org.apache.cassandra.io.util.File;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class EliasFanoBenchmark //extends AbstractOnDiskBenchmark
{
    private static final int NUM_INVOCATIONS = 10_000;

    protected int numMatches = 10;

    // @Param({ "1", "10", "100", "1000"})
    @Param({ "1000", "3000", "5000"})
    public int skippingDistance;

    protected PostingsReader reader;

    protected EliasFanoIndexedMonotoneLongBigList eliasFanoPostings;

    protected Directory directory;
    protected IndexInput input;

    private int[] rowIds;
    // @Override
    public int numRows()
    {
        return 10_000_000;
    }

    // @Override
    public int numPostings()
    {
        return 10_000_000;
    }

    @Setup(Level.Trial)
    public void beforeInvocation() throws IOException
    {
        rowIds = new int[NUM_INVOCATIONS];
        for (int i = 0; i < rowIds.length; i++)
        {
            rowIds[i] = i;
        }

        Path file = Files.createTempDirectory("jmh");
        directory = FSDirectory.open(file);

        long filePointer = -1;

        try (IndexOutput output = directory.createOutput("postings", IOContext.DEFAULT))
        {
            final int[] postings = IntStream.range(0, numRows()).toArray();
            final ArrayPostingList postingList = new ArrayPostingList(postings);

            try (PostingsWriter writer = new PostingsWriter(output))
            {
                filePointer = writer.write(postingList);
                writer.complete();
            }
        }

        input = directory.openInput("postings", IOContext.DEFAULT);

        reader = new PostingsReader(input, filePointer, QueryEventListener.PostingListEventListener.NO_OP);

        eliasFanoPostings = new EliasFanoIndexedMonotoneLongBigList(IntArrayList.wrap(rowIds));
    }

    @TearDown(Level.Trial)
    public void afterInvocation() throws Throwable
    {
        reader.close();
        directory.close();
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput})
    public void advanceSAI(Blackhole bh) throws Throwable
    {
        for (int i = 0; i < rowIds.length;)
        {
            int rowId = rowIds[i];

            bh.consume(reader.advance(rowId));

            i += skippingDistance;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput })
    public void advanceEliasFano(Blackhole bh) throws Throwable
    {
        for (int i = 0; i < rowIds.length;)
        {
            int rowId = rowIds[i];

            bh.consume(eliasFanoPostings.successorUnsafe(rowId));

            i += skippingDistance;
        }
    }
}
