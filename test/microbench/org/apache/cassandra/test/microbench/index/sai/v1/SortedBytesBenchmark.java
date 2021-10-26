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
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v2.primarykey.SortedBytesReader;
import org.apache.cassandra.index.sai.disk.v2.primarykey.SortedBytesWriter;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import static org.apache.cassandra.index.sai.disk.format.IndexComponent.TERMS_INDEX;

@Fork(value = 1, jvmArgsAppend = {
        //        "-XX:+UnlockCommercialFeatures", "-XX:+FlightRecorder","-XX:+UnlockDiagnosticVMOptions", "-XX:+DebugNonSafepoints",
        //        "-XX:StartFlightRecording=duration=60s,filename=./BlockPackedReaderBenchmark.jfr,name=profile,settings=profile",
        //                            "-XX:FlightRecorderOptions=settings=/home/jake/workspace/cassandra/profiling-advanced.jfc,samplethreads=true"
})
@Warmup(iterations = 1)
@Measurement(iterations = 1, timeUnit = TimeUnit.NANOSECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
public class SortedBytesBenchmark extends AbstractOnDiskBenchmark
{
    private static final int NUM_INVOCATIONS = 10_000;

    @Param({ "1", "10"})//, "100", "1000"})
    public int skippingDistance;

    protected LongArray rowIdToToken;
    private int[] rowIds;
    private long[] tokenValues;
    FileHandle minTermsFile;
    IndexInput bytesInput, blockFPInput;
    SortedBytesReader sortedBytesReader;

    @Override
    public int numRows()
    {
        return NUM_INVOCATIONS;
    }

    @Override
    public int numPostings()
    {
        return NUM_INVOCATIONS;
    }

    SortedBytesReader.Meta meta = null;
    byte[][] intBytes = new byte[NUM_INVOCATIONS][];

    @Setup(Level.Trial)
    public void perTrialSetup2() throws IOException
    {
        try (IndexOutputWriter minKeyWriter = indexDescriptor.openPerIndexOutput(TERMS_INDEX, indexContext);
             IndexOutput bytesOutput = indexDescriptor.openPerIndexOutput(IndexComponent.TERMS_DATA, indexContext);
             IndexOutput blockFPOutput = indexDescriptor.openPerIndexOutput(IndexComponent.KD_TREE, indexContext))
        {
            SortedBytesWriter bytesWriter = new SortedBytesWriter(minKeyWriter,
                                                                  bytesOutput,
                                                                  blockFPOutput);
            for (int i = 0; i < NUM_INVOCATIONS; i++)
            {
                ByteBuffer buffer = Int32Type.instance.decompose(i);
                ByteSource byteSource = Int32Type.instance.asComparableBytes(buffer, ByteComparable.Version.OSS41);
                byte[] bytes2 = ByteSourceInverse.readBytes(byteSource);
                intBytes[i] = bytes2;
                bytesWriter.add(new BytesRef(bytes2));
            }
            meta = bytesWriter.finish();
        }
    }

    @Override
    public void beforeInvocation() throws Throwable
    {
        // rowIdToToken.findTokenRowID keeps track of last position, so it must be per-benchmark-method-invocation.
        rowIdToToken = openRowIdToTokenReader();

        rowIds = new int[NUM_INVOCATIONS];
        tokenValues = new long[NUM_INVOCATIONS];

        minTermsFile = indexDescriptor.createPerIndexFileHandle(TERMS_INDEX, indexContext);
        bytesInput = indexDescriptor.openPerIndexInput(IndexComponent.TERMS_DATA, indexContext);
        blockFPInput = indexDescriptor.openPerIndexInput(IndexComponent.KD_TREE, indexContext);
        sortedBytesReader = new SortedBytesReader(minTermsFile,
                                                  bytesInput,
                                                  blockFPInput,
                                                  meta);
    }

    @Override
    public void afterInvocation() throws Throwable
    {
        bytesInput.close();
        blockFPInput.close();
        rowIdToToken.close();
        minTermsFile.close();
        sortedBytesReader.close();
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
    public void bytesSeekToPointID(Blackhole bh) throws IOException
    {
        for (int i = 0; i < NUM_INVOCATIONS;)
        {
            bh.consume(sortedBytesReader.seekTo(i));
            i++;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
    public void bytesSeekToTerm(Blackhole bh) throws IOException
    {
        for (int i = 0; i < NUM_INVOCATIONS;)
        {
            bh.consume(sortedBytesReader.seekTo(new BytesRef(intBytes[i])));
            i++;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
    public void get(Blackhole bh)
    {
        for (int i = 0; i < rowIds.length;)
        {
            bh.consume(rowIdToToken.get(rowIds[i]));
            i++;
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
    public void findTokenRowID(Blackhole bh)
    {
        for (int i = 0; i < tokenValues.length;)
        {
            bh.consume(rowIdToToken.findTokenRowID(tokenValues[i]));
            i++;
        }
    }
}
