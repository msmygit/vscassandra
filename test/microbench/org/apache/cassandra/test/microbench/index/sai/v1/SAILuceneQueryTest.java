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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.netty.util.internal.ThreadLocalRandom;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1)
@Threads(1)
@State(Scope.Benchmark)
public class SAILuceneQueryTest extends CQLTester
{
    private static final int NUM_INVOCATIONS = 10_000;
    public static int ROWS = 1_000_000;

    static String keyspace;
    String table;
    String writeStatement;
    String readStatement;
    ColumnFamilyStore cfs;
    FSDirectory directory;
    DirectoryReader luceneReader;
    IndexSearcher indexSearcher;

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        CQLTester.setUpClass();
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s (key bigint, columnA bigint, columnB bigint, PRIMARY KEY(key))");
        createIndex("CREATE CUSTOM INDEX ON "+keyspace+"."+table+"(columnA) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON "+keyspace+"."+table+"(columnB) USING 'StorageAttachedIndex'");

        execute("USE "+keyspace+";");
        writeStatement = "INSERT INTO "+table+" (key, columnA, columnB) VALUES (?, ?, ?)";
        readStatement = "SELECT * FROM "+table+" WHERE columnA = ? AND columnB = ? LIMIT 10";

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();

        Path luceneDir = Files.createTempDirectory("jmh_lucene_test");
        directory = FSDirectory.open(luceneDir);
        IndexWriterConfig config = new IndexWriterConfig(new WhitespaceAnalyzer());
        IndexWriter indexWriter = new IndexWriter(directory, config);

        Document document = new Document();

        System.err.println("Writing "+ROWS+" rows");
        for (long i = 0; i < ROWS; i++)
        {
            int columnAValue = ThreadLocalRandom.current().nextInt(0, ROWS / 50);
            int columnBValue = ThreadLocalRandom.current().nextInt(0, ROWS / 1000);

            execute(writeStatement, i, new Long(columnAValue), new Long(columnBValue));

            StringField columnA = new StringField("columnA", Integer.toString(columnAValue), Field.Store.NO);
            StringField columnB = new StringField("columnB", Integer.toString(columnBValue), Field.Store.NO);

            document.clear();
            document.add(columnA);
            document.add(columnB);

            indexWriter.addDocument(document);
        }

        indexWriter.forceMerge(1);
        indexWriter.close();

        flush(keyspace);
        compact(keyspace);

        luceneReader = DirectoryReader.open(directory);
        indexSearcher = new IndexSearcher(luceneReader);
    }

    @TearDown(Level.Trial)
    public void teardown() throws IOException, ExecutionException, InterruptedException
    {
        luceneReader.close();

        CommitLog.instance.shutdownBlocking();
        CQLTester.cleanup();
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
    public Object readLucene() throws Throwable
    {
        int columnA = ThreadLocalRandom.current().nextInt(0, ROWS / 1000);
        int columnB = ThreadLocalRandom.current().nextInt(0, ROWS / 10_000);

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(new BooleanClause(new TermQuery(new Term("columnA", new BytesRef(Integer.toString(columnA)))), BooleanClause.Occur.MUST));
        builder.add(new BooleanClause(new TermQuery(new Term("columnB", new BytesRef(Integer.toString(columnB)))), BooleanClause.Occur.MUST));

        BooleanQuery query = builder.build();

        return indexSearcher.search(query, 10);
    }

    @Benchmark
    @OperationsPerInvocation(NUM_INVOCATIONS)
    @BenchmarkMode({ Mode.Throughput, Mode.AverageTime })
    public Object readSAI() throws Throwable
    {
        int columnA = ThreadLocalRandom.current().nextInt(0, ROWS / 1000);
        int columnB = ThreadLocalRandom.current().nextInt(0, ROWS / 10_000);
        return execute(readStatement, new Long(columnA), new Long(columnB));
    }
}
