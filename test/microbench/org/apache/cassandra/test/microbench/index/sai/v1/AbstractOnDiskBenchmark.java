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

//<<<<<<< HEAD
//import org.apache.cassandra.index.sai.disk.SSTableComponentsWriter;
//=======
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.util.FileHandle;

import java.util.Random;

public abstract class AbstractOnDiskBenchmark
{
    private static Random random = new Random();

    private Descriptor descriptor;

//<<<<<<< HEAD
//    TableMetadata metadata;
//    IndexComponents groupComponents;
//    private PrimaryKey.PrimaryKeyFactory keyFactory;
//    private FileHandle primaryKeys;
//    PrimaryKeyMap primaryKeyMap;
//=======
//    IndexDescriptor indexDescriptor;
//    String index;
//    private IndexComponent postingLists;
//    private FileHandle token;
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)

    private FileHandle postings;
    private long summaryPosition;


    /**
     * @return num of rows to be stored in per-sstable components
     */
    public abstract int numRows();

    /**
     * @return num of postings to be written in posting file
     */
    public abstract int numPostings();

    /**
     * To be called before executing each @Benchmark method
     */
    public abstract void beforeInvocation() throws Throwable;

    /**
     * To be called after executing each @Benchmark method
     */
    public abstract void afterInvocation() throws Throwable;
}
//
//    protected int toPosting(int id)
//    {
//        return id;
//    }

//    @Setup(Level.Trial)
//    public void perTrialSetup() throws IOException, MemtableTrie.SpaceExhaustedException
//    {
//        DatabaseDescriptor.daemonInitialization(); // required to use ChunkCache
//        assert ChunkCache.instance != null;

//<<<<<<< HEAD
//        String keyspaceName = "ks";
//        String tableName = this.getClass().getSimpleName();
//        metadata = TableMetadata
//                .builder(keyspaceName, tableName)
//                .partitioner(Murmur3Partitioner.instance)
//                .addPartitionKeyColumn("pk", UTF8Type.instance)
//                .addRegularColumn("col", IntegerType.instance)
//                .build();
//
//        descriptor = new Descriptor(Files.createTempDirectory("jmh").toFile(), metadata.keyspace, metadata.name, 1);
//        keyFactory = PrimaryKey.factory(metadata);
//        groupComponents = IndexComponents.perSSTable(descriptor, keyFactory, null);
//        indexComponents = IndexComponents.create("col", descriptor, keyFactory, null);
//
//        writePrimaryKeysComponent(numRows());
//        primaryKeys = groupComponents.createFileHandle(IndexComponents.PRIMARY_KEYS);
//        primaryKeyMap = new PrimaryKeyMap.DefaultPrimaryKeyMap(groupComponents, metadata);
//=======
//        descriptor = new Descriptor(Files.createTempDirectory("jmh").toFile(), "ks", this.getClass().getSimpleName(), 1);
//        indexDescriptor = IndexDescriptor.create(descriptor);
//        index = "test";
//        postingLists = IndexComponent.create(IndexComponent.Type.POSTING_LISTS, index);
//
//        // write per-sstable components: token and offset
//        writeSSTableComponents(numRows());
//        token = indexDescriptor.createFileHandle(IndexComponent.TOKEN_VALUES);
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)

//        // write postings
//        summaryPosition = writePostings(numPostings());
//        postings = indexDescriptor.createFileHandle(postingLists);
//    }

//    @TearDown(Level.Trial)
//    public void perTrialTearDown() throws IOException
//    {
//        primaryKeyMap.close();
//        primaryKeys.close();
//        postings.close();
//        FileUtils.deleteRecursive(descriptor.directory);
//    }
//
//    @Setup(Level.Invocation)
//    public void perInvocationSetup() throws Throwable
//    {
//        beforeInvocation();
//    }
//
//    @TearDown(Level.Invocation)
//    public void perInvocationTearDown() throws Throwable
//    {
//        afterInvocation();
//    }
//
//    private long writePostings(int rows) throws IOException
//    {
//        final int[] postings = IntStream.range(0, rows).map(this::toPosting).toArray();
//        final ArrayPostingList postingList = new ArrayPostingList(postings);
//
//        try (PostingsWriter writer = new PostingsWriter(indexDescriptor, index, false))
//        {
//            long summaryPosition = writer.write(postingList);
//            writer.complete();
//
//            return summaryPosition;
//        }
//    }
//
//    protected final PostingsReader openPostingsReader() throws IOException
//    {
//<<<<<<< HEAD
//        IndexInput input = indexComponents.openInput(postings);
//        IndexInput summaryInput = indexComponents.openInput(postings);
//=======
//        IndexInput input = IndexFileUtils.instance.openInput(postings);
//        IndexInput summaryInput = IndexFileUtils.instance.openInput(postings);
//
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)
//        PostingsReader.BlocksSummary summary = new PostingsReader.BlocksSummary(summaryInput, summaryPosition);
//        return new PostingsReader(input, summary, QueryEventListener.PostingListEventListener.NO_OP, primaryKeyMap);
//    }
//
//    /**
//     * Generates a number of unique, random primary keys and writes them to the PRIMARY_KEYS component of the sstable
//     */
//    private void writePrimaryKeysComponent(int rowCount) throws IOException, MemtableTrie.SpaceExhaustedException
//    {
//<<<<<<< HEAD
//        Random rng = ThreadLocalRandom.current();
//        IPartitioner partitioner = Murmur3Partitioner.instance;
//        PrimaryKey.PrimaryKeyFactory factory = PrimaryKey.factory(metadata);
//        SortedSet<DecoratedKey> primaryKeys = new TreeSet<>();
//        while (primaryKeys.size() < rowCount)
//        {
//            String keyStr = RandomStrings.randomAsciiOfLength(rng, 8);
//            DecoratedKey dk = partitioner.decorateKey(UTF8Type.instance.decompose(keyStr));
//            primaryKeys.add(dk);
//        }
//=======
//        SSTableComponentsWriter writer = new SSTableComponentsWriter(indexDescriptor, null);
//        for (int i = 0; i < rows; i++)
//            writer.recordCurrentTokenOffset(toToken(i), toOffset(i));
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)
//
//        SSTableComponentsWriter writer = new SSTableComponentsWriter.OnDiskSSTableComponentsWriter(descriptor, keyFactory,null);
//        long rowId = 0;
//        for (DecoratedKey dk: primaryKeys)
//        {
//            PrimaryKey pk = factory.createKey(dk, Clustering.EMPTY, rowId);
//            writer.nextRow(pk);
//            rowId++;
//        }
//        writer.complete();
//    }
//
//<<<<<<< HEAD
//=======
//    protected final LongArray openRowIdToTokenReader() throws IOException
//    {
//        MetadataSource source = MetadataSource.load(indexDescriptor.openInput(IndexComponent.GROUP_META));
//        return new BlockPackedReader(token, IndexComponent.TOKEN_VALUES, source).open();
//    }
//>>>>>>> a1c417a8f0 (STAR-158: Add on-disk version support to SAI)
//}
