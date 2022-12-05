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

package org.apache.cassandra.index.sai.disk.v3;

import org.junit.Test;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexWriterConfig;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;

import static org.apache.cassandra.index.sai.disk.v3.BlockTermsTest.toBytes;

public class BinaryTreePostingsWriterTest extends SaiRandomizedTest
{
    @Test
    public void testSimple() throws Exception
    {
        final IndexDescriptor indexDescriptor = newIndexDescriptor();
        final String index = newIndex();
        final IndexContext indexContext = SAITester.createIndexContext(index, Int32Type.instance);

        final BlockTerms.Writer writer = new BlockTerms.Writer(1024, indexDescriptor, indexContext, false);

        final int count = 60_000;

        int value = 0;

        int rowCount = 0;

        long rowid = 0;
        for (int i = 0; i < count; i++)
        {
            int valueCount = nextInt(1, 50);
            for (int x = 0; x < valueCount; x++)
            {
                writer.add(toBytes(value), rowid++);
                rowCount++;
            }
            value++;
        }

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();
        writer.finish(components);

        try (V3PerIndexFiles indexFiles = new V3PerIndexFiles(indexDescriptor, indexContext, false);
             BlockTerms.Reader reader = new BlockTerms.Reader(indexDescriptor,
                                                              indexContext,
                                                              indexFiles,
                                                              components))
        {
            ByteBuffersDirectory dir = new ByteBuffersDirectory();
            try (IndexOutput out = dir.createOutput("binarytree", IOContext.DEFAULT))
            {
                BinaryTree.Writer treeWriter = new BinaryTree.Writer();
                treeWriter.finish(writer.minBlockTerms, out);
            }

            BinaryTreePostingsWriter.Result result = null;
            try (IndexInput binaryTreeInput = dir.openInput("binarytree", IOContext.DEFAULT);
                 IndexOutput upperPostingsOut = dir.createOutput("upperpostings", IOContext.DEFAULT))
            {
                final BinaryTree.Reader treeReader = new BinaryTree.Reader(writer.minBlockTerms.size(),
                                                                           writer.minBlockTerms.size() * 10,
                                                                           reader.meta.postingsBlockSize,
                                                                           binaryTreeInput);

                IndexWriterConfig config = IndexWriterConfig.defaultConfig("");
                final BinaryTreePostingsWriter treePostingsWriter = new BinaryTreePostingsWriter(config);
                treeReader.traverse(new IntArrayList(), treePostingsWriter);

                result = treePostingsWriter.finish(reader, upperPostingsOut, indexContext);
            }

            try (IndexInput binaryTreeInput = dir.openInput("binarytree", IOContext.DEFAULT);
                 IndexInput upperPostingsInput = dir.openInput("upperpostings", IOContext.DEFAULT))
            {
                final BinaryTree.Reader treeReader = new BinaryTree.Reader(writer.minBlockTerms.size(),
                                                                           writer.minBlockTerms.size() * 10,
                                                                           reader.meta.postingsBlockSize,
                                                                           binaryTreeInput);

                BinaryTreePostingsReader treePostingsReader = new BinaryTreePostingsReader(result.indexFilePointer, upperPostingsInput);
                System.out.println("upper postings index map="+treePostingsReader.index);
            }
        }
    }

    @Test
    public void test() throws Exception
    {
        BytesRefArray minBlockTerms = new BytesRefArray(Counter.newCounter());
        for (int x = 0; x < 10; x++)
        {
            int num = x * 10;
            String str = String.format("%05d", num);
            System.out.println(str);
            minBlockTerms.append(new BytesRef(str));
        }

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        try (IndexOutput out = dir.createOutput("file", IOContext.DEFAULT))
        {
            // create 4 min block terms (or 4 leaf blocks)
            BinaryTree.Writer writer = new BinaryTree.Writer();
            writer.finish(minBlockTerms, out);
        }

        try (IndexInput input = dir.openInput("file", IOContext.DEFAULT))
        {
            final BinaryTree.Reader reader = new BinaryTree.Reader(minBlockTerms.size(),
                                                                   minBlockTerms.size() * 10,
                                                                   10,
                                                                   input);

            IndexWriterConfig config = IndexWriterConfig.defaultConfig("");
            final BinaryTreePostingsWriter treePostingsWriter = new BinaryTreePostingsWriter(config);
            reader.traverse(new IntArrayList(), treePostingsWriter);

            // treePostingsWriter.finish();
        }
    }
}
