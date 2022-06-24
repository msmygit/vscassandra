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
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.Counter;

public class BinaryTreeTest extends SaiRandomizedTest
{
    @Test
    public void testSimple() throws Exception
    {
        BytesRefArray minBlockTerms = new BytesRefArray(Counter.newCounter());
        for (int x = 0; x < 10; x++)
        {
            int num = x * 10;
            String str = String.format("%05d", x);
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

        System.out.println();

        try (IndexInput input = dir.openInput("file", IOContext.DEFAULT))
        {
            BinaryTree.Reader reader = new BinaryTree.Reader(minBlockTerms.size(),
                                                             minBlockTerms.size() * 10,
                                                             10,
                                                             input);
            reader.traverse(new IntArrayList());
        }
    }
}
