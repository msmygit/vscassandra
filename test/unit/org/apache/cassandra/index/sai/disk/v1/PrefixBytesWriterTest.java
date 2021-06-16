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

package org.apache.cassandra.index.sai.disk.v1;

import java.util.ArrayList;
import java.util.List;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.store.*;
import org.apache.lucene.util.BytesRefBuilder;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class PrefixBytesWriterTest
{
    public static Pair<ByteComparable, IntArrayList> add(String term, int[] array)
    {
        IntArrayList list = new IntArrayList();
        list.add(array, 0, array.length);
        return Pair.create(ByteComparable.fixedLength(UTF8Type.instance.decompose(term)), list);
    }

    @Test
    public void test() throws Exception
    {
        List<Pair<ByteComparable, IntArrayList>> list = new ArrayList();
        list.add(add("aaabbb", new int[] {0, 2}));
        list.add(add("aaabbbbbb", new int[] {1, 3}));

        PrefixBytesWriter prefixBytesWriter = new PrefixBytesWriter();

        TermsIterator terms = new MemtableTermsIterator(null,
                                                        null,
                                                        list.iterator());

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput out = dir.createOutput("file", IOContext.DEFAULT);
        prefixBytesWriter.write(terms, out, 1024);
        out.close();

        IndexInput input = dir.openInput("file", IOContext.DEFAULT);
        IndexInput input2 = dir.openInput("file", IOContext.DEFAULT);
        BytesRefBuilder builder = new BytesRefBuilder();
        PrefixBytesReader reader = new PrefixBytesReader(input, input2,  3, builder);

    }
}
