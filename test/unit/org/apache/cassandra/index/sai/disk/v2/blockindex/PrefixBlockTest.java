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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

public class PrefixBlockTest extends NdiRandomizedTest
{
    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        List<BytesRef> terms = new ArrayList();
        terms.add(new BytesRef("aaaaaa"));
        terms.add(new BytesRef("aaaabb"));
        terms.add(new BytesRef("aaaabc"));
        terms.add(new BytesRef("aaaabg"));
        terms.add(new BytesRef("aaaabggg"));
        terms.add(new BytesRef("aaggbbbb"));
        terms.add(new BytesRef("aaggmmmmbggg"));
        terms.add(new BytesRef("ttttjjjjjjj"));
        terms.add(new BytesRef("tttzzzzzjjjjjjj"));

        IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
        PrefixBlockWriter writer = new PrefixBlockWriter();
        for (BytesRef term : terms)
        {
            writer.add(term);
        }

        long fp = writer.finish(out);
        out.close();

        final SharedIndexInput input = new SharedIndexInput(dir.openInput("test", IOContext.DEFAULT));

        List<BytesRef> list2 = new ArrayList<>();

        PrefixBlockReader reader = new PrefixBlockReader(fp, input);
        while (true)
        {
            BytesRef term = reader.next();
            if (term == null)
            {
                break;
            }
            list2.add(BytesRef.deepCopyOf(term));
            System.out.println("  term="+term.utf8ToString());
        }
        input.close();

        assertEquals(terms, list2);
    }

    @Test
    public void testRando() throws Exception
    {
        for (int x = 0; x < 100; x++)
        {
            doRando();
        }
    }

    public void doRando() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        int count = nextInt(1, 1024);

        List<BytesRef> terms = new ArrayList();

        for (int x = 0; x < count; x++)
        {
            byte[] bytes = new byte[ThreadLocalRandom.current().nextInt(1, 10)];
            nextBytes(bytes);
            terms.add(new BytesRef(bytes));
        }

        Collections.sort(terms);

        IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
        PrefixBlockWriter writer = new PrefixBlockWriter();
        for (BytesRef term : terms)
        {
            writer.add(term);
        }

        long fp = writer.finish(out);
        out.close();

        final SharedIndexInput input = new SharedIndexInput(dir.openInput("test", IOContext.DEFAULT));

        List<BytesRef> list2 = new ArrayList<>();

        PrefixBlockReader reader = new PrefixBlockReader(fp, input);
        while (true)
        {
            BytesRef term = reader.next();
            if (term == null)
            {
                break;
            }
            list2.add(BytesRef.deepCopyOf(term));
            System.out.println("  term.length="+term.length);
        }
        input.close();

        System.out.println("terms.size="+terms.size()+" list2.size="+list2.size());

        assertEquals(terms, list2);
    }

    @Test
    public void testSeekUpper() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        List<BytesRef> terms = new ArrayList();
        terms.add(new BytesRef("aaaaaa"));
        terms.add(new BytesRef("aaaabb"));
        terms.add(new BytesRef("aaaabc"));
        terms.add(new BytesRef("aaaabg"));

        terms.add(new BytesRef("aaaabggg"));
        terms.add(new BytesRef("aaggbbbb"));
        terms.add(new BytesRef("aaggmmmmbggg"));
        terms.add(new BytesRef("ttttjjjjjjj"));

        terms.add(new BytesRef("tttzzzzzjjjjjjj"));

        IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
        PrefixBlockWriter writer = new PrefixBlockWriter();
        for (BytesRef term : terms)
        {
            writer.add(term);
        }

        long fp = writer.finish(out);
        out.close();

        final SharedIndexInput input = new SharedIndexInput(dir.openInput("test", IOContext.DEFAULT));

        PrefixBlockReader reader = new PrefixBlockReader(fp, input);
        BytesRef result = reader.seekUpper(new BytesRef("aaaabb"));
        int upperOrdinal = reader.getUpperOrdinal() - 2;
        assertEquals(0, upperOrdinal);

        BytesRef result2 = reader.seekUpper(new BytesRef("aaaabc"));
        int upperOrdinal2 = reader.getUpperOrdinal() - 2;
        assertEquals(0, upperOrdinal2);

        BytesRef result3 = reader.seekUpper(new BytesRef("aaggbbba"));
        int upperOrdinal3 = reader.getUpperOrdinal() - 2;
        assertEquals(1, upperOrdinal3);

        BytesRef result4 = reader.seekUpper(new BytesRef("tttzzzzzjjjjjjz"));

        System.out.println("upperTermsReader.count="+reader.upperTermsReader.count());

        int upperOrdinal4 = reader.getUpperOrdinal() - 2;
        assertEquals(2, upperOrdinal4);

        System.out.println("result3="+result.utf8ToString()+" upperOrdinal3="+upperOrdinal3);

        input.close();
    }

    @Test
    public void testSeek() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        List<BytesRef> terms = new ArrayList();
        terms.add(new BytesRef("aaaaaa"));
        terms.add(new BytesRef("aaaabb"));
        terms.add(new BytesRef("aaaabc"));
        terms.add(new BytesRef("aaaabg"));

        terms.add(new BytesRef("aaaabggg"));
        terms.add(new BytesRef("aaggbbbb"));
        terms.add(new BytesRef("aaggmmmmbggg"));
        terms.add(new BytesRef("ttttjjjjjjj"));

        terms.add(new BytesRef("tttzzzzzjjjjjjj"));

        IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
        PrefixBlockWriter writer = new PrefixBlockWriter();
        for (BytesRef term : terms)
        {
            writer.add(term);
        }

        long fp = writer.finish(out);
        out.close();

        final SharedIndexInput input = new SharedIndexInput(dir.openInput("test", IOContext.DEFAULT));

        PrefixBlockReader reader = new PrefixBlockReader(fp, input);

        BytesRef result0 = reader.seek(new BytesRef("aaggbbbb"));
        System.out.println("result0="+result0.utf8ToString());

        BytesRef result1 = reader.seek(new BytesRef("ttttjjjjjjj"));
        System.out.println("result1="+result1.utf8ToString());

        reader.close();
        input.close();
    }

    @Test
    public void testSeekRandom() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        List<BytesRef> terms = new ArrayList();
        int val = 0;
        for (int x = 0; x < 1024; x++)
        {
            val += nextInt(1, 10);
            terms.add(new BytesRef(String.format("%08d", val)));
        }

        TreeSet<BytesRef> termsSet = new TreeSet<>(terms);

        IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
        PrefixBlockWriter writer = new PrefixBlockWriter();
        for (BytesRef term : terms)
        {
            writer.add(term);
        }

        long fp = writer.finish(out);
        out.close();

        final SharedIndexInput input = new SharedIndexInput(dir.openInput("test", IOContext.DEFAULT));

        PrefixBlockReader reader = new PrefixBlockReader(fp, input);

        int idx = 0;

        while (true)
        {
            idx += nextInt(1, 10);

            if (idx >= terms.size())
            {
                break;
            }
            BytesRef target = terms.get(idx);

            BytesRef actual = termsSet.ceiling(target);
            BytesRef result0 = reader.seek(target);

            assertEquals("idx="+idx+" terms.size="+terms.size()+" actual="+actual.utf8ToString()+" result0="+result0, actual, result0);
        }

        reader.close();
        input.close();
    }
}
