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
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import static org.junit.Assert.assertEquals;

public class PrefixBlockTest
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
            BytesRef term = reader.next(null);
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
}
