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

package org.apache.cassandra.index.sai.disk.v2.primarykey;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SharedIndexInput2;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;

import static org.junit.Assert.assertEquals;

public class PrefixBytesTest
{
    @Test
    public void testSkipTo() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
        PrefixBytesWriter writer = new PrefixBytesWriter();

        List<BytesRef> terms = new ArrayList<>();
        terms.add(new BytesRef("aaaaaa"));
        terms.add(new BytesRef("aaaabb"));
        terms.add(new BytesRef("aaaabc"));
        terms.add(new BytesRef("aaaabg"));
        terms.add(new BytesRef("aaaabggg"));

        for (BytesRef term : terms)
        {
            writer.add(term);
        }

        writer.finish(out);
        out.close();

        SharedIndexInput2 input = new SharedIndexInput2(dir.openInput("test", IOContext.DEFAULT));

        PrefixBytesReader reader = new PrefixBytesReader(input);
        reader.reset(0);

        List<BytesRef> terms2 = new ArrayList<>();
        for (int x=0; x < terms.size(); x++)
        {
            BytesRef term = reader.seek(x);
            terms2.add(BytesRef.deepCopyOf(term));
        }

        assertEquals(terms, terms2);
        input.close();
    }
}
