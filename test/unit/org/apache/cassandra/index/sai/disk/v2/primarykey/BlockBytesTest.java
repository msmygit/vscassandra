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
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SharedIndexInput2;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.junit.Assert.assertEquals;

public class BlockBytesTest
{
    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        IndexOutput out = dir.createOutput("test", IOContext.DEFAULT);
        BlockBytesWriter writer = new BlockBytesWriter();

        List<BytesRef> terms = new ArrayList();

        for (int x = 0; x < BlockBytesWriter.BLOCK_SIZE; x++)
        {
            byte[] bytes = new byte[4];
            NumericUtils.intToSortableBytes(x, bytes, 0);
            terms.add(new BytesRef(bytes));
        }

        for (BytesRef term : terms)
        {
            writer.add(term);
        }

        writer.finish(out);
        out.close();

        SharedIndexInput2 input = new SharedIndexInput2(dir.openInput("test", IOContext.DEFAULT));

        BlockBytesReader reader = new BlockBytesReader(input);
        reader.reset(0);

        for (int x = 0; x < 1000; x++)
        {
            int idx = ThreadLocalRandom.current().nextInt(terms.size());
            BytesRef term = reader.seek(idx);
            int value = NumericUtils.sortableBytesToInt(term.bytes, 0);
            assertEquals(idx, value);
        }

        List<BytesRef> terms2 = new ArrayList<>();

        for (int x = 0; x < terms.size(); x++)
        {
            BytesRef term = reader.seek(x);
            terms2.add(BytesRef.deepCopyOf(term));
        }

        assertEquals(terms, terms2);
        input.close();
    }
}
