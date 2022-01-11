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

package org.apache.cassandra.index.sai.disk.v1.kdtree;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

public class BKDWriterTest extends SaiRandomizedTest
{
    @Test
    public void test() throws Throwable
    {
        final BKDTreeRamBuffer buffer = new BKDTreeRamBuffer(Integer.BYTES);
        byte[] scratch = new byte[4];
        for (int docID = 0; docID < 4; docID++)
        {
            NumericUtils.intToSortableBytes(docID, scratch, 0);
            buffer.addPackedValue(docID, new BytesRef(scratch));
        }
        BKDWriter writer = new BKDWriter(100, Integer.BYTES, 2, 100);
        CRCIndexOutput output = new CRCIndexOutput("");
        writer.writeField(output, buffer.asPointValues(), null);
    }

    public static class CRCIndexOutput extends RAMIndexOutput
    {
        public CRCIndexOutput(String name)
        {
            super(name);
        }

        @Override
        public long getChecksum() throws IOException
        {
            return 0L;
        }
    }
}
