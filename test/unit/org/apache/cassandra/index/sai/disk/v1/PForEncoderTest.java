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

import java.util.Arrays;

import org.junit.Test;

import org.apache.lucene.codecs.lucene84.PForEncoder;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static org.junit.Assert.assertArrayEquals;

public class PForEncoderTest
{
    @Test
    public void testPFoR() throws Exception
    {
        PForEncoder encoder = new PForEncoder();
        long[] longs = new long[128];
        long[] longsOrig = null;

        long last = 0;
        for (int x=0; x < longs.length; x++)
        {
            longs[x] = x - last;
            //longsOrig[x] = x;
            last = x;
        }

        longsOrig = Arrays.copyOf(longs, longs.length);

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput out = dir.createOutput("name", IOContext.DEFAULT);
        long fp = out.getFilePointer();
        encoder.encodePFoR(longs, out);
        out.close();

        IndexInput in = dir.openInput("name", IOContext.DEFAULT);
        long[] longs2 = new long[128];
        encoder.decodePFoR(longs2, in);
        in.close();

        System.out.println("longsOrig=" + Arrays.toString(longsOrig));
        System.out.println("longs2=" + Arrays.toString(longs2));

        assertArrayEquals(longsOrig, longs2);
    }

    @Test
    public void test() throws Exception
    {
        PForEncoder encoder = new PForEncoder();
        long[] longs = new long[128];
        long[] longsOrig = new long[128];//rrays.copyOf(longs, longs.length);

        long last = 0;
        for (int x=0; x < longs.length; x++)
        {
            longs[x] = x - last;
            longsOrig[x] = x;
            last = x;
        }

        ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexOutput out = dir.createOutput("name", IOContext.DEFAULT);
        long fp = out.getFilePointer();
        encoder.encodeFoRDeltas(longs, out);
        out.close();

        IndexInput in = dir.openInput("name", IOContext.DEFAULT);
        long[] longs2 = new long[128];
        encoder.decodeFoRDeltaAndPrefixSum(fp, longs2, in);
        in.close();

        System.out.println("longsOrig=" + Arrays.toString(longsOrig));
        System.out.println("longs2=" + Arrays.toString(longs2));

        assertArrayEquals(longsOrig, longs2);
    }
}
