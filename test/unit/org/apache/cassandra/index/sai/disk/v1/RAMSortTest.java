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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.base.Charsets;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.index.sai.disk.RAMStringIndexer;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.*;

public class RAMSortTest extends NdiRandomizedTest
{
    private static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    @Test
    public void test() throws Exception
    {
        for (int x=0; x < 50; x++)
        {
            doTest();
        }
    }

    public void doTest() throws Exception
    {
        MemtableTrie trie = new MemtableTrie<>(BufferType.OFF_HEAP);
        RAMStringIndexer indexer = new RAMStringIndexer(UTF8Type.instance);

        int count = nextInt(1, 1000);

        System.out.println("count="+count);

        for (int x=0; x < count; x++)
        {
            int length = nextInt(11, 40);

            //String generatedString = RandomStrings.randomAsciiOfLength(getRandom(), 10);//RandomStrings.randomRealisticUnicodeOfLengthBetween(getRandom(), 10, length);
            String generatedString = RandomStrings.randomRealisticUnicodeOfLengthBetween(getRandom(), 10, length);

            System.out.println("generatedString="+generatedString);

            indexer.add(new BytesRef(generatedString), x);

            final ByteBuffer strByteBuffer = UTF8Type.instance.decompose(generatedString);
            ByteComparable byteComparable = ByteComparable.fixedLength(strByteBuffer);
//
//             .map(UTF8Type.instance::decompose)
//                .map(ByteComparable::fixedLength)
//
//            ByteComparable byteComparable = (version) -> UTF8Type.instance.asComparableBytes(strByteBuffer, ByteComparable.Version.OSS41);

            int length2 = ByteComparable.length(byteComparable, ByteComparable.Version.OSS41);

            System.out.println("strByteBuffer.limit="+strByteBuffer.limit()+" length2="+length2);

            MemtableTrie.UpsertTransformer transformer = (existing, update) -> {
                if (existing == null)
                {
                    existing = new Object();
                }
                return existing;
            };

            if (length2 <= MAX_RECURSIVE_KEY_LENGTH)
            {
                trie.putRecursive(byteComparable, new Object(), transformer);
            }
            else
            {
                trie.apply(Trie.singleton(byteComparable, new Object()), transformer);
            }
        }

        TermsIterator termsIterator = indexer.getTermsWithPostings();

        Iterator<Map.Entry<ByteComparable, Object>> iterator = trie.entrySet().iterator();
        while (iterator.hasNext() && termsIterator.hasNext())
        {
            Map.Entry<ByteComparable, Object> entry = iterator.next();
            ByteComparable term2 = termsIterator.next();

            String asString1 = entry.getKey().byteComparableAsString(ByteComparable.Version.OSS41);
            String asString2 = term2.byteComparableAsString(ByteComparable.Version.OSS41);

            byte[] bytes = ByteSourceInverse.readBytes(entry.getKey().asPeekableBytes(ByteComparable.Version.OSS41));
            System.out.println("bytes.length="+bytes.length);

            System.out.println("asString1='"+asString1+"'");
            System.out.println("asString2='"+asString2+"'");

            int length = ByteComparable.length(entry.getKey(), ByteComparable.Version.OSS41);
            int length2 = ByteComparable.length(term2, ByteComparable.Version.OSS41);

            System.out.println("length="+length+" length2="+length2);
            String str = toString(entry.getKey());

            String str2 = toString(term2);
            System.out.println("str='"+str+"'");
            System.out.println("str2='"+str2+"'");

//            ByteSource s1 = entry.getKey().asComparableBytes(ByteComparable.Version.OSS41);
//            ByteSource s2 = term2.asComparableBytes(ByteComparable.Version.OSS41);
//
////            if (s1 == null || s2 == null)
////                return Boolean.compare(s1 != null, s2 != null);
//            int c = 0;
//            while (true)
//            {
//                int b1 = s1.next();
//                int b2 = s2.next();
//                //System.out.println("b1="+b1+" b2="+b2);
//                int cmp = Integer.compare(b1, b2);
//                if (cmp != 0 && b1 == 0 && b2 == -1)
//                {
//                   break;
//                }
//
//                if (b1 == ByteSource.END_OF_STREAM)
//                    break;
//                c++;
//            }

            int cmp = ByteComparable.compare(entry.getKey(), term2, ByteComparable.Version.OSS41);
            assertEquals(0, cmp);
        }

        assertFalse(iterator.hasNext());
        while (termsIterator.hasNext())
        {
            ByteComparable term2 = termsIterator.next();
            String str = toString(term2);
            System.out.println("laststrs="+str);
        }
        //assertFalse(termsIterator.hasNext());
    }

    public static String toString(ByteComparable term2)
    {
        int length2 = ByteComparable.length(term2, ByteComparable.Version.OSS41);
        byte[] bytes = new byte[length2];
        ByteSource source = term2.asComparableBytes(ByteComparable.Version.OSS41);
        ByteBufferUtil.toBytes(source, bytes);
        return new String(bytes, Charsets.UTF_8);
    }
}
