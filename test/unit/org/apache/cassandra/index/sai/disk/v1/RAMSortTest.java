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

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import org.apache.cassandra.db.tries.MemtableTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class RAMSortTest extends NdiRandomizedTest
{
    private static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    @Test
    public void test() throws Exception
    {
        MemtableTrie trie = new MemtableTrie<>(BufferType.OFF_HEAP);

        for (int x=0; x < 100; x++)
        {
            int length = nextInt(1, 40);

            String generatedString = RandomStringUtils.random(length, true, true);
            ByteComparable byteComparable = ByteComparable.of(generatedString);
            int length2 = ByteComparable.length(byteComparable, ByteComparable.Version.OSS41);

            MemtableTrie.UpsertTransformer transformer = (existing, update) -> existing;

            if (length2 <= MAX_RECURSIVE_KEY_LENGTH)
            {
                trie.putRecursive(byteComparable, new Object(), transformer);
            }
            else
            {
                trie.apply(Trie.singleton(byteComparable, new Object()), transformer);
            }
        }
    }
}
