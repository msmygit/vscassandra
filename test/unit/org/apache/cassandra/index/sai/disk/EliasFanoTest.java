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

package org.apache.cassandra.index.sai.disk;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

import org.junit.Test;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.sux4j.util.EliasFanoIndexedMonotoneLongBigList;

public class EliasFanoTest
{
    @Test
    public void test() throws Exception
    {
        long[] array = new long[] { 0, 10, 20 };
        final EliasFanoIndexedMonotoneLongBigList postings = new EliasFanoIndexedMonotoneLongBigList(LongArrayList.wrap(array));

        long result = postings.successorUnsafe(11);
        System.out.println("result="+result);

        Path file = Files.createTempFile("eliasfano", "test", new FileAttribute[0]);
        postings.dump(file.toFile().getAbsolutePath());

    }
}
