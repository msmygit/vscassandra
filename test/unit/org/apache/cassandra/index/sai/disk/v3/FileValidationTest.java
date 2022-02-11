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

package org.apache.cassandra.index.sai.disk.v3;

import org.junit.Test;

import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static org.apache.cassandra.index.sai.disk.v3.FileValidation.BLOCK_SIZE;

public class FileValidationTest extends SaiRandomizedTest
{
    @Test
    public void testSmallFile() throws Exception
    {
        final ByteBuffersDirectory dir = new ByteBuffersDirectory();
        long rootFP = -1;
        try (IndexOutput out = dir.createOutput("blob", IOContext.DEFAULT))
        {
            // add bytes ahead to simulate a segmented file write
            byte[] bytes = new byte[nextInt(0, 2 * BLOCK_SIZE)];
            nextBytes(bytes);
            out.writeBytes(bytes, bytes.length);

            rootFP = out.getFilePointer();
            bytes = new byte[nextInt(0, 2 * BLOCK_SIZE)];
            nextBytes(bytes);
            out.writeBytes(bytes, bytes.length);
        }

        try (IndexInput input = dir.openInput("blob", IOContext.DEFAULT))
        {
            FileValidation.Meta meta = FileValidation.createValidate(rootFP, input);
            assertTrue(FileValidation.validate(input, meta));
        }
    }

    @Test
    public void testLargeFile() throws Exception
    {
        final ByteBuffersDirectory dir = new ByteBuffersDirectory();
        long rootFP = -1;
        try (IndexOutput out = dir.createOutput("blob", IOContext.DEFAULT))
        {
            // add bytes ahead to simulate a segmented file write
            byte[] bytes = new byte[nextInt(0, 2 * BLOCK_SIZE)];
            nextBytes(bytes);
            out.writeBytes(bytes, bytes.length);

            rootFP = out.getFilePointer();
            bytes = new byte[nextInt(2 * BLOCK_SIZE, BLOCK_SIZE * 50)];
            nextBytes(bytes);
            out.writeBytes(bytes, bytes.length);
        }

        try (IndexInput input = dir.openInput("blob", IOContext.DEFAULT))
        {
            FileValidation.Meta meta = FileValidation.createValidate(rootFP, input);
            assertTrue(FileValidation.validate(input, meta));
        }
    }
}
