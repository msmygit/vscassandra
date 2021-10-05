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

package org.apache.cassandra.index.sai.disk.v2;

import org.junit.Test;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexFileProvider;
import org.apache.cassandra.index.sai.disk.v2.blockindex.FileValidator;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.lucene.store.IndexOutput;


public class FileValidatorTest extends NdiRandomizedTest
{
    @Test
    public void testBig() throws Exception
    {
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        IndexContext indexContext = SAITester.createIndexContext("column", "test", UTF8Type.instance);

        IndexOutput out = indexDescriptor.openPerIndexOutput(IndexComponent.TERMS_DATA, indexContext);
        byte[] buffer = new byte[1];
        for (int x = 0; x < 10_000; x++)
        {
            nextBytes(buffer);
            out.writeByte(buffer[0]);
        }
        out.close();

        BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);

        FileValidator.FileInfo fileInfo = FileValidator.generate(fileProvider.openValuesInput(false));

        System.out.println("fileInfo=" + fileInfo);

        FileValidator.FileInfo fileInfo2 = FileValidator.generate(fileProvider.openValuesInput(false));

        assertEquals(fileInfo, fileInfo2);
    }

    @Test
    public void testSmall() throws Exception
    {
        IndexDescriptor indexDescriptor = newIndexDescriptor();

        IndexContext indexContext = SAITester.createIndexContext("column", "test", UTF8Type.instance);

        IndexOutput out = indexDescriptor.openPerIndexOutput(IndexComponent.TERMS_DATA, indexContext);
        byte[] buffer = new byte[1];
        for (int x = 0; x < 500; x++)
        {
            nextBytes(buffer);
            out.writeByte(buffer[0]);
        }
        out.close();

        BlockIndexFileProvider fileProvider = new PerIndexFileProvider(indexDescriptor, indexContext);

        FileValidator.FileInfo fileInfo = FileValidator.generate(fileProvider.openValuesInput(false));

        System.out.println("fileInfo=" + fileInfo);

        FileValidator.FileInfo fileInfo2 = FileValidator.generate(fileProvider.openValuesInput(false));

        assertEquals(fileInfo, fileInfo2);
    }
}
