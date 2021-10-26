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
import java.util.Collections;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.SaiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import static org.apache.cassandra.index.sai.SAITester.createIndexContext;

public class PrimaryKeyTest extends SaiRandomizedTest
{
    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testSeekToTerm() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();
        IndexContext indexContext = createIndexContext("index_test1", UTF8Type.instance);

        SortedBytesReader.Meta meta = null;

        List<BytesRef> terms = new ArrayList();

        try (IndexOutputWriter minKeyWriter = descriptor.openPerIndexOutput(IndexComponent.TERMS_INDEX, indexContext);
             IndexOutputWriter bytesWriter = descriptor.openPerIndexOutput(IndexComponent.TERMS_DATA, indexContext);
             IndexOutputWriter blockFPWriter = descriptor.openPerIndexOutput(IndexComponent.KD_TREE_POSTING_LISTS, indexContext))
        {
            SortedBytesWriter writer = new SortedBytesWriter(minKeyWriter,
                                                             bytesWriter,
                                                             blockFPWriter);

            for (int x = 0; x < 1000 * 4; x++)
            {
                byte[] bytes = new byte[4];
                NumericUtils.intToSortableBytes(x, bytes, 0);
                terms.add(new BytesRef(bytes));
            }

            for (BytesRef term : terms)
            {
                writer.add(term);
            }

            meta = writer.finish();
        }

        try (FileHandle minKeyHandle = descriptor.createPerIndexFileHandle(IndexComponent.TERMS_INDEX, indexContext);
             IndexInput bytesInput = descriptor.openPerIndexInput(IndexComponent.TERMS_DATA, indexContext);
             IndexInput blockFPInput = descriptor.openPerIndexInput(IndexComponent.KD_TREE_POSTING_LISTS, indexContext))
        {
            try (SortedBytesReader reader = new SortedBytesReader(minKeyHandle,
                                                                  bytesInput,
                                                                  blockFPInput,
                                                                  meta))
            {
                for (int x = 0; x < terms.size(); x++)
                {
                    byte[] bytes = new byte[4];
                    int target = nextInt(terms.size());
                    NumericUtils.intToSortableBytes(target, bytes, 0);
                    long pointId = reader.seekTo(new BytesRef(bytes));
                    assertEquals(target, pointId);
                }
            }
        }
    }

    @Test
    public void testSeekToPointId() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();
        IndexContext indexContext = createIndexContext("index_test1", UTF8Type.instance);

        SortedBytesReader.Meta meta = null;

        List<BytesRef> terms = new ArrayList();

        try (IndexOutputWriter minKeyWriter = descriptor.openPerIndexOutput(IndexComponent.TERMS_INDEX, indexContext);
             IndexOutputWriter bytesWriter = descriptor.openPerIndexOutput(IndexComponent.TERMS_DATA, indexContext);
             IndexOutputWriter blockFPWriter = descriptor.openPerIndexOutput(IndexComponent.KD_TREE_POSTING_LISTS, indexContext))
        {
            SortedBytesWriter writer = new SortedBytesWriter(minKeyWriter,
                                                             bytesWriter,
                                                             blockFPWriter);

            for (int x = 0; x < 1000 * 4; x++)
            {
                byte[] bytes = new byte[4];
                NumericUtils.intToSortableBytes(x, bytes, 0);
                terms.add(new BytesRef(bytes));
            }

            for (BytesRef term : terms)
            {
                writer.add(term);
            }

            meta = writer.finish();
        }

        List<BytesRef> reverseTerms = new ArrayList(terms);
        Collections.reverse(reverseTerms);

        try (FileHandle minKeyHandle = descriptor.createPerIndexFileHandle(IndexComponent.TERMS_INDEX, indexContext);
             IndexInput bytesInput = descriptor.openPerIndexInput(IndexComponent.TERMS_DATA, indexContext);
             IndexInput blockFPInput = descriptor.openPerIndexInput(IndexComponent.KD_TREE_POSTING_LISTS, indexContext))
        {
            try (SortedBytesReader reader = new SortedBytesReader(minKeyHandle,
                                                                  bytesInput,
                                                                  blockFPInput,
                                                                  meta))
            {
                BytesRef term = reader.seekTo(10);
                int value = NumericUtils.sortableBytesToInt(term.bytes, 0);
                assertEquals(10, value);

                term = reader.seekTo(2060);
                value = NumericUtils.sortableBytesToInt(term.bytes, 0);
                assertEquals(2060, value);

                // random seek to and verify the numbers are the same
                for (int x = 0; x < 1000; x++)
                {
                    int target = nextInt(terms.size());

                    term = reader.seekTo(target);
                    value = NumericUtils.sortableBytesToInt(term.bytes, 0);
                    assertEquals(target, value);
                }

                List<BytesRef> terms2 = new ArrayList<>();
                for (int x = 0; x < terms.size(); x++)
                {
                    term = reader.seekTo(x);
                    terms2.add(BytesRef.deepCopyOf(term));
                }
                assertEquals(terms, terms2);

                // reverse seek
                List<BytesRef> revTerms2 = new ArrayList<>();
                for (int x = terms.size() - 1; x >= 0; x--)
                {
                    term = reader.seekTo(x);
                    revTerms2.add(BytesRef.deepCopyOf(term));
                }
                assertEquals(reverseTerms, revTerms2);

                int numCheck = nextInt(10, 1000);
                for (int x = 0; x < numCheck; x++)
                {
                    int target = nextInt(terms.size());

                    term = reader.seekTo(target);
                    value = NumericUtils.sortableBytesToInt(term.bytes, 0);
                    assertEquals(target, value);
                }
            }
        }
    }

    @Test
    public void testLexicographicException() throws Exception
    {
        IndexDescriptor descriptor = newIndexDescriptor();
        IndexContext indexContext = createIndexContext("index_test1", UTF8Type.instance);

        SortedBytesReader.Meta meta = null;

        try (IndexOutputWriter minKeyWriter = descriptor.openPerIndexOutput(IndexComponent.TERMS_INDEX, indexContext);
             IndexOutputWriter bytesWriter = descriptor.openPerIndexOutput(IndexComponent.TERMS_DATA, indexContext);
             IndexOutputWriter blockFPWriter = descriptor.openPerIndexOutput(IndexComponent.KD_TREE_POSTING_LISTS, indexContext))
        {
            SortedBytesWriter writer = new SortedBytesWriter(minKeyWriter,
                                                             bytesWriter,
                                                             blockFPWriter);

            writer.add(new BytesRef("zzzzz"));

            expectedException.expect(IllegalArgumentException.class);
            
            writer.add(new BytesRef("aaaaaa"));

            meta = writer.finish();
        }
    }
}
