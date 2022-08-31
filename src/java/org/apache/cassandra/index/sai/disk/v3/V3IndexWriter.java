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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableLong;

import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.TermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryWriter;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.BytesRef;

/**
 * Writes an equality and range index.
 */
public class V3IndexWriter
{
    private final TrieTermsDictionaryWriter termsDictionaryWriter;
    private final PostingsWriter postingsWriter;
    private final BlockTerms.Writer blockTermsWriter;
    private long postingsAdded;

    public V3IndexWriter(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean segmented) throws IOException
    {
        blockTermsWriter = new BlockTerms.Writer(indexDescriptor, indexContext, segmented);

        this.termsDictionaryWriter = new TrieTermsDictionaryWriter(indexDescriptor, indexContext, segmented);
        this.postingsWriter = new PostingsWriter(indexDescriptor, indexContext, segmented);
    }

    public long getPostingsAdded()
    {
        return postingsAdded;
    }

    public SegmentMetadata.ComponentMetadataMap writeAll(TermsIterator terms) throws IOException
    {
        // Terms and postings writers are opened in append mode with pointers at the end of their respective files.
        long termsOffset = termsDictionaryWriter.getStartOffset();
        long postingsOffset = postingsWriter.getStartOffset();

        long totalPostings = 0;

        while (terms.hasNext())
        {
            final ByteComparable term = terms.next();

            ByteBuffer buffer = Int32Type.instance.fromComparableBytes(term.asPeekableBytes(ByteComparable.Version.OSS41), ByteComparable.Version.OSS41);

            int value = Int32Type.instance.compose(buffer);
            System.out.println("intvalue="+value);

            try (PostingList postings = terms.postings())
            {
                int size = 0;
                long segmentRowId;
                while ((segmentRowId = postings.nextPosting()) != PostingList.END_OF_STREAM)
                {
                    System.out.println("segmentRowId="+segmentRowId);
                    blockTermsWriter.add(term, segmentRowId);
                    postingsWriter.add(segmentRowId);
                    size++;
                    totalPostings++;
                }
                final long offset = postingsWriter.finishPostings();
                if (offset >= 0)
                    termsDictionaryWriter.add(term, offset);
            }
        }
        postingsAdded = postingsWriter.getTotalPostings();
        MutableLong footerPointer = new MutableLong();
        long termsRoot = termsDictionaryWriter.complete(footerPointer);
        postingsWriter.complete();

        // close terms related objects
        termsDictionaryWriter.close();
        postingsWriter.close();

        long termsLength = termsDictionaryWriter.getFilePointer() - termsOffset;
        long postingsLength = postingsWriter.getFilePointer() - postingsOffset;

        SegmentMetadata.ComponentMetadataMap components = new SegmentMetadata.ComponentMetadataMap();

        Map<String,String> map = new HashMap<>(2);
        map.put(SAICodecUtils.FOOTER_POINTER, "" + footerPointer.getValue());

        try
        {
            blockTermsWriter.finish(components);
        }
        catch (Exception ex)
        {
            throw Throwables.cleaned(ex);
        }

        // Postings list file pointers are stored directly in TERMS_DATA, so a root is not needed.
        components.put(IndexComponent.POSTING_LISTS, -1, postingsOffset, postingsLength);
        components.put(IndexComponent.TERMS_DATA, termsRoot, termsOffset, termsLength, map);

        return components;
    }
}
