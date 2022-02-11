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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.lang3.mutable.MutableLong;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.disk.v1.trie.TrieTermsDictionaryWriter;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Builds an on-disk inverted index structure: terms dictionary and postings lists.
 */
@NotThreadSafe
public class V3InvertedIndexWriter implements Closeable, BlockTerms.Writer.TermRowIDCallback
{
    private final TrieTermsDictionaryWriter termsDictionaryWriter;
    private final PostingsWriter postingsWriter;
    private long postingsAdded;
    private final long termsOffset, postingsOffset;
    private final BytesRefBuilder lastTerm = new BytesRefBuilder();

    public V3InvertedIndexWriter(IndexDescriptor indexDescriptor, IndexContext indexContext, boolean segmented) throws IOException
    {
        this.termsDictionaryWriter = new TrieTermsDictionaryWriter(indexDescriptor, indexContext, segmented);
        this.postingsWriter = new PostingsWriter(indexDescriptor, indexContext, segmented);

        this.termsOffset = termsDictionaryWriter.getStartOffset();
        this.postingsOffset = postingsWriter.getStartOffset();
    }

    @Override
    public void added(BytesRef term, long rowid, boolean sameTermAsLast) throws IOException
    {
        if (lastTerm.length() > 0 && !term.equals(lastTerm.get()))
        {
            long fp = postingsWriter.finishPostings();
            if (fp >= 0)
            {
                termsDictionaryWriter.add(ByteComparable.fixedLength(lastTerm.get().bytes, lastTerm.get().offset, lastTerm.get().length), fp);
            }
        }

        postingsWriter.add(rowid);
        lastTerm.copyBytes(term);
    }

    @Override
    public void finish(SegmentMetadata.ComponentMetadataMap components) throws IOException
    {
        // if there are no new postings
        long fp = postingsWriter.finishPostings();
        if (fp >= 0)
        {
            termsDictionaryWriter.add(ByteComparable.fixedLength(lastTerm.get().bytes, lastTerm.get().offset, lastTerm.get().length), fp);
        }

        postingsAdded = postingsWriter.getTotalPostings();
        MutableLong footerPointer = new MutableLong();
        long termsRoot = termsDictionaryWriter.complete(footerPointer);
        postingsWriter.complete();

        long termsLength = termsDictionaryWriter.getFilePointer() - termsOffset;
        long postingsLength = postingsWriter.getFilePointer() - postingsOffset;

        Map<String,String> map = new HashMap<>(2);
        map.put(SAICodecUtils.FOOTER_POINTER, "" + footerPointer.getValue());

        // Postings list file pointers are stored directly in TERMS_DATA, so a root is not needed.
        components.put(IndexComponent.POSTING_LISTS, -1, postingsOffset, postingsLength);
        components.put(IndexComponent.TERMS_DATA, termsRoot, termsOffset, termsLength, map);
    }

    @Override
    public void close() throws IOException
    {
        postingsWriter.close();
        termsDictionaryWriter.close();
    }

    /**
     * @return total number of row IDs added to posting lists
     */
    public long getPostingsCount()
    {
        return postingsAdded;
    }
}
