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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.ConjunctionPostingList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v2.SupplierWithIO;
import org.apache.cassandra.index.sai.disk.v2.postingscache.DocIdSetPostingList;
import org.apache.cassandra.index.sai.disk.v2.postingscache.PostingsCache;
import org.apache.cassandra.index.sai.disk.v2.postingscache.PostingsKey;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.RoaringDocIdSet;

import static org.apache.cassandra.index.sai.metrics.QueryEventListener.PostingListEventListener.NO_OP;

/**
 * Executes {@link Expression}s against the trie-based terms dictionary for an individual index segment.
 */
public class InvertedIndexSearcher extends IndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final TermsReader reader;
    private final QueryEventListener.TrieIndexEventListener perColumnEventListener;

    private final boolean missingValuesExists;

    InvertedIndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                          PerIndexFiles perIndexFiles,
                          SegmentMetadata segmentMetadata,
                          IndexDescriptor indexDescriptor,
                          IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext);

        long root = metadata.getIndexRoot(IndexComponent.TERMS_DATA);
        assert root >= 0;

        missingValuesExists = indexDescriptor.fileFor(IndexComponent.MISSING_VALUES, indexContext).exists();

        perColumnEventListener = (QueryEventListener.TrieIndexEventListener) indexContext.getColumnQueryMetrics();

        Map<String, String> map = metadata.componentMetadatas.get(IndexComponent.TERMS_DATA).attributes;
        String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

        reader = new TermsReader(indexContext,
                                 indexFiles.termsData().sharedCopy(),
                                 indexFiles.postingLists().sharedCopy(),
                                 root, footerPointer);
    }

    public SupplierWithIO<PostingList> missingValuesPostings()
    {
        if (indexDescriptor.fileFor(IndexComponent.MISSING_VALUES, indexContext).exists())
        {
            return () ->
            {
                IndexInput indexInput = indexDescriptor.openPerIndexInput(IndexComponent.MISSING_VALUES, indexContext);

                SegmentMetadata.ComponentMetadata componentMetadata = metadata.componentMetadatas.get(IndexComponent.MISSING_VALUES);
                PostingsReader reader = new PostingsReader(indexInput, componentMetadata.offset, NO_OP);
                return reader;
            };
        }
        return null;
    }

    @Override
    public long indexFileCacheSize()
    {
        // trie has no pre-allocated memory.
        // TODO: Is this still the case now the trie isn't using the chunk cache?
        return 0;
    }

    @Override
    @SuppressWarnings("resource")
    public RangeIterator search(Expression exp, SSTableQueryContext context, boolean defer) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (!exp.getOp().isEquality())
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression: " + exp));

        final ByteComparable term = ByteComparable.fixedLength(exp.lower.value.encoded);
        QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context.queryContext, perColumnEventListener);

        Pair<Long, SupplierWithIO<PostingList>> postingListCallable = reader.exactMatch(term, listener, context.queryContext);

//        if (postingListCallable != null && missingValuesExists)
//        {
//            PostingsKey key = new PostingsKey(this,
//                                              indexContext.getIndexName(),
//                                              postingListCallable.left.longValue(),
//                                              true);
//
//            PostingsCache.PostingsFactory factory = PostingsCache.INSTANCE.get(key);
//
//
//            if (factory == null)
//            {
//                PostingList missingValuePostings = missingValuesPostings();
//                PostingList postings = postingListCallable.right.get();
//
//                RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder((int)metadata.maxSSTableRowId + 1);
//
//                // AND the 2 posting lists and cache the result
//
//                // intersect with incongruentTS postings
//                try (ConjunctionPostingList incongruentAND = new ConjunctionPostingList(Lists.newArrayList(missingValuePostings, postings)))
//                {
//                    while (true)
//                    {
//                        long rowid = incongruentAND.nextPosting();
//                        if (rowid == PostingList.END_OF_STREAM) break;
//                        System.out.println("incongruent rowid="+rowid);
//                        builder.add((int)rowid);
//                    }
//                }
//                RoaringDocIdSet docIdSet = builder.build();
//                PostingsCache.INSTANCE.put(key, new PostingsCache.PostingsFactory()
//                {
//                    @Override
//                    public int sizeInBytes()
//                    {
//                        return (int)docIdSet.ramBytesUsed();
//                    }
//
//                    @Override
//                    public PostingList postings()
//                    {
//                        try
//                        {
//                           return new DocIdSetPostingList(docIdSet.iterator());
//                        }
//                        catch (IOException ioex)
//                        {
//                            throw new RuntimeException(ioex);
//                        }
//                    }
//                });
//            }
//        }

        if (postingListCallable == null) return RangeIterator.empty();

        PostingList postingList = postingListCallable.right.get();

        return toIterator(postingList, postingListCallable.right, context, defer);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                          .add("indexContext", indexContext)
                          .toString();
    }

    @Override
    public void close()
    {
        reader.close();
    }
}
