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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.TermsReader;
import org.apache.cassandra.index.sai.metrics.MulticastQueryEventListeners;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.SAICodecUtils;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Executes {@link Expression}s against the block terms index for an individual index segment.
 */
public class V3IndexSearcher extends IndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BlockTerms.Reader blockTermsReader;
    private final TermsReader termsReader;
    //private final QueryEventListener.TrieIndexEventListener perColumnEventListener;

    V3IndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                    V3PerIndexFiles perIndexFiles,
                    SegmentMetadata segmentMetadata,
                    IndexDescriptor indexDescriptor,
                    IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext);

        long root = metadata.getIndexRoot(IndexComponent.TERMS_DATA);
        assert root >= 0;

        //perColumnEventListener = (QueryEventListener.TrieIndexEventListener)indexContext.getColumnQueryMetrics();

        Map<String,String> map = metadata.componentMetadatas.get(IndexComponent.TERMS_DATA).attributes;
        String footerPointerString = map.get(SAICodecUtils.FOOTER_POINTER);
        long footerPointer = footerPointerString == null ? -1 : Long.parseLong(footerPointerString);

        termsReader = new TermsReader(indexContext,
                                      indexFiles.termsData().sharedCopy(),
                                      indexFiles.postingLists().sharedCopy(),
                                      root,
                                      footerPointer);

        blockTermsReader = new BlockTerms.Reader(indexDescriptor, indexContext, perIndexFiles, segmentMetadata.componentMetadatas);
    }

    @Override
    public long indexFileCacheSize()
    {
        return blockTermsReader.memoryUsage();
    }

    @Override
    @SuppressWarnings("resource")
    public RangeIterator search(Expression exp, SSTableQueryContext context, boolean defer) throws IOException
    {
        if (logger.isTraceEnabled())
            logger.trace(indexContext.logMessage("Searching on expression '{}'..."), exp);

        if (exp.getOp().isEqualityOrRange())
        {
            // TODO: opto for match all eg. lower and upper null
            ByteComparable lowerBound = null;
            boolean lowerExclusive = false;
            if (exp.lower != null)
            {
                lowerExclusive = !exp.lower.inclusive;

                if (TypeUtil.isLiteral(exp.validator))
                    lowerBound = ByteComparable.fixedLength(exp.lower.value.encoded);
                else
                    lowerBound = toComparableBytes(exp.lower.value.encoded, exp.validator);
            }

            ByteComparable upperBound = null;
            boolean upperExclusive = false;
            if (exp.upper != null)
            {
                upperExclusive = !exp.upper.inclusive;

                if (TypeUtil.isLiteral(exp.validator))
                    upperBound = ByteComparable.fixedLength(exp.upper.value.encoded);
                else
                    upperBound = toComparableBytes(exp.upper.value.encoded, exp.validator);
            }

            // if it's an equality query use the terms reader index
            if (exp.getOp().isEquality())
            {
                //final ByteComparable term = ByteComparable.fixedLength(exp.lower.value.encoded);
                //QueryEventListener.TrieIndexEventListener listener = MulticastQueryEventListeners.of(context.queryContext, perColumnEventListener);

                //QueryEventListener.TrieIndexEventListener listener = QueryEventListener.TrieIndexEventListener.

                // TODO: figure out the listener metrics
                final PostingList postingList = termsReader.exactMatch(lowerBound, null, context.queryContext);
                return toIterator(postingList, context, defer);
            }
            else
            {
                // postings may be null, handled by toIterator
                final PostingList postings = blockTermsReader.search(lowerBound, lowerExclusive, upperBound, upperExclusive);
                return toIterator(postings, context, defer);
            }
        }
        else
        {
            throw new IllegalArgumentException(indexContext.logMessage("Unsupported expression during index query: " + exp));
        }
    }

    private static ByteComparable toComparableBytes(ByteBuffer value, AbstractType<?> type)
    {
        byte[] buffer = new byte[TypeUtil.fixedSizeOf(type)];
        TypeUtil.toComparableBytes(value, type, buffer);
        return ByteComparable.fixedLength(buffer);
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
        FileUtils.closeQuietly(termsReader);
        FileUtils.closeQuietly(blockTermsReader);
    }
}
