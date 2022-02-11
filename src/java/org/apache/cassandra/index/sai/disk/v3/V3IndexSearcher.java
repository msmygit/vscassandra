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

import com.google.common.base.MoreObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableQueryContext;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.PrimaryKeyMap;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.IndexSearcher;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Executes {@link Expression}s against the block terms index for an individual index segment.
 */
public class V3IndexSearcher extends IndexSearcher
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final BlockTerms.Reader reader;

    V3IndexSearcher(PrimaryKeyMap.Factory primaryKeyMapFactory,
                    V3PerIndexFiles perIndexFiles,
                    SegmentMetadata segmentMetadata,
                    IndexDescriptor indexDescriptor,
                    IndexContext indexContext) throws IOException
    {
        super(primaryKeyMapFactory, perIndexFiles, segmentMetadata, indexDescriptor, indexContext);

        reader = new BlockTerms.Reader(indexDescriptor, indexContext, perIndexFiles, segmentMetadata.componentMetadatas);
    }

    @Override
    public long indexFileCacheSize()
    {
        return reader.memoryUsage();
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

            PostingList postings = reader.search(lowerBound, lowerExclusive, upperBound, upperExclusive);
            return toIterator(postings, context, defer);
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
        FileUtils.closeQuietly(reader);
    }
}
