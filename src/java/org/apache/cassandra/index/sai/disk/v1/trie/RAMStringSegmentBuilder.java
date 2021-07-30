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

package org.apache.cassandra.index.sai.disk.v1.trie;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.io.BytesRefUtil;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.ram.RAMStringIndexer;
import org.apache.cassandra.index.sai.utils.NamedMemoryLimiter;
import org.apache.lucene.util.BytesRefBuilder;

public class RAMStringSegmentBuilder extends SegmentBuilder
{
    final RAMStringIndexer ramIndexer;

    final BytesRefBuilder stringBuffer = new BytesRefBuilder();

    public RAMStringSegmentBuilder(AbstractType<?> termComparator, NamedMemoryLimiter limiter)
    {
        super(termComparator, limiter);

        ramIndexer = new RAMStringIndexer(termComparator);
        totalBytesAllocated = ramIndexer.estimatedBytesUsed();
    }

    public boolean isEmpty()
    {
        return ramIndexer.rowCount == 0;
    }

    protected long addInternal(ByteBuffer term, int segmentRowId)
    {
        BytesRefUtil.copyBufferToBytesRef(term, stringBuffer);
        return ramIndexer.add(stringBuffer.get(), segmentRowId);
    }

    @Override
    protected SegmentMetadata.ComponentMetadataMap flushInternal(IndexDescriptor indexDescriptor, IndexContext columnContext) throws IOException
    {
        try (InvertedIndexWriter writer = new InvertedIndexWriter(indexDescriptor, columnContext.getIndexName(), true))
        {
            return writer.writeAll(ramIndexer.getTermsWithPostings());
        }
    }
}
