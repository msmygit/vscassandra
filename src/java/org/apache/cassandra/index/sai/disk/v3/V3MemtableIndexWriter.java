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
import java.util.Collections;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.MemtableIndexWriter;
import org.apache.cassandra.index.sai.disk.v1.MetadataWriter;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;

public class V3MemtableIndexWriter extends MemtableIndexWriter
{
    public V3MemtableIndexWriter(MemtableIndex memtable, IndexDescriptor indexDescriptor, IndexContext indexContext, RowMapping rowMapping)
    {
        super(memtable, indexDescriptor, indexContext, rowMapping);
    }

    protected long flush(DecoratedKey minKey, DecoratedKey maxKey, AbstractType<?> termComparator, MemtableTermsIterator terms, long maxSegmentRowId) throws IOException
    {
        final BlockTerms.Writer writer = new BlockTerms.Writer(indexDescriptor, indexContext);

        final SegmentMetadata.ComponentMetadataMap indexMetas = writer.writeAll(terms);
        final long numRows = writer.pointCount();

        // If no rows were written we need to delete any created column index components
        // so that the index is correctly identified as being empty (only having a completion marker)
        if (numRows == 0)
        {
            indexDescriptor.deleteColumnIndex(indexContext);
            return 0;
        }

        // During index memtable flush, the data is sorted based on terms.
        final SegmentMetadata metadata = new SegmentMetadata(0,
                                                             numRows,
                                                             terms.getMinSSTableRowId(),
                                                             terms.getMaxSSTableRowId(),
                                                             indexDescriptor.primaryKeyFactory.createPartitionKeyOnly(minKey),
                                                             indexDescriptor.primaryKeyFactory.createPartitionKeyOnly(maxKey),
                                                             terms.getMinTerm(),
                                                             terms.getMaxTerm(),
                                                             indexMetas);

        try (final MetadataWriter metadataWriter = new MetadataWriter(indexDescriptor.openPerIndexOutput(IndexComponent.META, indexContext)))
        {
            SegmentMetadata.write(metadataWriter, Collections.singletonList(metadata));
        }

        return numRows;
    }
}
