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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.disk.MemtableTermsIterator;
import org.apache.cassandra.index.sai.disk.PerIndexWriter;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexMeta;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BlockIndexWriter;
import org.apache.cassandra.index.sai.memory.MemtableIndex;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Column index writer that flushes indexed data directly from the corresponding Memtable index, without buffering index
 * data in memory.
 */
public class V2MemtableIndexWriter implements PerIndexWriter
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected final IndexDescriptor indexDescriptor;
    protected final IndexContext indexContext;

    private final MemtableIndex memtable;
    private final RowMapping rowMapping;

    public V2MemtableIndexWriter(MemtableIndex memtable,
                                 IndexDescriptor indexDescriptor,
                                 IndexContext indexContext,
                                 RowMapping rowMapping)
    {
        assert rowMapping != null && rowMapping != RowMapping.DUMMY : "Row mapping must exist during FLUSH.";

        this.memtable = memtable;
        this.rowMapping = rowMapping;
        this.indexContext = indexContext;
        this.indexDescriptor = indexDescriptor;
    }

    @Override
    public void addRow(PrimaryKey key, Row row)
    {
        // Memtable indexes are flushed directly to disk with the aid of a mapping between primary
        // keys and row IDs in the flushing SSTable. This writer, therefore, does nothing in
        // response to the flushing of individual rows.
    }

    @Override
    public void abort(Throwable cause)
    {
        logger.warn(indexContext.logMessage("Aborting index memtable flush for {}..."), indexDescriptor.descriptor, cause);
        indexDescriptor.deleteColumnIndex(indexContext);
    }

    @Override
    public void flush() throws IOException
    {
        long start = System.nanoTime();

        try
        {
            if (!rowMapping.hasRows() || (memtable == null) || memtable.isEmpty())
            {
                logger.debug(indexContext.logMessage("No indexed rows to flush from SSTable {}."), indexDescriptor.descriptor);
                // Write a completion marker even though we haven't written anything to the index
                // so we won't try to build the index again for the SSTable
                indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);
                return;
            }

            final DecoratedKey minKey = rowMapping.minKey.partitionKey();
            final DecoratedKey maxKey = rowMapping.maxKey.partitionKey();

            final Iterator<Pair<ByteComparable, LongArrayList>> iterator = rowMapping.merge(memtable);

            try (MemtableTermsIterator terms = new MemtableTermsIterator(memtable.getMinTerm(), memtable.getMaxTerm(), iterator))
            {
                long cellCount = flush(minKey, maxKey, indexContext.getValidator(), terms, rowMapping.maxSegmentRowId);

                indexDescriptor.createComponentOnDisk(IndexComponent.COLUMN_COMPLETION_MARKER, indexContext);

                indexContext.getIndexMetrics().memtableIndexFlushCount.inc();

                long durationMillis = Math.max(1, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));

                if (logger.isTraceEnabled())
                {
                    logger.trace(indexContext.logMessage("Flushed {} Memtable index cells for {} in {} ms."), cellCount, indexDescriptor.descriptor, durationMillis);
                }

                indexContext.getIndexMetrics().memtableFlushCellsPerSecond.update((long) (cellCount * 1000.0 / durationMillis));
            }
        }
        catch (Throwable t)
        {
            logger.error(indexContext.logMessage("Error while flushing index {}"), t.getMessage(), t);
            indexContext.getIndexMetrics().memtableIndexFlushErrors.inc();

            throw t;
        }
    }

    private long flush(DecoratedKey minKey,
                       DecoratedKey maxKey,
                       AbstractType<?> termComparator,
                       MemtableTermsIterator terms,
                       long maxSegmentRowId) throws IOException
    {
        BlockIndexWriter writer = new BlockIndexWriter(indexDescriptor, indexContext, false);
        writer.addAll(terms);
        BlockIndexMeta meta = writer.finish();

        // TODO: could do meta serialize in BlockIndexWriter
        V2IndexOnDiskMetadata.serializer.serialize(meta, indexDescriptor, indexContext);

        // If no rows were written we need to delete any created column index components
        // so that the index is correctly identified as being empty (only having a completion marker)
        if (meta.numRows == 0)
        {
            indexDescriptor.deleteColumnIndex(indexContext);
            return 0;
        }

        return meta.numRows;
    }
}
