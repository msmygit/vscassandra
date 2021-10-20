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
package org.apache.cassandra.index.sai.disk;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.StorageAttachedIndexGroup;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v2.RowLoader;
import org.apache.cassandra.index.sai.disk.v2.blockindex.BytesUtil;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Writes all on-disk index structures attached to a given SSTable.
 */
public class StorageAttachedIndexWriter implements SSTableFlushObserver
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final IndexDescriptor indexDescriptor;
    private final PrimaryKey.PrimaryKeyFactory primaryKeyFactory;
    private final Collection<StorageAttachedIndex> indices;
    private final Collection<PerIndexWriter> columnIndexWriters;
    private final PerSSTableWriter sstableComponentsWriter;
    private final Stopwatch stopwatch = Stopwatch.createUnstarted();
    private final RowMapping rowMapping;
    private final List<ColumnMetadata> columns = new ArrayList();
    private final ColumnFilter columnFilter;

    private DecoratedKey currentKey;
    private boolean tokenOffsetWriterCompleted = false;
    private boolean aborted = false;

    final StorageAttachedIndexGroup group;

    private ColumnFamilyStore cfs;

    private long sstableRowId = 0;

    public StorageAttachedIndexWriter(IndexDescriptor indexDescriptor,
                                      Collection<StorageAttachedIndex> indices,
                                      LifecycleNewTracker lifecycleNewTracker) throws IOException
    {
        this(indexDescriptor, indices, lifecycleNewTracker, false);
    }

    public StorageAttachedIndexWriter(IndexDescriptor indexDescriptor,
                                      Collection<StorageAttachedIndex> indices,
                                      LifecycleNewTracker lifecycleNewTracker,
                                      boolean perColumnOnly) throws IOException
    {
        this.indexDescriptor = indexDescriptor;
        this.primaryKeyFactory = indexDescriptor.primaryKeyFactory;
        this.indices = indices;
        this.rowMapping = RowMapping.create(lifecycleNewTracker.opType());
        this.columnIndexWriters = indices.stream().map(i -> indexDescriptor.newPerIndexWriter(i,
                                                                                              lifecycleNewTracker,
                                                                                              rowMapping))
                                         .filter(Objects::nonNull) // a null here means the column had no data to flush
                                         .collect(Collectors.toList());

        this.sstableComponentsWriter = perColumnOnly ? PerSSTableWriter.NONE : indexDescriptor.newPerSSTableWriter();
        ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
        for (StorageAttachedIndex index : indices)
        {
            cfs = index.baseCfs();
            columns.add(index.getIndexContext().getDefinition());
            builder.add(index.getIndexContext().getDefinition());
        }

        group = StorageAttachedIndexGroup.getIndexGroup(cfs);

        columnFilter = builder.build();
    }

    @Override
    public void begin()
    {
        logger.debug(indexDescriptor.logMessage("Starting partition iteration for storage attached index flush for SSTable {}..."), indexDescriptor.descriptor);
        stopwatch.start();
    }

    @Override
    public void startPartition(DecoratedKey key, long position)
    {
        if (aborted) return;
        
        currentKey = key;

        PrimaryKey primaryKey = primaryKeyFactory.createKey(currentKey.getToken(), sstableRowId);

        try
        {
            sstableComponentsWriter.startPartition(primaryKey, position);
        }
        catch (Throwable t)
        {
            logger.error(indexDescriptor.logMessage("Failed to record a partition start during an index build"), t);
            abort(t, true);
        }
    }

    @Override
    public void nextUnfilteredCluster(Unfiltered unfiltered, long position)
    {
        if (aborted) return;
        
        try
        {
            // Ignore range tombstones...
            if (unfiltered.isRow())
            {
                final PrimaryKey primaryKey = primaryKeyFactory.createKey(currentKey, ((Row) unfiltered).clustering(), sstableRowId++);

                sstableComponentsWriter.nextRow(primaryKey);
                rowMapping.add(primaryKey);

                for (PerIndexWriter w : columnIndexWriters)
                {
                    w.addRow(primaryKey, (Row) unfiltered);
                }
            }
        }
        catch (Throwable t)
        {
            logger.error(indexDescriptor.logMessage("Failed to record a row during an index build"), t);
            abort(t, true);
        }
    }

    @Override
    public void partitionLevelDeletion(DeletionTime deletionTime, long position)
    {
        // Deletions (including partition deletions) are accounted for during reads.
    }

    @Override
    public void staticRow(Row staticRow, long position)
    {
        if (aborted) return;
        
        if (staticRow.isEmpty())
            return;

        try
        {
            PrimaryKey primaryKey = primaryKeyFactory.createKey(currentKey, staticRow.clustering(), sstableRowId++);
            sstableComponentsWriter.nextRow(primaryKey);
            rowMapping.add(primaryKey);

            for (PerIndexWriter w : columnIndexWriters)
            {
                w.addRow(primaryKey, staticRow);
            }
        }
        catch (Throwable t)
        {
            logger.error(indexDescriptor.logMessage("Failed to record a static row during an index build"), t);
            abort(t, true);
        }
    }

    @Override
    public void complete()
    {
        if (aborted) return;
        
        logger.debug(indexDescriptor.logMessage("Completed partition iteration for index flush for SSTable {}. Elapsed time: {} ms"),
                     indexDescriptor.descriptor, stopwatch.elapsed(TimeUnit.MILLISECONDS));

        Map<ColumnMetadata,StorageAttachedIndex> indexedColumns = new HashMap();
        for (StorageAttachedIndex index : indices)
        {
            indexedColumns.put(index.getIndexContext().getDefinition(), index);
        }

        try
        {
            for (Map.Entry<PrimaryKey,Row> entry : group.nonUniqueKeys.entrySet())
            {
                List<ColumnMetadata> missingColumns = null;
                for (ColumnMetadata indexedColumn : indexedColumns.keySet())
                {
                    Cell memCell = entry.getValue().getCell(indexedColumn);
                    if (memCell == null)
                    {
                        if (missingColumns == null)
                        {
                            missingColumns = new ArrayList<>();
                        }
                        missingColumns.add(indexedColumn);
                        System.out.println("indexedColumn no mem cell="+indexedColumn.name);
                    }
                }

                try (UnfilteredRowIterator iterator = RowLoader.loadRow(cfs,
                                                                        entry.getKey(),
                                                                        columnFilter))
                {
                    // for each memory row, find columns not indexed
                    // if the row does not have some indexed columns
                    // load the row from disk and index the unindexed disk row column cells

                    if (iterator.hasNext())
                    {
                        final Row realRow = (Row)iterator.next();

                        if (missingColumns != null)
                        {
                            for (ColumnMetadata missingColumn : missingColumns)
                            {
                                Cell diskCell = realRow.getCell(missingColumn);
                                System.out.println("diskCell missingColumn="+missingColumn);
                                StorageAttachedIndex index = indexedColumns.get(missingColumn);
                                index.getIndexContext().indexFirstMemtable(entry.getKey().partitionKey(), realRow, true);
                            }
                        }

//                        for (ColumnMetadata col : entry.getValue().columns())
//                        {
//                            Cell diskCell = realRow.getCell(col);
//                            Cell memCell = entry.getValue().getCell(col);
//
//                            boolean same = diskCell.equals(memCell);
//
//                            System.out.println("diskCell col="+col.name+" same="+same);
//                        }

                        System.out.println("unfiltered2=" + realRow.toString(cfs.metadata())+" memrow="+entry.getValue());
                    }
                }
            }

            sstableComponentsWriter.complete();
            tokenOffsetWriterCompleted = true;

            logger.debug(indexDescriptor.logMessage("Flushed tokens and offsets for SSTable {}. Elapsed time: {} ms."),
                         indexDescriptor.descriptor, stopwatch.elapsed(TimeUnit.MILLISECONDS));

            rowMapping.complete();

            logger.debug(indexDescriptor.logMessage("About to flush per-column index writers for {} indexes."), columnIndexWriters.size());
            for (PerIndexWriter columnIndexWriter : columnIndexWriters)
            {
                logger.debug("flushing columnIndexWriter=" + columnIndexWriter.toString());
                columnIndexWriter.flush();
            }
        }
        catch (Throwable t)
        {
            logger.error(indexDescriptor.logMessage("Failed to complete an index build"), t);
            abort(t, true);
        }
    }

    /**
     * Aborts all column index writers and, only if they have not yet completed, SSTable-level component writers.
     * 
     * @param accumulator the initial exception thrown from the failed writer
     */
    @Override
    public void abort(Throwable accumulator)
    {
        abort(accumulator, false);
    }

    /**
     *
     * @param accumulator original cause of the abort
     * @param fromIndex true if the cause of the abort was the index itself, false otherwise
     */
    public void abort(Throwable accumulator, boolean fromIndex)
    {
        // Mark the write aborted, so we can short-circuit any further operations on the component writers.
        aborted = true;
        
        // Make any indexes involved in this transaction non-queryable, as they will likely not match the backing table.
        if (fromIndex)
            indices.forEach(StorageAttachedIndex::makeIndexNonQueryable);
        
        for (PerIndexWriter writer : columnIndexWriters)
        {
            try
            {
                writer.abort(accumulator);
            }
            catch (Throwable t)
            {
                if (accumulator != null)
                {
                    accumulator.addSuppressed(t);
                }
            }
        }
        
        if (!tokenOffsetWriterCompleted)
        {
            // If the token/offset files have already been written successfully, they can be reused later. 
            sstableComponentsWriter.abort(accumulator);
        }
    }
}
