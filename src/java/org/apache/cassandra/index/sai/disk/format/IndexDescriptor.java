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

package org.apache.cassandra.index.sai.disk.format;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.disk.ColumnIndexWriter;
import org.apache.cassandra.index.sai.disk.PerIndexFiles;
import org.apache.cassandra.index.sai.disk.PerSSTableComponentsWriter;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.memory.RowMapping;
import org.apache.cassandra.index.sai.utils.IndexFileUtils;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;

public class IndexDescriptor
{
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    public static final String SAI_DESCRIPTOR = "SAI";

    private static final String SEPARATOR = "-";
    private static final String TMP_EXTENSION = ".tmp";

    public final Version version;
    public final Descriptor descriptor;
    public final Set<IndexComponent> perSSTableComponents = Sets.newHashSet();
    public final Map<String, Set<IndexComponent>> perIndexComponents = Maps.newHashMap();
    public final Map<IndexComponent, File> onDiskPerSSTableFileMap = Maps.newHashMap();
    public final Map<Pair<IndexComponent, String>, File> onDiskPerIndexFileMap = Maps.newHashMap();
    public final Map<Pair<IndexComponent, String>, File> onDiskTemporaryFileMap = Maps.newHashMap();

    private IndexDescriptor(Version version, Descriptor descriptor)
    {
        this.version = version;
        this.descriptor = descriptor;
    }

    public static IndexDescriptor create(Descriptor descriptor)
    {
        Preconditions.checkArgument(descriptor != null, "Descriptor can't be null");

        for (Version version : Version.ALL_VERSIONS)
        {
            IndexDescriptor indexDescriptor = new IndexDescriptor(version, descriptor);

            if (indexDescriptor.fileFor(IndexComponent.GROUP_COMPLETION_MARKER).exists())
            {
                indexDescriptor.registerSSTable();
                return indexDescriptor;
            }
        }
        return new IndexDescriptor(Version.LATEST, descriptor);
    }

    public IndexDescriptor registerSSTable()
    {
        version.onDiskFormat()
               .perSSTableComponents()
               .stream()
               .filter(c -> !perSSTableComponents.contains(c) && fileFor(c).exists())
               .forEach(perSSTableComponents::add);
        return this;
    }

    public IndexDescriptor registerIndex(IndexContext indexContext)
    {
        Set<IndexComponent> indexComponents = perIndexComponents.computeIfAbsent(indexContext.getIndexName(), k -> Sets.newHashSet());
        version.onDiskFormat()
               .perIndexComponents(indexContext)
               .stream()
               .filter(c -> !indexComponents.contains(c) && fileFor(c, indexContext.getIndexName()).exists())
               .forEach(indexComponents::add);
        return this;
    }

    public boolean hasComponent(IndexComponent indexComponent)
    {
        return perSSTableComponents.contains(indexComponent);
    }

    public boolean hasComponent(IndexComponent indexComponent, String index)
    {
        return perIndexComponents.containsKey(index) && perIndexComponents.get(index).contains(indexComponent);
    }

    public int numberOfComponents(String indexName)
    {
        return perIndexComponents.containsKey(indexName) ? perIndexComponents.get(indexName).size() : 0;
    }

    public File tmpFileFor(IndexComponent component, String index)
    {
        return onDiskTemporaryFileMap.computeIfAbsent(Pair.create(component, index), c -> new File(tmpFilenameFor(component, index)));
    }

    public File fileFor(IndexComponent component)
    {
        return onDiskPerSSTableFileMap.computeIfAbsent(component, c -> new File(filenameFor(c, null)));
    }

    public File fileFor(IndexComponent component, String index)
    {
        return onDiskPerIndexFileMap.computeIfAbsent(Pair.create(component, index), p -> new File(filenameFor(component, index)));
    }

    private String tmpFilenameFor(IndexComponent component, String index)
    {
        return filenameFor(component, index) + TMP_EXTENSION;
    }

    private String filenameFor(IndexComponent component, String index)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(descriptor.baseFilename()).append(SEPARATOR).append(version.onDiskFormat().componentName(component, index));
        return stringBuilder.toString();
    }

    public Set<Component> getSSTableComponents()
    {
        return perSSTableComponents.stream().map(c -> new Component(Component.Type.CUSTOM, version.onDiskFormat().componentName(c, null))).collect(Collectors.toSet());
    }

    public Set<Component> getSSTableComponents(String index)
    {
        return perIndexComponents.containsKey(index) ? perIndexComponents.get(index)
                                                                         .stream()
                                                                         .map(c -> new Component(Component.Type.CUSTOM, version.onDiskFormat().componentName(c, index)))
                                                                         .collect(Collectors.toSet())
                                                     : Collections.emptySet();
    }

    public SSTableContext newSSTableContext(SSTableReader sstable)
    {
        return version.onDiskFormat().newSSTableContext(sstable, this);
    }

    public PerSSTableComponentsWriter newPerSSTableComponentsWriter(boolean perColumnOnly,
                                                                    CompressionParams compressionParams) throws IOException
    {
        return version.onDiskFormat().createPerSSTableComponentsWriter(perColumnOnly, this, compressionParams);
    }

    public ColumnIndexWriter newIndexWriter(StorageAttachedIndex index,
                                            LifecycleNewTracker tracker,
                                            RowMapping rowMapping,
                                            CompressionParams compressionParams)
    {
        return version.onDiskFormat().newIndexWriter(index, this, tracker, rowMapping, compressionParams);
    }

    public boolean isGroupIndexComplete()
    {
        return version.onDiskFormat().isGroupIndexComplete(this);
    }

    public boolean isColumnIndexComplete(IndexContext indexContext)
    {
        return version.onDiskFormat().isColumnIndexComplete(this, indexContext);
    }

    public boolean isColumnIndexEmpty(IndexContext indexContext)
    {
        return isColumnIndexComplete(indexContext) && numberOfComponents(indexContext.getIndexName()) == 1;
    }

    public PerIndexFiles perIndexFiles(IndexContext indexContext, boolean temporary)
    {
        return version.onDiskFormat().perIndexFiles(this, indexContext, temporary);
    }

    public long sizeOfPerColumnComponents(String index)
    {
        if (perIndexComponents.containsKey(index))
            return perIndexComponents.get(index)
                                     .stream()
                                     .map(c -> Pair.create(c, index))
                                     .map(onDiskPerIndexFileMap::get)
                                     .filter(java.util.Objects::nonNull)
                                     .filter(File::exists)
                                     .mapToLong(File::length)
                                     .sum();
        return 0;
    }

    public void validatePerIndexComponents(IndexContext indexContext) throws IOException
    {
        logger.info("validatePerColumnComponents called for " + indexContext.getIndexName());
        registerIndex(indexContext);
        if (perIndexComponents.containsKey(indexContext.getIndexName()))
            for (IndexComponent indexComponent : perIndexComponents.get(indexContext.getIndexName()))
                version.onDiskFormat().validatePerIndexComponent(this, indexComponent, indexContext, false);
    }

    public boolean validatePerIndexComponentsChecksum(IndexContext indexContext)
    {
        registerIndex(indexContext);
        if (perIndexComponents.containsKey(indexContext.getIndexName()))
            for (IndexComponent indexComponent : perIndexComponents.get(indexContext.getIndexName()))
            {
                try
                {
                    version.onDiskFormat().validatePerIndexComponent(this, indexComponent, indexContext, true);
                }
                catch (Throwable e)
                {
                    return false;
                }
            }
        return true;
    }

    public void validatePerSSTableComponents() throws IOException
    {
        registerSSTable();
        for (IndexComponent indexComponent : perSSTableComponents)
            version.onDiskFormat().validatePerSSTableComponent(this, indexComponent, false);
    }

    public boolean validatePerSSTableComponentsChecksum()
    {
        registerSSTable();
        for (IndexComponent indexComponent : perSSTableComponents)
        {
            try
            {
                version.onDiskFormat().validatePerSSTableComponent(this, indexComponent, true);
            }
            catch (Throwable e)
            {
                return false;
            }
        }
        return true;
    }

    public void deletePerSSTableIndexComponents()
    {
        registerSSTable();
        perSSTableComponents.stream()
                            .map(onDiskPerSSTableFileMap::remove)
                            .filter(java.util.Objects::nonNull)
                            .forEach(this::deleteComponent);
        perSSTableComponents.clear();
    }

    public void deleteColumnIndex(IndexContext indexContext)
    {
        registerIndex(indexContext);
        if (perIndexComponents.containsKey(indexContext.getIndexName()))
            perIndexComponents.remove(indexContext.getIndexName())
                              .stream()
                              .map(c -> Pair.create(c, indexContext.getIndexName()))
                              .map(onDiskPerIndexFileMap::remove)
                              .filter(java.util.Objects::nonNull)
                              .forEach(this::deleteComponent);
    }

    public void deleteTemporaryComponents(IndexContext indexContext)
    {
        version.onDiskFormat()
               .perIndexComponents(indexContext)
               .stream()
               .map(c -> tmpFileFor(c, indexContext.getIndexName()))
               .filter(File::exists)
               .forEach(this::deleteComponent);
    }

    private void deleteComponent(File file)
    {
        logger.debug("Deleting storage attached index component file {}", file);
        try
        {
            IOUtils.deleteFilesIfExist(file.toPath());
        }
        catch (IOException e)
        {
            logger.warn("Unable to delete storage attached index component file {} due to {}.", file, e.getMessage(), e);
        }
    }

    public void createComponentOnDisk(IndexComponent component) throws IOException
    {
        Files.touch(fileFor(component));
        registerComponent(component);
    }

    public void createComponentOnDisk(IndexComponent component, String index) throws IOException
    {
        Files.touch(fileFor(component, index));
        registerComponent(component, index);
    }

    public IndexInput openPerSSTableInput(IndexComponent indexComponent)
    {
        final File file = fileFor(indexComponent);
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Opening blocking index input for file {} ({})"),
                         file,
                         FBUtilities.prettyPrintMemory(file.length()));

        return IndexFileUtils.instance.openBlockingInput(file);
    }

    public IndexInput openPerIndexInput(IndexComponent indexComponent, String index)
    {
        final File file = fileFor(indexComponent, index);
        if (logger.isTraceEnabled())
            logger.trace(logMessage("Opening blocking index input for file {} ({})"),
                         file,
                         FBUtilities.prettyPrintMemory(file.length()));

        return IndexFileUtils.instance.openBlockingInput(file);
    }

    public IndexOutputWriter openPerSSTableOutput(IndexComponent component) throws IOException
    {
        return openPerSSTableOutput(component, false);
    }

    public IndexOutputWriter openPerSSTableOutput(IndexComponent component, boolean append) throws IOException
    {
        final File file = fileFor(component);

        if (logger.isTraceEnabled())
            logger.trace(logMessage("Creating SSTable attached index output for component {} on file {}..."),
                         component,
                         file);

        IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file);

        if (append)
        {
            writer.skipBytes(file.length());
        }

        registerComponent(component);

        return writer;
    }

    public IndexOutputWriter openPerIndexOutput(IndexComponent indexComponent, String index) throws IOException
    {
        return openPerIndexOutput(indexComponent, index, false, false);
    }

    public IndexOutputWriter openPerIndexOutput(IndexComponent component, String index, boolean append, boolean temporary) throws IOException
    {
        final File file = temporary ? tmpFileFor(component, index) : fileFor(component, index);

        if (logger.isTraceEnabled())
            logger.trace(logMessage(index, "Creating {} sstable attached index output for component {} on file {}..."),
                         temporary ? "temporary" : "",
                         component,
                         file);

        IndexOutputWriter writer = IndexFileUtils.instance.openOutput(file);

        if (append)
        {
            writer.skipBytes(file.length());
        }

        if (!temporary)
            registerComponent(component);

        return writer;
    }

    private void registerComponent(IndexComponent indexComponent)
    {
        perSSTableComponents.add(indexComponent);
    }

    private void registerComponent(IndexComponent indexComponent, String index)
    {
        perIndexComponents.computeIfAbsent(index, k -> Sets.newHashSet()).add(indexComponent);
    }

    public FileHandle createPerSSTableFileHandle(IndexComponent indexComponent)
    {
        final File file = fileFor(indexComponent);

        if (logger.isTraceEnabled())
        {
            logger.trace(logMessage("Opening {} file handle for {} ({})"),
                         file, FBUtilities.prettyPrintMemory(file.length()));
        }

        try (final FileHandle.Builder builder = new FileHandle.Builder(file.getAbsolutePath()).mmapped(true))
        {
            return builder.complete();
        }
    }

    public FileHandle createPerIndexFileHandle(IndexComponent indexComponent, String index)
    {
        return createPerIndexFileHandle(indexComponent, index, false);
    }

        public FileHandle createPerIndexFileHandle(IndexComponent indexComponent, String index, boolean temporary)
    {
        final File file = temporary ? tmpFileFor(indexComponent, index) : fileFor(indexComponent, index);

        if (logger.isTraceEnabled())
        {
            logger.trace(logMessage(index, "Opening {} file handle for {} ({})"),
                         temporary ? "temporary" : "", file, FBUtilities.prettyPrintMemory(file.length()));
        }

        try (final FileHandle.Builder builder = new FileHandle.Builder(file.getAbsolutePath()).mmapped(true))
        {
            return builder.complete();
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(descriptor, version);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDescriptor other = (IndexDescriptor)o;
        return Objects.equal(descriptor, other.descriptor) &&
               Objects.equal(version, other.version);
    }

    @Override
    public String toString()
    {
        if (version.onOrAfter(Version.BA))
            return descriptor.toString() + "-SAI+" + version;
        else
            return descriptor.toString() + "-SAI";
    }

    public String logMessage(String index, String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.%s] %s",
                             descriptor.ksname,
                             descriptor.cfname,
                             Strings.isNullOrEmpty(index) ? "*" : index,
                             message);
    }

    public String logMessage(String message)
    {
        // Index names are unique only within a keyspace.
        return String.format("[%s.%s.*] %s",
                             descriptor.ksname,
                             descriptor.cfname,
                             message);
    }
}
