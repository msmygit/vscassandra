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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.IndexDescriptor;
import org.apache.cassandra.index.sai.disk.v1.readers.BlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.readers.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.utils.LongArray;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Ref;
import org.apache.cassandra.utils.concurrent.RefCounted;

import static org.apache.cassandra.index.sai.disk.OnDiskKeyProducer.NO_OFFSET;

public class V1SSTableContext extends SSTableContext
{
    // mapping from sstable row id to token or offset
    public final LongArray.Factory tokenReaderFactory, offsetReaderFactory;
    public final KeyFetcher keyFetcher;

    private V1SSTableContext(SSTableReader sstable,
                           LongArray.Factory tokenReaderFactory,
                           LongArray.Factory offsetReaderFactory,
                           KeyFetcher keyFetcher,
                           Cleanup cleanup,
                           IndexDescriptor indexDescriptor)
    {
        super(sstable, indexDescriptor, cleanup);
        this.tokenReaderFactory = tokenReaderFactory;
        this.offsetReaderFactory = offsetReaderFactory;
        this.keyFetcher = keyFetcher;
    }

    private V1SSTableContext(SSTableReader sstable, V1SSTableContext copy)
    {
        super(sstable, copy.indexDescriptor, copy);
        this.tokenReaderFactory = copy.tokenReaderFactory;
        this.offsetReaderFactory = copy.offsetReaderFactory;
        this.keyFetcher = copy.keyFetcher;
    }

    public static SSTableContext create(SSTableReader sstable, IndexDescriptor indexDescriptor)
    {
        Ref<? extends SSTableReader> sstableRef = null;
        FileHandle token = null, offset = null;
        LongArray.Factory tokenReaderFactory, offsetReaderFactory;
        KeyFetcher keyFetcher;
        try
        {
            MetadataSource source = MetadataSource.load(indexDescriptor.openPerSSTableInput(IndexComponent.GROUP_META));

            sstableRef = sstable.tryRef();

            if (sstableRef == null)
            {
                throw new IllegalStateException("Couldn't acquire reference to the sstable: " + sstable);
            }

            token = indexDescriptor.createPerSSTableFileHandle(IndexComponent.TOKEN_VALUES);
            offset  = indexDescriptor.createPerSSTableFileHandle(IndexComponent.OFFSETS_VALUES);

            tokenReaderFactory = new BlockPackedReader(token, IndexComponent.TOKEN_VALUES, source);
            offsetReaderFactory = new MonotonicBlockPackedReader(offset, IndexComponent.OFFSETS_VALUES, source);
            keyFetcher = new DecoratedKeyFetcher(sstable);

            Cleanup cleanup = new Cleanup(token, offset, sstableRef);

            return new V1SSTableContext(sstable, tokenReaderFactory, offsetReaderFactory, keyFetcher, cleanup, indexDescriptor);
        }
        catch (Throwable t)
        {
            if (sstableRef != null)
            {
                sstableRef.release();
            }

            throw Throwables.unchecked(Throwables.close(t, token, offset));
        }
    }

    @Override
    public SSTableContext sharedCopy()
    {
        return new V1SSTableContext(sstable, this);
    }

    @Override
    public int openFilesPerSSTable()
    {
        return 2;
    }

    private static class Cleanup implements RefCounted.Tidy
    {
        private final FileHandle token, offset;
        private final Ref<? extends SSTableReader> sstableRef;

        private Cleanup(FileHandle token, FileHandle offset, Ref<? extends SSTableReader> sstableRef)
        {
            this.token = token;
            this.offset = offset;
            this.sstableRef = sstableRef;
        }

        @Override
        public void tidy()
        {
            Throwable t = sstableRef.ensureReleased(null);
            t = Throwables.close(t, token, offset);

            Throwables.maybeFail(t);
        }

        @Override
        public String name()
        {
            return null;
        }
    }

    public interface KeyFetcher
    {
        DecoratedKey apply(RandomAccessReader reader, long keyOffset);

        /**
         * Create a shared RAR for all tokens in the same segment.
         */
        RandomAccessReader createReader();
    }

    @VisibleForTesting
    public static class DecoratedKeyFetcher implements KeyFetcher
    {
        private final SSTableReader sstable;

        DecoratedKeyFetcher(SSTableReader sstable)
        {
            this.sstable = sstable;
        }

        @Override
        public RandomAccessReader createReader()
        {
            return sstable.openKeyComponentReader();
        }

        @Override
        public DecoratedKey apply(RandomAccessReader reader, long keyOffset)
        {
            assert reader != null : "RandomAccessReader null";

            // If the returned offset is the sentinel value, we've seen this offset
            // before or we've run out of valid keys due to ZCS:
            if (keyOffset == NO_OFFSET)
                return null;

            try
            {
                // can return null
                return sstable.keyAt(reader, keyOffset);
            }
            catch (IOException e)
            {
                throw Throwables.cleaned(e);
            }
        }

        @Override
        public String toString()
        {
            return MoreObjects.toStringHelper(this).add("sstable", sstable).toString();
        }

        @Override
        public int hashCode()
        {
            return sstable.descriptor.hashCode();
        }

        @Override
        public boolean equals(Object other)
        {
            if (other == null)
            {
                return false;
            }
            if (other == this)
            {
                return true;
            }
            if (other.getClass() != getClass())
            {
                return false;
            }
            DecoratedKeyFetcher rhs = (DecoratedKeyFetcher) other;
            return sstable.descriptor.equals(rhs.sstable.descriptor);
        }
    }
}
