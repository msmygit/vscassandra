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
package org.apache.cassandra.io.util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.utils.ChecksumType;
import org.apache.cassandra.utils.Throwables;

public class DataIntegrityMetadata
{
    public static ChecksumValidator checksumValidator(Descriptor desc) throws IOException
    {
        return new ChecksumValidator(desc);
    }

    public static class ChecksumValidator implements Closeable
    {
        private final ChecksumType checksumType;
        private final RandomAccessReader reader;
        public final int chunkSize;
        private final File dataFile;

        public ChecksumValidator(Descriptor descriptor) throws IOException
        {
            this(ChecksumType.CRC32,
                 RandomAccessReader.open(descriptor.fileFor(Component.CRC)),
                 descriptor.fileFor(Component.DATA));
        }

        public ChecksumValidator(ChecksumType checksumType, RandomAccessReader reader, File dataFile) throws IOException
        {
            this.checksumType = checksumType;
            this.reader = reader;
            this.dataFile = dataFile;
            chunkSize = reader.readInt();
        }

        @VisibleForTesting
        protected ChecksumValidator(ChecksumType checksumType, RandomAccessReader reader, int chunkSize)
        {
            this.checksumType = checksumType;
            this.reader = reader;
            this.dataFile = null;
            this.chunkSize = chunkSize;
        }

        public void seek(long offset)
        {
            long start = chunkStart(offset);
            reader.seek(((start / chunkSize) * 4L) + 4); // 8 byte checksum per chunk + 4 byte header/chunkLength
        }

        public long chunkStart(long offset)
        {
            long startChunk = offset / chunkSize;
            return startChunk * chunkSize;
        }

        public void validate(byte[] bytes, int start, int end) throws IOException
        {
            int current = (int) checksumType.of(bytes, start, end);
            int actual = reader.readInt();
            if (current != actual)
                throw new IOException("Corrupted File : " + dataFile);
        }

        /**
         * validates the checksum with the bytes from the specified buffer.
         *
         * Upon return, the buffer's position will
         * be updated to its limit; its limit will not have been changed.
         */
        public void validate(ByteBuffer buffer) throws IOException
        {
            int current = (int) checksumType.of(buffer);
            int actual = reader.readInt();
            if (current != actual)
                throw new IOException("Corrupted File : " + dataFile);
        }

        public void close()
        {
            reader.close();
        }
    }

    public static FileDigestValidator fileDigestValidator(Descriptor desc) throws IOException
    {
        return new FileDigestValidator(desc);
    }

    public static class FileDigestValidator implements Closeable
    {
        private final Checksum checksum;
        private final RandomAccessReader digestReader;
        private final RandomAccessReader dataReader;
        private final Descriptor descriptor;
        private long storedDigestValue;

        public FileDigestValidator(Descriptor descriptor) throws IOException
        {
            this.descriptor = descriptor;
            checksum = ChecksumType.CRC32.newInstance();
            digestReader = RandomAccessReader.open(descriptor.fileFor(Component.DIGEST));
            dataReader = RandomAccessReader.open(descriptor.fileFor(Component.DATA));
            try
            {
                storedDigestValue = Long.parseLong(digestReader.readLine());
            }
            catch (Exception e)
            {
                close();
                // Attempting to create a FileDigestValidator without a DIGEST file will fail
                throw new IOException("Corrupted SSTable : " + descriptor.fileFor(Component.DATA));
            }
        }

        // Validate the entire file
        public void validate() throws IOException
        {
            CheckedInputStream checkedInputStream = new CheckedInputStream(dataReader, checksum);
            byte[] chunk = new byte[64 * 1024];

            while( checkedInputStream.read(chunk) > 0 ) { }
            long calculatedDigestValue = checkedInputStream.getChecksum().getValue();
            if (storedDigestValue != calculatedDigestValue)
            {
                throw new IOException("Corrupted SSTable : " + descriptor.fileFor(Component.DATA));
            }
        }

        public void close()
        {
            Throwables.perform(digestReader::close,
                               dataReader::close);
        }
    }
}
