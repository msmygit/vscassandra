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

package org.apache.cassandra.index.sai.disk.v2.blockindex;

import java.io.IOException;
import java.io.Serializable;
import java.util.zip.CRC32;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.lucene.store.IndexInput;

public class FileValidator
{
    public static class FileInfo implements Serializable
    {
        public final long headerCRC;
        public final long middleCRC;
        public final long footerCRC;

        public FileInfo(long headerCRC, long middleCRC, long footerCRC)
        {
            this.headerCRC = headerCRC;
            this.middleCRC = middleCRC;
            this.footerCRC = footerCRC;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileInfo fileInfo = (FileInfo) o;
            return headerCRC == fileInfo.headerCRC && middleCRC == fileInfo.middleCRC && footerCRC == fileInfo.footerCRC;
        }

        @Override
        public String toString()
        {
            return "FileInfo{" +
                   "headerCRC=" + headerCRC +
                   ", middleCRC=" + middleCRC +
                   ", footerCRC=" + footerCRC +
                   '}';
        }
    }

    public static long generateCRC(final IndexInput input,
                                   final byte[] buffer,
                                   final int length) throws IOException
    {
        input.readBytes(buffer, 0, length);
        CRC32 crc = new CRC32();
        crc.update(buffer, 0, length);
        return crc.getValue();
    }

    public static FileInfo generate(IndexInput indexInput) throws IOException
    {
        final long fileLength = indexInput.length();

        int bufferLen = fileLength < 1024 ? Math.max(1,(int)fileLength / 3) : 1024;
        byte[] buffer = new byte[bufferLen];

        final long headerCRC = generateCRC(indexInput, buffer, bufferLen);

        indexInput.seek(fileLength / 2);
        int midLength = fileLength - fileLength / 2 < bufferLen ? (int)(fileLength - fileLength / 2) : bufferLen;
        final long middleCRC = generateCRC(indexInput, buffer, midLength);

        indexInput.seek(fileLength - bufferLen);
        final long footerCRC = generateCRC(indexInput, buffer, bufferLen);
        FileUtils.closeQuietly(indexInput);
        return new FileInfo(headerCRC, middleCRC, footerCRC);
    }
}
