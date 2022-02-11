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
import java.util.Arrays;
import java.util.Base64;
import java.util.zip.CRC32;

import org.apache.cassandra.index.sai.disk.io.RAMIndexOutput;
import org.apache.lucene.store.ByteArrayIndexInput;
import org.apache.lucene.store.IndexInput;

/**
 * Uses CRC32 to validate the first and last 1k of a given section of a file.
 * If the file is less than 2k then the whole file is validated.
 */
public class FileValidation
{
    public static final int BLOCK_SIZE = 1024;

    /**
     * Meta object representing a validated file section.
     */
    public static class Meta
    {
        public final long fp0;
        public final int len0;
        public final long crc0;
        public final long fp1;
        public final int len1;
        public final long crc1;

        public byte[] toBytes()
        {
            try
            {
                RAMIndexOutput out = new RAMIndexOutput("");
                out.writeVLong(fp0);
                out.writeVLong(len0);
                out.writeVLong(crc0);
                out.writeVLong(fp1);
                out.writeVLong(len1);
                out.writeVLong(crc1);
                return Arrays.copyOf(out.getBytes(), (int) out.getFilePointer());
            }
            catch (IOException ex)
            {
                throw new RuntimeException(ex);
            }
        }

        public String toBase64()
        {
            return Base64.getEncoder().encodeToString(toBytes());
        }

        public Meta(String base64)
        {
            this(Base64.getDecoder().decode(base64));
        }

        public Meta(byte[] bytes)
        {
            ByteArrayIndexInput input = new ByteArrayIndexInput("", bytes);
            fp0 = input.readVLong();
            len0 = (int)input.readVLong();
            crc0 = input.readVLong();
            fp1 = input.readVLong();
            len1 = (int)input.readVLong();
            crc1 = input.readVLong();
        }

        public Meta(long fp0, int len0, long crc0, long fp1, int len1, long crc1)
        {
            this.fp0 = fp0;
            this.len0 = len0;
            this.crc0 = crc0;
            this.fp1 = fp1;
            this.len1 = len1;
            this.crc1 = crc1;
        }
    }

    /**
     * Create a Meta on a given part of a file.
     *
     * The length is determined by the file length minus root file pointer,
     * which works for the segmented index case.
     *
     * @param rootFP File pointer to start
     * @param input Input stream to read
     * @return Meta object defining file parts with CRC32 values
     * @throws IOException
     */
    public static Meta createValidate(long rootFP, final IndexInput input) throws IOException
    {
        final CRC32 crc = new CRC32();
        final int length = (int)(input.length() - rootFP);

        if (length < 2 * BLOCK_SIZE)
        {
            // the total length is less than 2 blocks, load the whole file section and validate
            final byte[] bytes = new byte[length];
            input.seek(rootFP);
            input.readBytes(bytes, 0, length);
            crc.update(bytes);
            final long crcValue = crc.getValue();
            return new Meta(rootFP, bytes.length, crcValue, rootFP, bytes.length, crcValue);
        }
        else
        {
            final long fp0 = rootFP;
            final long fp1 = rootFP + (length - BLOCK_SIZE);

            final long crc0 = loadCRCValue(fp0, BLOCK_SIZE, input, crc);
            final long crc1 = loadCRCValue(fp1, BLOCK_SIZE, input, crc);

            return new Meta(fp0, BLOCK_SIZE, crc0, fp1, BLOCK_SIZE, crc1);
        }
    }

    /**
     * CRC32 validate a part of a file using the Meta
     * @param input Input to use
     * @param meta Meta to use
     * @return True if the file validation check.
     * @throws IOException
     */
    public static boolean validate(final IndexInput input, Meta meta) throws IOException
    {
        final long prevFP = input.getFilePointer();
        try
        {
            final CRC32 crc = new CRC32();

            return loadValidate(meta.fp0, meta.len0, meta.crc0, input, crc)
                   && loadValidate(meta.fp1, meta.len1, meta.crc1, input, crc);
        }
        finally
        {
            input.seek(prevFP);
        }
    }

    private static boolean loadValidate(long fp, int len, long crcValue, IndexInput input, CRC32 crc) throws IOException
    {
        final long diskCRCValue = loadCRCValue(fp, len, input, crc);
        return crcValue == diskCRCValue;
    }

    private static long loadCRCValue(long fp, int len, IndexInput input, CRC32 crc) throws IOException
    {
        crc.reset();
        byte[] bytes = new byte[len];
        input.seek(fp);
        input.readBytes(bytes, 0, bytes.length);
        crc.update(bytes);
        return crc.getValue();
    }
}
