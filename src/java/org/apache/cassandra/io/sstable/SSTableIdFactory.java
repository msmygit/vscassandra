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

package org.apache.cassandra.io.sstable;

import java.nio.ByteBuffer;

public class SSTableIdFactory
{
    public static final SSTableIdFactory instance = new SSTableIdFactory();

    /**
     * Tries to guess the exact identifier type and create an instance of {@link SSTableId} using
     * {@link SSTableId.Builder#fromString(String)} from the guessed builder
     *
     * @throws IllegalArgumentException when the provided string representation is malformed
     */
    public SSTableId fromString(String str) throws IllegalArgumentException
    {
        SSTableId.Builder<?> builder = str.length() == UUIDBasedSSTableId.STRING_LEN
                                                     ? UUIDBasedSSTableId.Builder.instance
                                                     : SequenceBasedSSTableId.Builder.instance;
        return builder.fromString(str);
    }

    /**
     * Tries to guess the exact identifier type and create an instance of {@link SSTableId} using
     * {@link SSTableId.Builder#fromBytes(ByteBuffer)} from the guessed builder
     *
     * @throws IllegalArgumentException whenn the provided binary representation is malformed
     */
    public SSTableId fromBytes(ByteBuffer bytes)
    {
        SSTableId.Builder<?> builder = bytes.remaining() == UUIDBasedSSTableId.BYTES_LEN
                                                     ? UUIDBasedSSTableId.Builder.instance
                                                     : SequenceBasedSSTableId.Builder.instance;
        return builder.fromBytes(bytes);
    }

    /**
     * Returns default identifiers builder.
     */
    @SuppressWarnings("unchecked")
    public SSTableId.Builder<SSTableId> defaultBuilder()
    {
        SSTableId.Builder<? extends SSTableId> builder = UUIDBasedSSTableId.Builder.instance;
        return (SSTableId.Builder<SSTableId>) builder;
    }
}
