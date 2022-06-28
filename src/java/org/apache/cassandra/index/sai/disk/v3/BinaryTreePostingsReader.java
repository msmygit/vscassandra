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

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.IntLongMap;
import org.apache.lucene.store.IndexInput;

public class BinaryTreePostingsReader
{
    private final int size;
    public final IntLongMap index = new IntLongHashMap();

    public BinaryTreePostingsReader(long fp, IndexInput upperPostingsInput) throws IOException
    {
        upperPostingsInput.seek(fp);

        size = upperPostingsInput.readVInt();

        for (int x = 0; x < size; x++)
        {
            final int node = upperPostingsInput.readVInt();
            final long filePointer = upperPostingsInput.readVLong();

            index.put(node, filePointer);
        }
    }
}
