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

package org.apache.cassandra.index.sai.disk.v2.postingscache;

import com.google.common.base.Objects;

public class PostingsKey
{
    public final Object sstableId;
    public final String indexName;
    public final long sstableIndexId; // file pointer or node id
    public final boolean incongruentTS;

    public PostingsKey(Object sstableId, String indexName, long sstableIndexId, boolean incongruentTS)
    {
        this.sstableId = sstableId;
        this.indexName = indexName;
        this.sstableIndexId = sstableIndexId;
        this.incongruentTS = incongruentTS;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostingsKey that = (PostingsKey) o;
        return sstableIndexId == that.sstableIndexId && incongruentTS == that.incongruentTS && Objects.equal(sstableId, that.sstableId) && Objects.equal(indexName, that.indexName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(sstableId, indexName, sstableIndexId, incongruentTS);
    }
}
