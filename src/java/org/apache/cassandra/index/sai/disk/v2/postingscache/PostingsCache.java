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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.cassandra.index.sai.disk.PostingList;

/**
 * Global size limited postings cache.
 */
public class PostingsCache
{
    private final Cache<PostingsKey, PostingsFactory> cache;

    // 100 megabytes max
    public static final PostingsCache INSTANCE = new PostingsCache(100 * 1024 * 1024);

    public interface PostingsFactory
    {
        public int sizeInBytes();

        public PostingList postings();
    }

    public PostingsCache(int maxBytes)
    {
        cache = Caffeine.newBuilder()
                        .maximumWeight(maxBytes)
                        .weigher((PostingsKey key, PostingsFactory factory) -> factory.sizeInBytes())
                        .build();
    }

    public PostingsFactory get(PostingsKey key)
    {
        return cache.getIfPresent(key);
    }

    public void put(PostingsKey key, PostingsFactory factory)
    {
        cache.put(key, factory);
    }
}
