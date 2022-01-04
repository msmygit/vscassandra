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

package org.apache.cassandra.utils.concurrent;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

/**
 * An extension of {@link NonBlockingHashMap} where all values are wrapped by {@link CompletableFuture}.
 * <p>
 * The main purpose of this class is to provide the functionality of concurrent hash map which may perform operations
 * like {@link ConcurrentHashMap#compute(Object, BiFunction)} with synchronization scope reduced to the single key -
 * that is, when dealing with a single key, unlike {@link ConcurrentHashMap} the whole map is not locked for the time
 * the mapping function is running. This may help to avoid the case when loading/unloading a value for a key K1 while
 * loading/unloading a value for a key K2. Such scenario is forbidden in case of {@link ConcurrentHashMap} and leads to
 * a deadlock. On the other hand, {@link NonBlockingHashMap} does not guarantee at-most-once semantics of running the
 * mapping function for a single key.
 * <p>
 * TODO when merging with OSS trunk, this class should be integrated with LoadingMap
 */
public class LoadingMap<K, V>
{
    // The map of futures lets us synchronize on per key basis rather than synchronizing the whole map.
    // It works in the way that when there is an ongoing computation (update) on a key, the other thread
    // trying to access that key recevies an incomplete future and needs to wait until the computation is done.
    // This way we can achieve serial execution for each key while different keys can be processed concurrently.
    // It also ensures exactly-once semantics for the update operation.
    private final ConcurrentMap<K, CompletableFuture<V>> internalMap = new NonBlockingHashMap<>();

    /**
     * Recomputes the given object in the map in a thread-safe way.
     * The remapping function is applied for the entry of the provided key with the following rules:
     * - if entry exists, it is passed to the remapping function
     * - if entry does not exist, null is passed to the remapping function
     * - if the remapping function returns non-null value, the entry is added or replaced
     * - if the remapping function returns null value, the entry is removed
     * <p>
     * The remapping function is guaranteed to be applied exactly once.
     * <p>
     * The method blocks until the update is applied. The method waits for the ongoing updates for the same key, but
     * it does not wait for any updates for other keys.
     */
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        CompletableFuture<V> newEntry = new CompletableFuture<>();
        CompletableFuture<V> previousEntry;
        V previousValue = null;
        do
        {
            previousEntry = internalMap.get(key);

            if (previousEntry == null && internalMap.putIfAbsent(key, newEntry) == null)
                // there were no entry for the provided key, so we put a promise there and break
                break;

            if (previousEntry != null)
            {
                previousValue = previousEntry.join();
                if (previousValue != null && internalMap.replace(key, previousEntry, newEntry))
                    // there was a legitmate entry with a non-null value - we replace it with a promise and break
                    break;

                // otherwise, if previousValue == null, some other thread deleted the entry in the meantime; we need
                // to try again because yet another thread might have attempted to do something for that key
            }
        } while (true);

        try
        {
            // apply the provided remapping function
            V newValue = remappingFunction.apply(key, previousValue);
            if (newValue == null)
            {
                // null result means we should remove the entry
                CompletableFuture<V> removedEntry = internalMap.remove(key);
                newEntry.complete(null);
                assert removedEntry == newEntry;
                return null;
            }
            else
            {
                // non-null result means we should complete the new entry promise with the returned value
                newEntry.complete(newValue);
                return newValue;
            }
        }
        catch (RuntimeException ex)
        {
            // in case of exception (which may happen only in remapping function), we simply revert the change and
            // rethrow the exception
            if (previousEntry != null)
            {
                // if the entry existed before, the new entry promise is simply completed with the old value
                newEntry.complete(previousValue);
            }
            else
            {
                // if the entry did not exist before, the new entry is removed and promise is completed with null, which
                // tells other threads waiting for the promise completion to try again
                CompletableFuture<V> f = internalMap.remove(key);
                assert f == newEntry;
                newEntry.complete(null);
            }

            throw ex;
        }
    }

    @VisibleForTesting
    Future<V> getUnsafe(K key)
    {
        return internalMap.get(key);
    }

    public V get(K key)
    {
        while (true)
        {
            CompletableFuture<V> entry = internalMap.get(key);
            if (entry == null)
                // value not found
                return null;

            V value = entry.join();
            if (value != null)
                return value;

            // we need to retry because info == null means that the entry got removed
            Thread.yield();
        }
    }

    public Stream<V> values()
    {
        return internalMap.keySet().stream().map(this::get).filter(Objects::nonNull);
    }
}
