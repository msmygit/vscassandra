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
package org.apache.cassandra.index.sai.disk.v2.fastfilter;

/**
 * An approximate membership filter.
 */
public interface Filter {

    /**
     * Whether the set may contain the key.
     *
     * @param key the key
     * @return true if the set might contain the key, and false if not
     */
    boolean mayContain(long key);

    /**
     * Get the number of bits in thhe filter.
     *
     * @return the number of bits
     */
    long getBitCount();

    /**
     * Whether the add operation (after construction) is supported.
     *
     * @return true if yes
     */
    default boolean supportsAdd() {
        return false;
    }

    /**
     * Add a key.
     *
     * @param key the key
     */
    default void add(long key) {
        throw new UnsupportedOperationException();
    }

    /**
     * Whether the remove operation is supported.
     *
     * @return true if yes
     */
    default boolean supportsRemove() {
        return false;
    }

    /**
     * Remove a key.
     *
     * @param key the key
     */
    default void remove(long key) {
        throw new UnsupportedOperationException();
    }

    /**
     * Get the number of set bits. This should be 0 for an empty filter.
     *
     * @return the number of bits
     */
    default long cardinality() {
        return -1;
    }

}
