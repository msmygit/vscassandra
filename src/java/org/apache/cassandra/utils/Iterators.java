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

package org.apache.cassandra.utils;

import java.util.Iterator;

/**
 * General utility methods related to iterators.
 */
public abstract class Iterators
{
    private Iterators() {}

    /**
     * Creates an {@link Iterator} over the elements of the provided array.
     * <p>
     * This is a shortcut for {@code forArray(array, 0, array.length, false)}.
     * <p>
     * Note that this is the same as the similarly named Guava method, but is provided for consistency with the other
     * related methods of this class.
     *
     * @see #forArray(Object[], int, int, boolean)
     */
    public static <T> Iterator<T> forArray(T[] array)
    {
        return forArray(array, 0, array.length, false);
    }

    /**
     * Creates an {@link Iterator} over the provided range of the provided array.
     * <p>
     * This is a shortcut for {@code forArray(array, fromIndex, toIndex, false)}.
     *
     * @see #forArray(Object[], int, int, boolean)
     */
    public static <T> Iterator<T> forArray(T[] array, int fromIndex, int toIndex)
    {
        return forArray(array, fromIndex, toIndex, false);
    }

    /**
     * Creates an {@link Iterator} over the provided range of the provided array.
     * <p>
     * There returned iterator is a view of the array; subsequent changes to the array will be reflected in the
     * iterator.
     * <p>
     * This is similar to Guava's {@code Iterators.forArray} method, but allows to restrict the range of elements
     * returned and allows reversed iteration.
     *
     * @param array the array to create an iterator over.
     * @param fromIndex the index in {@code array} of the first (last if {@code reversed}) element  returned by the
     *                  created iterator (inclusive).
     * @param toIndex the index in {@code array} just after the last (first if {@code reversed}) element returned by the
     *                created iterator (exclusive).
     * @param reversed if {@code true}, the returned elements are still those between {@code fromIndex} inclusive and
     *                 {@code toIndex} exclusive, but they are returned in reverse order ({@code array[toIndex - 1]},
     *                 {@code array[toIndex - 2]}, ...).
     * @return an iterator returning elements of {@code array} between {@code fromIndex} (inclusive) and {@code toIndex}
     * (exclusive), either in forward or reverse order (depending on {@code reversed}).
     */
    public static <T> Iterator<T> forArray(T[] array, int fromIndex, int toIndex, boolean reversed)
    {
        if (reversed)
        {
            return new AbstractArrayIterator<T>(array, toIndex, fromIndex)
            {
                @Override
                public boolean hasNext()
                {
                    return index > end;
                }

                @Override
                public T next()
                {
                    return array[--index];
                }
            };
        }
        else
        {
            return new AbstractArrayIterator<T>(array, fromIndex, toIndex)
            {
                @Override
                public boolean hasNext()
                {
                    return index < end;
                }

                @Override
                public T next()
                {
                    return array[index++];
                }
            };
        }
    }

    private static abstract class AbstractArrayIterator<T> implements Iterator<T>
    {
        protected final T[] array;
        protected final int end;
        protected int index;

        AbstractArrayIterator(T[] array, int startIndex, int endIndex)
        {
            this.array = array;
            this.end = endIndex;
            this.index = startIndex;
        }
    }
}
