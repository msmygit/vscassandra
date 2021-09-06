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

package org.apache.cassandra.index.sai.disk.format;

/**
 * The {@code IndexFeatureSet} represents the set of features available is SAI that are not
 * available in the {@code V1OnDiskFormat}. Version 1 features should not be added to here
 * unless the feature is removed from a future version and Version 1 is still supported.
 */
public interface IndexFeatureSet
{
    /**
     * Returns whether the index supports row-awareness. Row-awareness means that the per-sstable
     * index supports mapping rowID -> {@code PrimaryKey} where the {@code PrimaryKey} contains both
     * partition key and clustering information.
     *
     * @return true if the index supports row-awareness
     */
    boolean isRowAware();

    /**
     * The {@code Accumulator} is used to accumulate the {@code IndexFeatureSet} responses from
     * multiple sources. This will include all the SSTables included in a query and all the indexes
     * attached to those SSTables.
     *
     * The {@code Accumulator} creates an {@code IndexFeatureSet} this contains the features supported
     * by the lowest version index involved in a query.
     */
    public static class Accumulator
    {
        boolean isRowAware = true;
        boolean complete = false;

        /**
         * Add another {@code IndexFeatureSet} to the accumulation
         *
         * @param indexFeatureSet the feature set to accumulate
         */
        public void accumulate(IndexFeatureSet indexFeatureSet)
        {
            assert !complete : "Cannot accumulate after complete has been called";
            if (!indexFeatureSet.isRowAware())
                isRowAware = false;
        }

        /**
         * Complete the accumulation of feature sets and return the
         * result of the accumulation.
         *
         * @return an {@IndexFeatureSet} containing the accumulated feature set
         */
        public IndexFeatureSet complete()
        {
            complete = true;
            return new IndexFeatureSet()
            {
                @Override
                public boolean isRowAware()
                {
                    return isRowAware;
                }
            };
        }
    }
}
