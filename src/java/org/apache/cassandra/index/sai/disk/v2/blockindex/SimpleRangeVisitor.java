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

import org.apache.lucene.index.PointValues;

class SimpleRangeVisitor implements BlockIndexReader.SimpleVisitor
{
    public final SimpleBound lower, upper;

    public SimpleRangeVisitor(SimpleBound lower, SimpleBound upper)
    {
        this.lower = lower;
        this.upper = upper;
    }

    @Override
    public PointValues.Relation compare(int minValue, int maxValue)
    {
        boolean crosses = false;

        if (lower != null)
        {
            int maxCmp = Integer.compare(maxValue, lower.bound);
            if (lower.greaterThan(maxCmp))
                return PointValues.Relation.CELL_OUTSIDE_QUERY;

            int minCmp = Integer.compare(minValue, lower.bound);
            crosses |= lower.greaterThan(minCmp);
        }

        if (upper != null)
        {
            int minCmp = Integer.compare(minValue, upper.bound);
            if (upper.smallerThan(minCmp))
                return PointValues.Relation.CELL_OUTSIDE_QUERY;

            int maxCmp = Integer.compare(maxValue, upper.bound);
            crosses |= upper.smallerThan(maxCmp);
        }

        if (crosses)
        {
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }
        else
        {
            return PointValues.Relation.CELL_INSIDE_QUERY;
        }
    }
}
