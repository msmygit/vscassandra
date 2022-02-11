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

import javax.annotation.concurrent.NotThreadSafe;

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.IntLongMap;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;

/**
 * Mapping between node ID and an offset to its auxiliary posting list (containing every row id from all leaves
 * reachable from that node. See {@link BinaryTreePostingsWriter}).
 */
@NotThreadSafe
public class BinaryTreePostingsReader
{
    final IntLongMap index = new IntLongHashMap();

    public BinaryTreePostingsReader(long fp, IndexInput upperPostingsInput) throws IOException
    {
        upperPostingsInput.seek(fp);

        int size = upperPostingsInput.readVInt();

        for (int x = 0; x < size; x++)
        {
            final int node = upperPostingsInput.readVInt();
            final long filePointer = upperPostingsInput.readVLong();

            index.put(node, filePointer);
        }
    }
    
    public int size()
    {
        return index.size();
    }

    public long getPostingsFilePointer(int nodeID)
    {
        return index.get(nodeID);
    }

    private static final BinaryTree.IntersectVisitor MATCH_ALL = (minPackedValue, maxPackedValue) -> CELL_INSIDE_QUERY;

    public static BinaryTree.IntersectVisitor bkdQueryFrom(BytesRef lowerBytes, BytesRef upperBytes)
    {
        if (lowerBytes == null && upperBytes == null)
            return MATCH_ALL;

        Bound lower = lowerBytes != null ? new Bound(lowerBytes) : null;
        Bound upper = upperBytes != null ? new Bound(upperBytes) : null;

        return new RangeQueryVisitor(lower, upper);
    }

    public static class Bound
    {
        private final BytesRef bound;

        Bound(BytesRef bound)
        {
            this.bound = bound;
        }

        boolean smallerThan(BytesRef packedValue, Bound bound)
        {
            return packedValue.compareTo(bound.bound) > 0;
        }

        boolean greaterThan(BytesRef packedValue, Bound bound)
        {
            return packedValue.compareTo(bound.bound) < 0;
        }
    }

    /**
     * Returns <tt>true</tt> if given node ID has an auxiliary posting list.
     */
    boolean exists(int nodeID)
    {
        checkArgument(nodeID > 0);
        return index.containsKey(nodeID);
    }

    public static class RangeQueryVisitor implements BinaryTree.IntersectVisitor
    {
        private final Bound lower;
        private final Bound upper;

        public RangeQueryVisitor(Bound lower, Bound upper)
        {
            this.lower = lower;
            this.upper = upper;
        }

        public BytesRef getLowerBytes()
        {
            return lower != null ? lower.bound: null;
        }

        public BytesRef getUpperBytes()
        {
            return upper != null ? upper.bound: null;
        }

        @Override
        public PointValues.Relation compare(BytesRef minPackedValue, BytesRef maxPackedValue)
        {
            boolean crosses = false;

            if (lower != null)
            {
                if (lower.greaterThan(maxPackedValue, lower))
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;

                crosses |= lower.greaterThan(minPackedValue, lower);
            }

            if (upper != null)
            {
                if (upper.smallerThan(minPackedValue, upper))
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;

                crosses |= upper.smallerThan(maxPackedValue, upper);
            }

            return crosses ? PointValues.Relation.CELL_CROSSES_QUERY : PointValues.Relation.CELL_INSIDE_QUERY;
        }
    }
}
