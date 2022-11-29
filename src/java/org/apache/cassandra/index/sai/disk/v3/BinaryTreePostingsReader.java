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
import java.nio.ByteBuffer;

import com.carrotsearch.hppc.IntLongHashMap;
import com.carrotsearch.hppc.IntLongMap;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.lucene.index.PointValues.Relation.CELL_INSIDE_QUERY;

/**
 * Mapping between node ID and an offset to its auxiliary posting list (containing every row id from all leaves
 * reachable from that node. See {@link BinaryTreePostingsWriter}).
 */
public class BinaryTreePostingsReader
{
    final IntLongMap index = new IntLongHashMap();
    private final int size;

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

    public long getPostingsFilePointer(int nodeID)
    {
        return index.get(nodeID);
    }

    private static final BinaryTree.IntersectVisitor MATCH_ALL = new BinaryTree.IntersectVisitor()
    {
        @Override
        public PointValues.Relation compare(BytesRef minPackedValue, BytesRef maxPackedValue)
        {
            return CELL_INSIDE_QUERY;
        }
    };

    public static BinaryTree.IntersectVisitor bkdQueryFrom(BytesRef lowerBytes, BytesRef upperBytes)
    {
        if (lowerBytes == null && upperBytes == null)
        {
            return MATCH_ALL;
        }

        Bound lower = null;
        if (lowerBytes != null)
        {
            lower = new Bound(lowerBytes, false);
        }

        Bound upper = null;
        if (upperBytes != null)
        {
            upper = new Bound(upperBytes, false);
        }

        return new RangeQueryVisitor(lower, upper);
    }

    public static BinaryTree.IntersectVisitor bkdQueryFrom(Expression expression)
    {
        if (expression.lower == null && expression.upper == null)
        {
            return MATCH_ALL;
        }

        Bound lower = null ;
        if (expression.lower != null)
        {
            final byte[] lowerBound = toComparableBytes(expression.lower.value.encoded, expression.validator);
            lower = new Bound(new BytesRef(lowerBound), !expression.lower.inclusive);
        }

        Bound upper = null;
        if (expression.upper != null)
        {
            final byte[] upperBound = toComparableBytes(expression.upper.value.encoded, expression.validator);
            upper = new Bound(new BytesRef(upperBound), !expression.upper.inclusive);
        }

        return new RangeQueryVisitor(lower, upper);
    }

    public static class Bound
    {
        private final BytesRef bound;
        private final boolean exclusive;

        Bound(BytesRef bound, boolean exclusive)
        {
            this.bound = bound;
            this.exclusive = exclusive;
        }

        boolean smallerThan(int cmp)
        {
            return cmp > 0 || (cmp == 0 && exclusive);
        }

        boolean greaterThan(int cmp)
        {
            return cmp < 0 || (cmp == 0 && exclusive);
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

    private static byte[] toComparableBytes(ByteBuffer value, AbstractType<?> type)
    {
        byte[] buffer = new byte[TypeUtil.fixedSizeOf(type)];
        TypeUtil.toComparableBytes(value, type, buffer);
        return buffer;
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

        int compareUnsigned(BytesRef packedValue, Bound bound)
        {
            return packedValue.compareTo(bound.bound);
        }

        @Override
        public PointValues.Relation compare(BytesRef minPackedValue, BytesRef maxPackedValue)
        {
            boolean crosses = false;

            if (lower != null)
            {
                int maxCmp = compareUnsigned(maxPackedValue, lower);
                if (lower.greaterThan(maxCmp))
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;

                int minCmp = compareUnsigned(minPackedValue, lower);
                crosses |= lower.greaterThan(minCmp);
            }

            if (upper != null)
            {
                int minCmp = compareUnsigned(minPackedValue, upper);
                if (upper.smallerThan(minCmp))
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;

                int maxCmp = compareUnsigned(maxPackedValue, upper);
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
}
