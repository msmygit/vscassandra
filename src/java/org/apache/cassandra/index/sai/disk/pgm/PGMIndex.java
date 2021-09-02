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

package org.apache.cassandra.index.sai.disk.pgm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.lucene.store.IndexOutput;

public class PGMIndex
{
    final List<Segment> segments = new ArrayList();
    final long firstKey;

    static
    {
        System.load("/Users/jasonrutherglen/datastax/STAR-851/pgmindex/libpgmindex.dylib");
    }

    public static void main(String[] args)
    {
        int count = 100_000_000;
        LongArrayList list = new LongArrayList();
        long rowid = 0;
        for (int x = 0; x < count; x++)
        {
            rowid += ThreadLocalRandom.current().nextInt(1, 1000);
            list.add(rowid);
        }

        long[] array = list.toArray();
        PGMIndex index = new PGMIndex(array);

        long target = ThreadLocalRandom.current().nextLong(list.get(0), list.get(list.size() - 1));

        long pos = index.search(target);
        System.out.println("target="+target+" pos="+pos+" posting="+array[(int)pos]);
    }

    public PGMIndex(long[] array)
    {
        firstKey = array[0];

        create(array, segments);

        System.out.println("segments="+segments);
    }

    public void writeSegments(IndexOutput out) throws IOException
    {

    }

    public static class ApproxPos
    {
        public final long pos, lo, hi;

        public ApproxPos(long pos, long lo, long hi)
        {
            this.pos = pos;
            this.lo = lo;
            this.hi = hi;
        }
    }

    public long search(long targetKey)
    {
         long k = Math.max(firstKey, targetKey);
         int r;
         for (r = 0; r < segments.size(); r++)
         {
             if (segments.get(r).key >= k)
             {
                 break;
             }
         }
         r--;
         long origin = segments.get(r).key;

         long pos = Math.min(segments.get(r).operator(origin, k), segments.get(r + 1).intercept);

         //long pos = Math.min(segments.get(r).operator(origin), segments.get(r + 1).intercept);
         return pos;
    }
//
//    ApproxPos search(const K &key) const {
//    auto k = std::max(first_key, key);
//    auto[r, origin] = pred(k - first_key);
//    auto pos = std::min<size_t>(segments[r](origin + first_key, k), segments[r + 1].intercept);
//    auto lo = PGM_SUB_EPS(pos, Epsilon);
//    auto hi = PGM_ADD_EPS(pos, Epsilon, n);
//    return {pos, lo, hi};
//    }

    public native void create(long[] array, List<Segment> segments);
}
