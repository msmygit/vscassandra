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

package org.apache.cassandra.index.sai.disk.hnsw;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.slf4j.Logger;

import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.utils.Pair;

public class VectorPostingsWriter<T>
{
    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(VectorPostingsWriter.class);
    private long finishedFirstPosition;

    public void writePostings(SequentialWriter writer,
                              ConcurrentVectorValues vectorValues,
                              Map<float[], VectorPostings<T>> postingsMap,
                              Function<T, Integer> postingTransformer) throws IOException
    {
        logger.debug("Writing postings to {}", writer.getFile().toPath());
        writeNodeOrdinalToRowIdMapping(writer, vectorValues, postingsMap, postingTransformer);
        writeRowIdToNodeOrdinalMapping(writer, vectorValues, postingsMap, postingTransformer);
        writer.flush();
    }

    public void writeNodeOrdinalToRowIdMapping(SequentialWriter writer,
                                               ConcurrentVectorValues vectorValues,
                                               Map<float[], VectorPostings<T>> postingsMap,
                                               Function<T, Integer> postingTransformer) throws IOException
    {
        // total number of vectors
        writer.writeInt(vectorValues.size());

        // Write the offsets of the postings for each ordinal
        var offsetsStartAt = 4L + 8L * vectorValues.size();
        var offset = offsetsStartAt;
        for (var i = 0; i < vectorValues.size(); i++) {
            // (ordinal is implied; don't need to write it)
            writer.writeLong(offset);
            var postings = postingsMap.get(vectorValues.vectorValue(i));
            offset += 4 + (postings.size() * 4L); // 4 bytes for size and 4 bytes for each integer in the list
        }
        assert writer.position() == offsetsStartAt;

        // Write postings lists
        for (var i = 0; i < vectorValues.size(); i++) {
            var postings = postingsMap.get(vectorValues.vectorValue(i));
            writer.writeInt(postings.size());
            for (var key : postings.getPostings()) {
                writer.writeInt(postingTransformer.apply(key));
            }
        }
        assert writer.position() == offset;
        this.finishedFirstPosition = offset;
        logger.debug("Finished writing ordinal -> rows at position {}", offset);
    }

    public void writeRowIdToNodeOrdinalMapping(SequentialWriter writer,
                                               ConcurrentVectorValues vectorValues,
                                               Map<float[], VectorPostings<T>> postingsMap,
                                               Function<T, Integer> postingTransformer) throws IOException
    {
        assert writer.position() == finishedFirstPosition : String.format("Expected to be at position %d, but was at %d", finishedFirstPosition, writer.position());
        List<Pair<Integer, Integer>> pairs = new ArrayList<>();

        // Collect all (rowId, vectorOrdinal) pairs
        for (var i = 0; i < vectorValues.size(); i++) {
            var postings = postingsMap.get(vectorValues.vectorValue(i));
            for (var posting : postings.getPostings()) {
                pairs.add(Pair.create(postingTransformer.apply(posting), i));
            }
        }

        // Sort the pairs by rowId
        pairs.sort(Comparator.comparingInt(Pair::left));
        assert pairs.get(0).left == 0 : "First rowId should be 0, found " + pairs.get(0).left; // FIXME

        // Write the pairs to the file
        long startOffset = writer.position();
        for (var pair : pairs) {
            assert pair.right < vectorValues.size() : "Vector ordinal " + pair.right + " is out of bounds";
            writer.writeInt(pair.left);
            writer.writeInt(pair.right);
        }

        // write the position of the beginning of rowid -> ordinals mappings to the end of the file
        assert startOffset == finishedFirstPosition : String.format("Expected to be at position %d, but was at %d", finishedFirstPosition, startOffset);
        writer.writeLong(startOffset);
        logger.debug("Finished writing rowId -> ordinals at position {}", startOffset);
        logger.debug("File size is " + writer.position());
    }
}
