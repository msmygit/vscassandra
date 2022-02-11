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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.TreeMap;

import org.agrona.collections.LongArrayList;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.io.IndexInputReader;
import org.apache.cassandra.index.sai.disk.v1.LongArray;
import org.apache.cassandra.index.sai.disk.v1.SegmentMetadata;
import org.apache.cassandra.index.sai.disk.v1.bitpack.MonotonicBlockPackedReader;
import org.apache.cassandra.index.sai.disk.v1.bitpack.NumericValuesMeta;
import org.apache.cassandra.index.sai.disk.v1.postings.MergePostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.OrdinalPostingList;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader;
import org.apache.cassandra.index.sai.disk.v1.postings.PostingsWriter;
import org.apache.cassandra.index.sai.metrics.QueryEventListener;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import static org.apache.cassandra.index.sai.disk.v3.SegmentNumericValuesWriter.MONOTONIC_BLOCK_SIZE;

public class UpperPostings
{
    public static class Reader implements AutoCloseable
    {
        private final BlockTerms.Reader reader;
        private final int baseUnit;
        private final FileHandle upperPostingsFile;
        private final SortedMap<Integer, LongArray> levelFPMap = new TreeMap<>();

        public Reader(final int baseUnit,
                      final BlockTerms.Reader reader,
                      final Map<Integer,NumericValuesMeta> levelNumericMeta) throws IOException
        {
            this.baseUnit = baseUnit;
            this.reader = reader;

            final FileHandle upperPostingsOffsetsFile = reader.indexFiles.getFileAndCache(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS);

            for (Map.Entry<Integer,NumericValuesMeta> entry : levelNumericMeta.entrySet())
            {
                final MonotonicBlockPackedReader numericReader = new MonotonicBlockPackedReader(upperPostingsOffsetsFile, entry.getValue());
                final LongArray array = numericReader.open();
                levelFPMap.put(entry.getKey(), array);
            }

            this.upperPostingsFile = reader.indexFiles.getFileAndCache(IndexComponent.BLOCK_UPPER_POSTINGS);
        }

        public Reader(final BlockTerms.Reader reader) throws IOException
        {
            this.reader = reader;

            final FileHandle upperPostingsOffsetsFile = reader.indexFiles.getFileAndCache(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS);

            final SegmentMetadata.ComponentMetadata compMeta = reader.componentMetadatas.get(IndexComponent.BLOCK_UPPER_POSTINGS);
            final Meta meta = Meta.create(compMeta.attributes);

            this.baseUnit = meta.baseUnit;

            for (Map.Entry<Integer,NumericValuesMeta> entry : meta.levelMetas.entrySet())
            {
                final MonotonicBlockPackedReader numericReader = new MonotonicBlockPackedReader(upperPostingsOffsetsFile, entry.getValue());
                final LongArray array = numericReader.open();
                levelFPMap.put(entry.getKey(), array);
            }

            this.upperPostingsFile = reader.indexFiles.getFileAndCache(IndexComponent.BLOCK_UPPER_POSTINGS);
        }

        @Override
        public void close() throws Exception
        {
            for (LongArray array : levelFPMap.values())
                array.close();
            upperPostingsFile.close();
        }

        public void search(final long startPoint,
                           final long endPoint,
                           final PriorityQueue<PostingList.PeekablePostingList> queue) throws IOException
        {
            final IndexInput upperPostingsInput = IndexInputReader.create(upperPostingsFile);
            final IndexInput postingsInput = IndexInputReader.create(reader.postingsHandle);

            final int firstLevel = 0;
            final int lastLevel = levelFPMap.lastKey();

            //System.out.println("firstLevel="+firstLevel+" lastLevel="+lastLevel);

            long gatheredStart = Integer.MAX_VALUE;
            long gatheredEnd = -1;

            int postingsCount = 0;

            for (int level = lastLevel; level >= firstLevel; level--)
            {
                final LongArray levelPostingFPs = levelFPMap.get(level);

                final int levelUnit = (int) Math.pow(baseUnit, level);
                final long levelLength = levelUnit * reader.meta.postingsBlockSize;

                final int startDerivedIdx = (int)(startPoint / levelLength);
                final int endDerivedIdx = (int)(endPoint / levelLength);

                final LongArray fps = levelFPMap.get(level);

                final long levelGatheredStart = gatheredStart;
                final long levelGatheredEnd = gatheredEnd;

                boolean resetToEnd = false;
                // iterate forwards over the level file pointers
                // process the first section
                // then skip to the last section
                for (int x = startDerivedIdx; x < fps.length(); x++)
                {
                    if (x > endDerivedIdx)
                        break;

                    final long startPoint2 = x * levelLength;
                    final long endPoint2 = (x * levelLength) + levelLength - 1;

                    if ((startPoint2 >= startPoint && endPoint2 <= endPoint)
                        && (startPoint2 < levelGatheredStart || endPoint2 > levelGatheredEnd))
                    {
                        System.out.println("level=" + level
                                           + " index=" + x
                                           + " startDerivedIdx=" + startDerivedIdx
                                           + " endDerivedIdx=" + endDerivedIdx
                                           + " startPoint=" + startPoint
                                           + " endPoint=" + endPoint
                                           + " startPoint2=" + startPoint2
                                           + " endPoint2=" + endPoint2
                                           + " levelGatheredStart=" + levelGatheredStart
                                           + " levelGatheredEnd=" + levelGatheredEnd
                                           + " gatheredStart=" + gatheredStart
                                           + " gatheredEnd=" + gatheredEnd);
                        gatheredStart = Math.min(gatheredStart, startPoint2);
                        gatheredEnd = Math.max(gatheredEnd, endPoint2);

                        final long fp = levelPostingFPs.get(x);

                        // leaf level zero postings are in a separate file
                        if (level == 0)
                        {
                            PostingsReader postings = new PostingsReader(postingsInput, fp, QueryEventListener.PostingListEventListener.NO_OP);
                            queue.add(postings.peekable());
                        }
                        else // upper level postings in a separate file
                        {
                            PostingsReader postings = new PostingsReader(upperPostingsInput, fp, QueryEventListener.PostingListEventListener.NO_OP);
                            queue.add(postings.peekable());
                        }

                        postingsCount++;
                    }
                    else
                    {
                        // after the first section of postings are gathered
                        // skip to the last section
                        if (endPoint2 > levelGatheredStart && !resetToEnd)
                        {
                            x = (int)(gatheredEnd / levelLength);
                            resetToEnd = true;
                        }
                        System.out.println("levelno=" + level
                                           + " index=" + x
                                           + " startDerivedIdx=" + startDerivedIdx
                                           + " endDerivedIdx=" + endDerivedIdx
                                           + " index=" + x
                                           + " startPoint=" + startPoint
                                           + " endPoint=" + endPoint
                                           + " startPoint2=" + startPoint2
                                           + " endPoint2=" + endPoint2
                                           + " levelGatheredStart=" + levelGatheredStart
                                           + " levelGatheredEnd=" + levelGatheredEnd
                                           + " gatheredStart=" + gatheredStart
                                           + " gatheredEnd=" + gatheredEnd);
                    }
                }
            }
           //  System.out.println("postingsCount="+postingsCount);
        }
    }

    public static class MetaCRC
    {
        public final FileValidation.Meta upperPostingsCRC, upperPostingsOffsetsCRC;
        public final Meta meta;

        public MetaCRC(FileValidation.Meta upperPostingsCRC,
                       FileValidation.Meta upperPostingsOffsetsCRC,
                       Meta meta)
        {
            this.upperPostingsCRC = upperPostingsCRC;
            this.upperPostingsOffsetsCRC = upperPostingsOffsetsCRC;
            this.meta = meta;
        }
    }

    public static class Meta
    {
        public final Map<Integer, NumericValuesMeta> levelMetas;
        public final int baseUnit;

        public Meta(int baseUnit,
                    Map<Integer, NumericValuesMeta> levelMetas)
        {
            this.levelMetas = levelMetas;
            this.baseUnit = baseUnit;
        }

        public static Meta create(Map<String, String> stringMap)
        {
            int count = 0;
            final Map<Integer, NumericValuesMeta> levelMetas = new HashMap<>();

            final int baseUnit = Integer.parseInt(stringMap.get("baseUnit"));

            while (true)
            {
                String valueCountKey = "valueCount_" + count;
                String blockSizeKey = "blockSize_" + count;
                String blockMetaOffsetKey = "blockMetaOffset_" + count;

                String valueCount = stringMap.get(valueCountKey);
                // stop iterating when the attribute + count is not found
                if (valueCount == null)
                    break;
                String blockSize = stringMap.get(blockSizeKey);
                String blockMetaOffset = stringMap.get(blockMetaOffsetKey);
                levelMetas.put(count, new NumericValuesMeta(Long.parseLong(valueCount),
                                                            Integer.parseInt(blockSize),
                                                            Long.parseLong(blockMetaOffset)));
                count++;
            }
            return new Meta(baseUnit, levelMetas);
        }

        public Map<String, String> stringMap()
        {
            final Map<String, String> map = new HashMap<>();

            map.put("baseUnit", Integer.toString(baseUnit));

            int count = 0;
            for (Map.Entry<Integer, NumericValuesMeta> entry : levelMetas.entrySet())
            {
                final Map<String, String> subMap = entry.getValue().stringMap();

                for (Map.Entry<String, String> subEntry : subMap.entrySet())
                {
                    final String key = subEntry.getKey()+"_"+count;
                    map.put(key, subEntry.getValue());
                }
                count++;
            }
            return map;
        }
    }

    public static class Writer
    {
        private final BlockTerms.Reader reader;
        private final int baseUnit = 10;

        public Writer(BlockTerms.Reader reader) throws IOException
        {
            this.reader = reader;
        }

        /**
         * For each level gather the previous level's postings up to the baseUnit.
         * @param components Component meta data
         * @param segmented Segmented
         * @return Level -> numeric values meta
         * @throws IOException
         */
        public MetaCRC finish(final SegmentMetadata.ComponentMetadataMap components,
                              boolean segmented) throws IOException
        {
            int level = 1; // level 0 is leaf level posting blocks

            LongArray previousLevelPostingFPs = reader.postingBlockFPs();

            final SortedMap<Integer, LongArray> levelFPMap = new TreeMap<>();

            levelFPMap.put(0, reader.postingBlockFPs());

            while (true)
            {
                final LongArrayList postingsFPs = processLevel(level, segmented, previousLevelPostingFPs);

                if (postingsFPs == null)
                    break;

                levelFPMap.put(level, new LongArrayImpl(postingsFPs));
                // System.out.println("leafFPs.size="+postingsFPs.size());

                level++;
                previousLevelPostingFPs = new LongArrayImpl(postingsFPs);
            }

            final long upperPostingsStartFP = getEndFP(IndexComponent.BLOCK_UPPER_POSTINGS, segmented);
            final long upperPostingsOffsetsStartFP = getEndFP(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS, segmented);

            final Map<Integer,NumericValuesMeta> levelMetas = new HashMap<>();

            try (final IndexOutput output = reader.indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS, reader.context, true, segmented))
            {
                for (Map.Entry<Integer,LongArray> entry : levelFPMap.entrySet())
                {
                    final SegmentNumericValuesWriter numericWriter = new SegmentNumericValuesWriter(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS,
                                                                                                    output,
                                                                                                    null,
                                                                                                    true,
                                                                                                    MONOTONIC_BLOCK_SIZE);
                    for (int x = 0; x < entry.getValue().length(); x++)
                    {
                        numericWriter.add(entry.getValue().get(x));
                    }

                    numericWriter.close();

                    levelMetas.put(entry.getKey(), numericWriter.meta());
                }
            }

            final long upperPostingsLength = getEndFP(IndexComponent.BLOCK_UPPER_POSTINGS, segmented) - upperPostingsStartFP;
            final long upperPostingsOffsetsLength = getEndFP(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS, segmented) - upperPostingsOffsetsStartFP;

            components.put(IndexComponent.BLOCK_UPPER_POSTINGS, upperPostingsStartFP, upperPostingsStartFP, upperPostingsLength, reader.meta.stringMap());
            components.put(IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS, upperPostingsOffsetsStartFP, upperPostingsOffsetsStartFP, upperPostingsOffsetsLength, reader.meta.stringMap());

            final FileValidation.Meta upperPostingsCRC = createCRCMeta(upperPostingsStartFP, IndexComponent.BLOCK_UPPER_POSTINGS, segmented);
            final FileValidation.Meta upperPostingsOffsetsCRC = createCRCMeta(upperPostingsOffsetsStartFP, IndexComponent.BLOCK_UPPER_POSTINGS_OFFSETS, segmented);

            return new MetaCRC(upperPostingsCRC, upperPostingsOffsetsCRC, new Meta(baseUnit, levelMetas));
        }

        private FileValidation.Meta createCRCMeta(long rootFP, IndexComponent comp, boolean segmented) throws IOException
        {
            try (IndexInput input = reader.indexDescriptor.openPerIndexInput(comp, reader.context, segmented))
            {
                return FileValidation.createValidate(rootFP, input);
            }
        }

        private long getEndFP(IndexComponent comp, boolean segmented) throws IOException
        {
            try (IndexOutput output = reader.indexDescriptor.openPerIndexOutput(comp, reader.context, true, segmented))
            {
                return output.getFilePointer();
            }
        }

        private LongArrayList processLevel(final int level,
                                           final boolean segmented,
                                           final LongArray previousLevelPostingFPs) throws IOException
        {
            // System.out.println("processBlock level="+level+" baseUnit="+baseUnit);

            final LongArrayList levelFPs = new LongArrayList();
            final LongArrayList prevLevelFPs = new LongArrayList();
            for (int x = 0; x < previousLevelPostingFPs.length(); x++)
            {
                final long prevFP = previousLevelPostingFPs.get(x);
                prevLevelFPs.add(prevFP);

                if (prevLevelFPs.size() == baseUnit)
                {
                    final long fp = writePostings(level, segmented, prevLevelFPs);
                    levelFPs.add(fp);

                    prevLevelFPs.clear();
                }
            }

            // write remainder
            if (prevLevelFPs.size() > 0)
            {
                final long fp = writePostings(level, segmented, prevLevelFPs);
                levelFPs.add(fp);
            }

            if (levelFPs.size() <= 1)
                return null;

            return levelFPs;
        }

        private long writePostings(final int level,
                                   final boolean segmented,
                                   final LongArrayList prevLevelFPs) throws IOException
        {
            // file handles must be opened and closed per level
            // to ensure the previously written level postings available for reads
            final List<FileHandle> fileHandlesToClose = new ArrayList();

            final PriorityQueue<PostingList.PeekablePostingList> queue = new PriorityQueue<>(Comparator.comparingLong(PostingList.PeekablePostingList::peek));
            for (int index = 0; index < prevLevelFPs.size(); index++)
            {
                final long fp = prevLevelFPs.get(index);
                if (level == 1)
                {
                    final OrdinalPostingList postings = reader.openBlockPostingsFP(fp);
                    if (postings.size() > 0)
                        queue.add(postings.peekable());
                }
                else
                {
                    assert level > 1;

                    // make sure to close file handle because more is written and read
                    final FileHandle upperPostingFile = reader.indexFiles.getFile(IndexComponent.BLOCK_UPPER_POSTINGS);
                    final IndexInput upperPostingInput = IndexInputReader.create(upperPostingFile);

                    fileHandlesToClose.add(upperPostingFile);

                    final PostingList.PeekablePostingList postings = new PostingsReader(upperPostingInput, fp, QueryEventListener.PostingListEventListener.NO_OP).peekable();

                    queue.add(postings);
                }
            }
            final PostingList mergedPostings = MergePostingList.merge(queue);

            // System.out.println("postings.size="+mergedPostings.size());

            try
            {
                try (IndexOutput upperPostingsOut = reader.indexDescriptor.openPerIndexOutput(IndexComponent.BLOCK_UPPER_POSTINGS, reader.context, true, segmented))
                {
                    PostingsWriter postingsWriter = new PostingsWriter(upperPostingsOut);
                    long fp = postingsWriter.write(mergedPostings);
                    return fp;
                }
            }
            finally
            {
                for (FileHandle handle : fileHandlesToClose)
                {
                    handle.close();
                }
            }
        }
    }

    public static class LongArrayImpl implements LongArray
    {
        final LongArrayList list;

        public LongArrayImpl(LongArrayList list)
        {
            this.list = list;
        }

        @Override
        public long get(long idx)
        {
            return list.getLong((int)idx);
        }

        @Override
        public long length()
        {
            return list.size();
        }

        @Override
        public long findTokenRowID(long targetToken)
        {
            throw new UnsupportedOperationException();
        }
    }
}
