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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.index.sai.disk.io.IndexComponents;
import org.apache.cassandra.index.sai.utils.NdiRandomizedTest;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.codecs.MultiLevelSkipListReader;
import org.apache.lucene.codecs.MultiLevelSkipListWriter;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class LuceneSkipTest extends NdiRandomizedTest
{
    static final int MAX_SKIP_LEVELS = 10;

    // TODO: add postings file pointers
    @Test
    public void test() throws Exception
    {
        //ByteBuffersDirectory dir = new ByteBuffersDirectory();
        IndexComponents comps = newIndexComponents();

        int blockSize = 128;
        int df = (128 * 10);

        long skipFP = -1;
        //try (IndexOutput output = dir.createOutput("test", IOContext.DEFAULT))
        try (IndexOutput output = comps.createOutput(comps.termsData);
             IndexOutput postingsOut = comps.createOutput(comps.postingLists))
        {
            LuceneSkipWriter skipWriter = new LuceneSkipWriter(blockSize,
                                                               8,
                                                               MAX_SKIP_LEVELS,
                                                               df,
                                                               postingsOut);

            for (int doc = 128; doc < df; doc += 128)
            {
                System.out.println("bufferSkip doc="+doc);
                skipWriter.bufferSkip(doc, doc);
                byte[] fake = new byte[] {99, 100};
                postingsOut.writeBytes(fake, 0, fake.length);
            }
            skipFP = skipWriter.writeSkip(output);
        }

        //try (IndexInput input = dir.openInput("test", IOContext.DEFAULT))
        try (FileHandle fileHandle = comps.createFileHandle(comps.termsData);
             IndexInput input = comps.openLuceneInput(fileHandle))
        {
            LuceneSkipReader reader = new LuceneSkipReader(input, MAX_SKIP_LEVELS, blockSize);
            reader.init(skipFP, df);

            int skipDoc = reader.skipTo(500) + 1;

            System.out.println("skipDoc=" + skipDoc+" lastDocPointer="+reader.lastDocPointer);
        }
    }

    public static class LuceneSkipReader extends MultiLevelSkipListReader
    {
        private final long[] docPointer;
        private long lastDocPointer;

        public LuceneSkipReader(IndexInput skipStream, int maxSkipLevels, int blockSize)
        {
            super(skipStream, maxSkipLevels, blockSize, 8);
            docPointer = new long[maxSkipLevels];
        }

        @Override
        protected void seekChild(int level) throws IOException {
            super.seekChild(level);
            docPointer[level] = lastDocPointer;
        }

        @Override
        protected void setLastSkipData(int level) {
            super.setLastSkipData(level);
            lastDocPointer = docPointer[level];
        }

        protected int trim(int df)
        {
            return df % 128 == 0 ? df - 1 : df;
        }

        public void init(long skipPointer, int df) throws IOException
        {
            super.init(skipPointer, trim(df));
            Arrays.fill(docPointer, 0);
        }

        @Override
        protected int readSkipData(int level, IndexInput skipStream) throws IOException {
            int delta = skipStream.readVInt();
            docPointer[level] += skipStream.readVLong();
            return delta;
        }
    }

    public static class LuceneSkipWriter extends MultiLevelSkipListWriter
    {
        private boolean initialized = false;
        private int curDoc;
        private long curDocPointer;
        private int[] lastSkipDoc;
        private long[] lastSkipDocPointer;
        private long lastDocFP;

        private final IndexOutput postingsOut;

        public LuceneSkipWriter(int skipInterval,
                                int skipMultiplier,
                                int maxSkipLevels,
                                int df,
                                IndexOutput postingsOut)
        {
            super(skipInterval, skipMultiplier, maxSkipLevels, df);
            this.postingsOut = postingsOut;
            lastSkipDoc = new int[maxSkipLevels];
            lastSkipDocPointer = new long[maxSkipLevels];
        }

        @Override
        public void resetSkip() {
            lastDocFP = postingsOut.getFilePointer();
            initialized = false;
        }

        public void bufferSkip(int doc, int numDocs) throws IOException
        {
            initSkip();
            this.curDoc = doc;
            this.curDocPointer = postingsOut.getFilePointer();
            bufferSkip(numDocs);
        }

        public void initSkip()
        {
            if (!initialized)
            {
                super.resetSkip();
                Arrays.fill(lastSkipDoc, 0);
                Arrays.fill(lastSkipDocPointer, lastDocFP);
                initialized = true;
            }
        }

        @Override
        protected void writeSkipData(int level, IndexOutput skipBuffer) throws IOException
        {
            int delta = curDoc - lastSkipDoc[level];
            skipBuffer.writeVInt(delta);
            lastSkipDoc[level] = curDoc;

            skipBuffer.writeVLong(curDocPointer - lastSkipDocPointer[level]);
            lastSkipDocPointer[level] = curDocPointer;
        }
    }
}
