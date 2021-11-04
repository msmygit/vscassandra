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

import org.apache.lucene.codecs.MultiLevelSkipListReader;
import org.apache.lucene.codecs.MultiLevelSkipListWriter;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

public class LuceneSkipTest
{
    static final int MAX_SKIP_LEVELS = 10;

    @Test
    public void test() throws Exception
    {
        ByteBuffersDirectory dir = new ByteBuffersDirectory();

        int blockSize = 128;
        int df = (128 * 10);

        LuceneSkipWriter skipWriter = new LuceneSkipWriter(blockSize,
                                                           8,
                                                           MAX_SKIP_LEVELS,
                                                           df);

        long skipFP = -1;
        try (IndexOutput output = dir.createOutput("test", IOContext.DEFAULT))
        {
            for (int doc = 128; doc < df; doc += 128)
            {
                System.out.println("bufferSkip doc="+doc);
                skipWriter.bufferSkip(doc, doc);
            }

            skipFP = skipWriter.writeSkip(output);
        }

        try (IndexInput input = dir.openInput("test", IOContext.DEFAULT))
        {
            LuceneSkipReader reader = new LuceneSkipReader(input, MAX_SKIP_LEVELS, blockSize);
            reader.init(skipFP, df);

            int skipDoc = reader.skipTo(500) + 1;
            System.out.println("skipDoc=" + skipDoc);
        }
    }

    public static class LuceneSkipReader extends MultiLevelSkipListReader
    {
        public LuceneSkipReader(IndexInput skipStream, int maxSkipLevels, int blockSize)
        {
            super(skipStream, maxSkipLevels, blockSize, 8);
        }

        protected int trim(int df)
        {
            return df % 128 == 0 ? df - 1 : df;
        }

        public void init(long skipPointer, int df) throws IOException
        {
            super.init(skipPointer, trim(df));
        }

        @Override
        protected int readSkipData(int level, IndexInput skipStream) throws IOException
        {
            return skipStream.readVInt();
        }
    }

    public static class LuceneSkipWriter extends MultiLevelSkipListWriter
    {
        private boolean initialized = false;
        private int curDoc;
        private int[] lastSkipDoc;

        public LuceneSkipWriter(int skipInterval, int skipMultiplier, int maxSkipLevels, int df)
        {
            super(skipInterval, skipMultiplier, maxSkipLevels, df);
            lastSkipDoc = new int[maxSkipLevels];
        }

        public void bufferSkip(int doc, int numDocs) throws IOException
        {
            initSkip();
            this.curDoc = doc;
            bufferSkip(numDocs);
        }

        public void initSkip()
        {
            if (!initialized)
            {
                super.resetSkip();
                Arrays.fill(lastSkipDoc, 0);
                initialized = true;
            }
        }

        @Override
        protected void writeSkipData(int level, IndexOutput skipBuffer) throws IOException
        {
            int delta = curDoc - lastSkipDoc[level];
            skipBuffer.writeVInt(delta);
            lastSkipDoc[level] = curDoc;
        }
    }
}
