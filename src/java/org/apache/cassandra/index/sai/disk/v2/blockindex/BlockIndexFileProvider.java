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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.io.IndexOutputWriter;
import org.apache.cassandra.index.sai.utils.SharedIndexInput;
import org.apache.cassandra.io.util.FileHandle;
import org.apache.lucene.store.IndexInput;

public interface BlockIndexFileProvider extends AutoCloseable
{
    IndexOutputWriter openValuesOutput(boolean temporary) throws IOException;

    IndexOutputWriter openIndexOutput(boolean temporary) throws IOException;

    IndexOutputWriter openLeafPostingsOutput(boolean temporary) throws IOException;

    IndexOutputWriter openOrderMapOutput(boolean temporary) throws IOException;

    IndexOutputWriter openCompressedValuesOutput(boolean temporary) throws IOException;

    IndexOutputWriter openMultiPostingsOutput(boolean temporary) throws IOException;

    IndexOutputWriter openPointIdMapOutput(boolean temporary) throws IOException;

    SharedIndexInput openValuesInput(boolean temporary);

    SharedIndexInput openOrderMapInput(boolean temporary);

    SharedIndexInput openLeafPostingsInput(boolean temporary);

    SharedIndexInput openIndexInput(boolean temporary);

    SharedIndexInput openMultiPostingsInput(boolean temporary);

    SharedIndexInput openCompressedValuesInput(boolean temporary);

    FileHandle getIndexFileHandle(boolean temporary);

    HashMap<IndexComponent, FileValidator.FileInfo> fileInfoMap(boolean temporary) throws IOException;

    void validate(Map<IndexComponent, FileValidator.FileInfo> fileInfoMap, boolean temporary) throws IOException;

    void close();
}
