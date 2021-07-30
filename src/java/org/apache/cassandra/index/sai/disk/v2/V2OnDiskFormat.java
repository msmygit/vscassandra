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

package org.apache.cassandra.index.sai.disk.v2;

import org.apache.cassandra.index.sai.disk.format.IndexComponent;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.V1OnDiskFormat;

import static org.apache.cassandra.index.sai.disk.format.IndexDescriptor.SAI_DESCRIPTOR;

public class V2OnDiskFormat extends V1OnDiskFormat
{
    private static final String SAI_SEPARATOR = "+";
    private static final String EXTENSION = ".db";

    public static final V2OnDiskFormat instance = new V2OnDiskFormat();

    protected V2OnDiskFormat()
    {}

    @Override
    public String componentName(IndexComponent indexComponent, String index)
    {
        StringBuilder stringBuilder = new StringBuilder();

        stringBuilder.append(SAI_DESCRIPTOR);
        stringBuilder.append(SAI_SEPARATOR).append(Version.BA);
        if (index != null)
            stringBuilder.append(SAI_SEPARATOR).append(index);
        stringBuilder.append(SAI_SEPARATOR).append(indexComponent.representation);
        stringBuilder.append(EXTENSION);

        return stringBuilder.toString();
    }
}
