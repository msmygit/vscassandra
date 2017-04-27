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
package org.apache.cassandra.repair.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairVerbs.RepairVersion;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class PrepareMessage extends RepairMessage<PrepareMessage>
{
    public final static Versioned<RepairVersion, MessageSerializer<PrepareMessage>> serializers = RepairVersion.versioned(v -> new MessageSerializer<PrepareMessage>(v)
    {
        public void serialize(PrepareMessage message, DataOutputPlus out) throws IOException
        {
            out.writeInt(message.tableIds.size());
            for (TableId tableId : message.tableIds)
                tableId.serialize(out);
            UUIDSerializer.serializer.serialize(message.parentRepairSession, out);
            out.writeInt(message.ranges.size());
            for (Range<Token> r : message.ranges)
            {
                MessagingService.validatePartitioner(r);
                Range.tokenSerializer.serialize(r, out, version.boundsVersion);
            }
            out.writeBoolean(message.isIncremental);
            out.writeLong(message.timestamp);
            out.writeBoolean(message.isGlobal);
            out.writeInt(message.previewKind.getSerializationVal());
        }

        public PrepareMessage deserialize(DataInputPlus in) throws IOException
        {
            int tableIdCount = in.readInt();
            List<TableId> tableIds = new ArrayList<>(tableIdCount);
            for (int i = 0; i < tableIdCount; i++)
                tableIds.add(TableId.deserialize(in));
            UUID parentRepairSession = UUIDSerializer.serializer.deserialize(in);
            int rangeCount = in.readInt();
            List<Range<Token>> ranges = new ArrayList<>(rangeCount);
            for (int i = 0; i < rangeCount; i++)
                ranges.add((Range<Token>) Range.tokenSerializer.deserialize(in, MessagingService.globalPartitioner(), version.boundsVersion));
            boolean isIncremental = in.readBoolean();
            long timestamp = in.readLong();
            boolean isGlobal = in.readBoolean();
            PreviewKind previewKind = PreviewKind.deserialize(in.readInt());
            return new PrepareMessage(parentRepairSession, tableIds, ranges, isIncremental, timestamp, isGlobal, previewKind);
        }

        public long serializedSize(PrepareMessage message)
        {
            long size = TypeSizes.sizeof(message.tableIds.size());
            for (TableId tableId : message.tableIds)
                size += tableId.serializedSize();
            size += UUIDSerializer.serializer.serializedSize(message.parentRepairSession);
            size += TypeSizes.sizeof(message.ranges.size());
            for (Range<Token> r : message.ranges)
                size += Range.tokenSerializer.serializedSize(r, version.boundsVersion);
            size += TypeSizes.sizeof(message.isIncremental);
            size += TypeSizes.sizeof(message.timestamp);
            size += TypeSizes.sizeof(message.isGlobal);
            size += TypeSizes.sizeof(message.previewKind.getSerializationVal());
            return size;
        }
    });

    public final List<TableId> tableIds;
    public final Collection<Range<Token>> ranges;

    public final UUID parentRepairSession;
    public final boolean isIncremental;
    public final long timestamp;
    public final boolean isGlobal;
    public final PreviewKind previewKind;

    public PrepareMessage(UUID parentRepairSession, List<TableId> tableIds, Collection<Range<Token>> ranges, boolean isIncremental, long timestamp, boolean isGlobal,
                          PreviewKind previewKind)
    {
        super(null);
        this.parentRepairSession = parentRepairSession;
        this.tableIds = tableIds;
        this.ranges = ranges;
        this.isIncremental = isIncremental;
        this.timestamp = timestamp;
        this.isGlobal = isGlobal;
        this.previewKind = previewKind;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof PrepareMessage))
            return false;
        PrepareMessage other = (PrepareMessage) o;
        return parentRepairSession.equals(other.parentRepairSession) &&
               isIncremental == other.isIncremental &&
               isGlobal == other.isGlobal &&
               timestamp == other.timestamp &&
               tableIds.equals(other.tableIds) &&
               ranges.equals(other.ranges);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parentRepairSession, isGlobal, isIncremental, timestamp, tableIds, ranges);
    }

    public MessageSerializer<PrepareMessage> serializer(RepairVersion version)
    {
        return serializers.get(version);
    }

    public Verb<PrepareMessage, ?> verb()
    {
        return Verbs.REPAIR.PREPARE;
    }

    @Override
    public String toString()
    {
        return "PrepareMessage{" +
               "tableIds='" + tableIds + '\'' +
               ", ranges=" + ranges +
               ", parentRepairSession=" + parentRepairSession +
               ", isIncremental=" + isIncremental +
               ", timestamp=" + timestamp +
               ", isGlobal=" + isGlobal +
               '}';
    }
}
