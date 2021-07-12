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

package org.apache.cassandra.repair;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MerkleTrees;
import org.apache.cassandra.utils.UUIDGen;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Matchers;
import org.mockito.Mockito;

/**
 * The purpose of this test is to verify whether the Merkle trees are consistent for the exactly same table rows,
 * regardless of the number of sstables, whether there was a compaction, what is the history, etc.
 */
public class DigestConsistencyTest extends CQLTester
{
    long timestamp;

    @Before
    public void before()
    {
        timestamp = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
    }

    @Test
    public void testForCKAndStatic_fullyFilledRows() throws Exception
    {
        testScenarios(
                "CREATE TABLE %s (pk INT, ck INT, s INT STATIC, v INT, PRIMARY KEY (pk, ck))",
                i -> String.format("INSERT INTO %%s (pk, ck, s, v) VALUES (%d, 1, 1, 1) USING TIMESTAMP %d", i, timestamp),
                0);
    }

    @Test
    public void testForCKAndStatic_missingStatic() throws Exception
    {
        testScenarios(
                "CREATE TABLE %s (pk INT, ck INT, s INT STATIC, v INT, PRIMARY KEY (pk, ck))",
                i -> String.format("INSERT INTO %%s (pk, ck, v) VALUES (%d, 1, 1) USING TIMESTAMP %d", i, timestamp),
                0);
    }

    @Test
    public void testForCKAndStatic_missingValue() throws Exception
    {
        testScenarios(
                "CREATE TABLE %s (pk INT, ck INT, s INT STATIC, v INT, PRIMARY KEY (pk, ck))",
                i -> String.format("INSERT INTO %%s (pk, ck, s) VALUES (%d, 1, 1) USING TIMESTAMP %d", i, timestamp),
                0);
    }

    @Test
    public void testForCKAndStatic_missingStaticAndValue() throws Exception
    {
        testScenarios(
                "CREATE TABLE %s (pk INT, ck INT, s INT STATIC, v INT, PRIMARY KEY (pk, ck))",
                i -> String.format("INSERT INTO %%s (pk, ck) VALUES (%d, 1) USING TIMESTAMP %d", i, timestamp),
                0);
    }

    @Test
    public void testForGCGrace_mapTombstone() throws Exception
    {
        testScenarios(
                "CREATE TABLE %s (pk INT PRIMARY KEY, v MAP<INT,INT>) WITH gc_grace_seconds = 5",
                i -> String.format("INSERT INTO %%s (pk, v) VALUES (%d, {1:1}) USING TIMESTAMP %d", i, timestamp),
                10);
    }

    private void testScenarios(String createTableQuery, Function<Integer, String> query, int compactionDelay) throws Exception
    {
        Map<String, String> results = new HashMap<>();

        results.put("flush after inserting two rows", executeAndGetHash(createTableQuery, () -> {
            sessionNet().execute(formatQuery(query.apply(1)));
            sessionNet().execute(formatQuery(query.apply(2)));
            flush(true);
            Uninterruptibles.sleepUninterruptibly(compactionDelay, TimeUnit.SECONDS);
        }));

        results.put("flush after inserting each row", executeAndGetHash(createTableQuery, () -> {
            sessionNet().execute(formatQuery(query.apply(1)));
            flush(true);
            sessionNet().execute(formatQuery(query.apply(2)));
            flush(true);
            Uninterruptibles.sleepUninterruptibly(compactionDelay, TimeUnit.SECONDS);
        }));

        results.put("flush after inserting each row, then compact", executeAndGetHash(createTableQuery, () -> {
            sessionNet().execute(formatQuery(query.apply(1)));
            flush(true);
            sessionNet().execute(formatQuery(query.apply(2)));
            flush(true);
            Uninterruptibles.sleepUninterruptibly(compactionDelay, TimeUnit.SECONDS);
            compact();
        }));

        results.put("flush after inserting first row", executeAndGetHash(createTableQuery, () -> {
            sessionNet().execute(formatQuery(query.apply(1)));
            flush(true);
            sessionNet().execute(formatQuery(query.apply(2)));
            Uninterruptibles.sleepUninterruptibly(compactionDelay, TimeUnit.SECONDS);
        }));

        Map<String, List<Map.Entry<String, String>>> groups = results.entrySet().stream().collect(Collectors.groupingBy(Map.Entry::getValue));
        Assert.assertEquals("We should have one group but there are more: " + groups, 1, groups.size());
    }

    private String executeAndGetHash(String createTableQuery, Runnable code) throws Exception
    {
        DataRange dataRange = DataRange.allData(DatabaseDescriptor.getPartitioner());
        Range<Token> range = new Range<>(dataRange.startKey().getToken(), dataRange.stopKey().getToken());
        String tableName = createTable(createTableQuery);
        code.run();
        MerkleTrees mt = getMerkleTrees(tableName, range);
        return Hex.encodeHexString(mt.hash(range));
    }

    private MerkleTrees getMerkleTrees(String tableName, Range<Token> range) throws Exception
    {
        ColumnFamilyStore cfs = Keyspace.open(CQLTester.KEYSPACE).getColumnFamilyStore(tableName);
        UUID repairSessionId = UUIDGen.getTimeUUID();
        final RepairJobDesc desc = new RepairJobDesc(
                repairSessionId,
                UUIDGen.getTimeUUID(),
                cfs.keyspace.getName(),
                cfs.getTableName(),
                Collections.singletonList(range));

        ActiveRepairService.instance.registerParentRepairSession(
                repairSessionId,
                FBUtilities.getBroadcastAddressAndPort(),
                Collections.singletonList(cfs),
                desc.ranges,
                false,
                ActiveRepairService.UNREPAIRED_SSTABLE,
                false,
                PreviewKind.NONE);

        Validator validator = Mockito.spy(new Validator(desc, FBUtilities.getBroadcastAddressAndPort(), (int) (System.currentTimeMillis() / 1000), true, false, PreviewKind.NONE));
        ValidationManager.instance.submitValidation(cfs, validator).get(10, TimeUnit.MINUTES);

        ArgumentCaptor<MerkleTrees> mtArgCaptor = ArgumentCaptor.forClass(MerkleTrees.class);
        Mockito.verify(validator).prepare(ArgumentMatchers.any(ColumnFamilyStore.class), mtArgCaptor.capture());
        return mtArgCaptor.getValue();
    }
}