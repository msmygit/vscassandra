/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

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

package org.apache.cassandra.guardrails;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.schema.Schema;

import static java.lang.String.format;

public class GuardrailKeyspacesTest extends GuardrailTester
{
    private static final long WARN_THRESHOLD = 3; // CQLTester creates two keyspaces
    private static final long FAIL_THRESHOLD = WARN_THRESHOLD + 1;
    private static long defaultKeyspacesSoftLimit;
    private static long defaultKeyspacesHardLimit;

    @BeforeClass
    public static void setup()
    {
        defaultKeyspacesSoftLimit = DatabaseDescriptor.getGuardrailsConfig().keyspaces_warn_threshold;
        defaultKeyspacesHardLimit = DatabaseDescriptor.getGuardrailsConfig().keyspaces_failure_threshold;
    }

    @AfterClass
    public static void tearDown()
    {
        DatabaseDescriptor.getGuardrailsConfig().keyspaces_warn_threshold = defaultKeyspacesSoftLimit;
        DatabaseDescriptor.getGuardrailsConfig().keyspaces_failure_threshold = defaultKeyspacesHardLimit;
    }

    protected long currentValue()
    {
        return Schema.instance.getUserKeyspaces().size();
    }
    
    @Test
    public void testCreateKeyspace() throws Throwable
    {
        DatabaseDescriptor.getGuardrailsConfig().keyspaces_warn_threshold = WARN_THRESHOLD;
        DatabaseDescriptor.getGuardrailsConfig().keyspaces_failure_threshold = FAIL_THRESHOLD;

        // create keyspaces until hitting the two warn/abort thresholds
        String k1 = assertCreateKeyspaceValid();
        String k2 = assertCreateKeyspaceWarns();
        assertCreateKeyspaceAborts();

        // drop a keyspace and hit the warn/abort threshold again
        dropKeyspace(k2);
        String k3 = assertCreateKeyspaceWarns();
        assertCreateKeyspaceAborts();

        // drop two keyspaces and hit the warn/abort threshold again
        dropKeyspace(k1);
        dropKeyspace(k3);
        assertCreateKeyspaceValid();
        assertCreateKeyspaceWarns();
        assertCreateKeyspaceAborts();

        // test excluded users
        assertSuperuserIsExcluded(createKeyspaceQuery(),
                          createKeyspaceQuery(),
                          createKeyspaceQuery());
        assertInternalQueriesAreExcluded(createKeyspaceQuery(),
                          createKeyspaceQuery(),
                          createKeyspaceQuery());
    }

    private void dropKeyspace(String keyspaceName)
    {
        schemaChange(format("DROP KEYSPACE %s", keyspaceName));
    }

    private String assertCreateKeyspaceValid() throws Throwable
    {
        String keyspaceName = createKeyspaceName();
        assertValid(createKeyspaceQuery(keyspaceName));
        return keyspaceName;
    }

    private String assertCreateKeyspaceWarns() throws Throwable
    {
        String keyspaceName = createKeyspaceName();
        assertWarns(format("Creating keyspace %s, current number of keyspaces %d exceeds warning threshold of %d",
                                    keyspaceName, currentValue() + 1, WARN_THRESHOLD),
                             createKeyspaceQuery(keyspaceName));
        return keyspaceName;
    }

    private void assertCreateKeyspaceAborts() throws Throwable
    {
        String keyspaceName = createKeyspaceName();
        assertFails(format("Cannot have more than %d keyspaces, failed to create keyspace %s",
                           FAIL_THRESHOLD, keyspaceName),
                              createKeyspaceQuery(keyspaceName));
    }

    private String createKeyspaceQuery()
    {
        return createKeyspaceQuery(createKeyspaceName());
    }

    private String createKeyspaceQuery(String keyspaceName)
    {
        return format("CREATE KEYSPACE %s WITH replication={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 }",
                      keyspaceName);
    }
}