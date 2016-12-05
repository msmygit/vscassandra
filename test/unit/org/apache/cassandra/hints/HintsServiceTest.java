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
package org.apache.cassandra.hints;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.utils.MoreFutures;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.Util.dk;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.apache.cassandra.net.MockMessagingService.verb;

public class HintsServiceTest
{
    private static final String KEYSPACE = "hints_service_test";
    private static final String TABLE = "table";

    private final MockFailureDetector failureDetector = new MockFailureDetector();

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                KeyspaceParams.simple(1),
                SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @After
    public void cleanup()
    {
        MockMessagingService.cleanup();
    }

    @Before
    public void reinstanciateService() throws Throwable
    {
        MessagingService.instance().clearMessageSinks();

        if (!HintsService.instance.isShutDown())
        {
            HintsService.instance.shutdownBlocking();
            HintsService.instance.deleteAllHints();
        }

        failureDetector.isAlive = true;

        HintsService.instance = new HintsService(failureDetector);

        HintsService.instance.startDispatch();
    }

    @Test
    public void testDispatchHints() throws InterruptedException, ExecutionException
    {
        long cnt = StorageMetrics.totalHints.getCount();

        // create spy for hint messages
        MockMessagingSpy spy = sendHintsAndResponses(100, -1);

        // metrics should have been updated with number of create hints
        assertEquals(cnt + 100, StorageMetrics.totalHints.getCount());

        // wait until hints have been send
        spy.interceptRequests(100).get();
        spy.interceptNoMsg(500, TimeUnit.MILLISECONDS).get();
    }

    @Test
    public void testPauseAndResume() throws InterruptedException, ExecutionException
    {
        HintsService.instance.pauseDispatch();

        // create spy for hint messages
        MockMessagingSpy spy = sendHintsAndResponses(100, -1);

        // we should not send any hints while paused
        ListenableFuture<Boolean> noMessagesWhilePaused = spy.interceptNoMsg(15, TimeUnit.SECONDS);
        Futures.addCallback(noMessagesWhilePaused, new MoreFutures.SuccessCallback<Boolean>()
        {
            public void onSuccess(@Nullable Boolean aBoolean)
            {
                HintsService.instance.resumeDispatch();
            }
        });

        Futures.allAsList(
                noMessagesWhilePaused,
                spy.interceptRequests(100),
                spy.interceptNoMsg(200, TimeUnit.MILLISECONDS)
        ).get();
    }

    @Test
    public void testPageRetry() throws InterruptedException, ExecutionException, TimeoutException
    {
        // create spy for hint messages, but only create responses for 5 hints
        MockMessagingSpy spy = sendHintsAndResponses(20, 5);

        Futures.allAsList(
                // the dispatcher will always send all hints within the current page
                // and only wait for the acks before going to the next page
                spy.interceptRequests(20),
                spy.interceptNoMsg(200, TimeUnit.MILLISECONDS),

                // next tick will trigger a retry of the same page as we only replied with 5/20 acks
                spy.interceptRequests(20)
        ).get();

        // marking the destination node as dead should stop sending hints
        failureDetector.isAlive = false;
        spy.interceptNoMsg(20, TimeUnit.SECONDS).get();
    }

    @Test
    public void testPageSeek() throws InterruptedException, ExecutionException
    {
        // create spy for hint messages, stop replying after 12k (should be on 3rd page)
        MockMessagingSpy spy = sendHintsAndResponses(20000, 12000);

        // At this point the dispatcher will constantly retry the page we stopped acking,
        // thus we receive the same hints from the page multiple times and in total more than
        // all written hints. Lets just consume them for a while and then pause the dispatcher.
        spy.interceptRequests(22000).get();
        HintsService.instance.pauseDispatch();
        Thread.sleep(1000);

        // verify that we have a dispatch offset set for the page we're currently stuck at
        HintsStore store = HintsService.instance.getCatalog().get(StorageService.instance.getLocalHostUUID());
        HintsDescriptor descriptor = store.poll();
        store.offerFirst(descriptor); // add again for cleanup during re-instanciation
        InputPosition dispatchOffset = store.getDispatchOffset(descriptor);
        assertTrue(dispatchOffset != null);
        assertTrue(((ChecksummedDataInput.Position) dispatchOffset).sourcePosition > 0);
    }

    private MockMessagingSpy sendHintsAndResponses(int noOfHints, int noOfResponses)
    {
        // create spy for hint messages, but only create responses for noOfResponses hints
        Response<EmptyPayload> response = Response.testLocalAck(Verbs.HINTS.HINT);

        MockMessagingSpy spy;
        if (noOfResponses != -1)
        {
            spy = MockMessagingService.when(verb(Verbs.HINTS.HINT)).respondN(response, noOfResponses);
        }
        else
        {
            spy = MockMessagingService.when(verb(Verbs.HINTS.HINT)).respond(response);
        }

        // create and write noOfHints using service
        UUID hostId = StorageService.instance.getLocalHostUUID();
        for (int i = 0; i < noOfHints; i++)
        {
            long now = System.currentTimeMillis();
            DecoratedKey dkey = dk(String.valueOf(i));
            TableMetadata metadata = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
            PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(metadata, dkey).timestamp(now);
            builder.row("column0").add("val", "value0");
            Hint hint = Hint.create(builder.buildAsMutation(), now);
            HintsService.instance.write(hostId, hint);
        }
        return spy;
    }

    private static class MockFailureDetector implements IFailureDetector
    {
        private boolean isAlive = true;

        public boolean isAlive(InetAddress ep)
        {
            return isAlive;
        }

        public void interpret(InetAddress ep)
        {
            throw new UnsupportedOperationException();
        }

        public void report(InetAddress ep)
        {
            throw new UnsupportedOperationException();
        }

        public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
            throw new UnsupportedOperationException();
        }

        public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
            throw new UnsupportedOperationException();
        }

        public void remove(InetAddress ep)
        {
            throw new UnsupportedOperationException();
        }

        public void forceConviction(InetAddress ep)
        {
            throw new UnsupportedOperationException();
        }
    }
}
