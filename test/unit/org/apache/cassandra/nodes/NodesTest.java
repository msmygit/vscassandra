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

package org.apache.cassandra.nodes;

import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

public class NodesTest
{
    private Nodes nodes;
    private INodesPersistence persistence = mock(INodesPersistence.class);


    private static InetAddressAndPort addr1;
    private static InetAddressAndPort addr2;
    private static InetAddressAndPort addr3;
    private CyclicBarrier barrier;
    private ArgumentCaptor<LocalInfo> localInfoCaptor;
    private AtomicReference<LocalInfo> localInfoRef;
    private ArgumentCaptor<PeerInfo> peerInfoCaptor;
    private AtomicReference<PeerInfo> peerInfoRef;

    @BeforeClass
    public static void beforeClass() throws Exception
    {
        addr1 = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.1", 7001);
        addr2 = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", 7001);
        addr3 = InetAddressAndPort.getByNameOverrideDefaults("127.0.0.2", 7002);
    }

    @Before
    public void beforeTest()
    {
        reset(persistence);
        nodes = new Nodes(persistence);
        barrier = new CyclicBarrier(2);
        localInfoCaptor = ArgumentCaptor.forClass(LocalInfo.class);
        localInfoRef = new AtomicReference<>();
        peerInfoCaptor = ArgumentCaptor.forClass(PeerInfo.class);
        peerInfoRef = new AtomicReference<>();
    }

    @Test
    public void getEmptyLocal()
    {
        assertThat(nodes.getLocal().get()).isEqualTo(new LocalInfo());
    }

    @Test
    public void getEmptyPeer()
    {
        assertThat(nodes.getPeers().get(addr1)).isNull();
        assertThat(nodes.getPeers().get(addr2)).isNull();
        assertThat(nodes.getPeers().get(addr3)).isNull();
    }

    @Test
    public void updateLocalNoChanges() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        nodes.getLocal().update(current -> current.setHostId(hostId), true, true);

        ForkJoinTask<LocalInfo> r = updateLocalInfo(hostId, false, false);
        await().untilAsserted(() -> assertThat(r).isDone());
        assertThat(r.get().getHostId()).isEqualTo(hostId);
        assertThat(barrier.getNumberWaiting()).isZero(); // force == false and we did not change anything
        assertThat(localInfoRef.get()).isNotNull().isNotSameAs(r); // r should be a duplicate while updatedInfo should be a live object
        assertThat(r.get().setHostId(UUID.randomUUID())).isNotEqualTo(nodes.getLocal().get()); // mutation on duplicate should not have effect on live object
    }

    @Test
    public void updateLocalWithSomeChange() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        ForkJoinTask<LocalInfo> r = updateLocalInfo(hostId, false, false);
        await().untilAsserted(() -> assertThat(r).isDone());
        assertThat(r.get().getHostId()).isEqualTo(hostId);
        await().untilAsserted(() -> assertThat(barrier.getNumberWaiting()).isOne());
        assertThat(localInfoRef.get()).isNotNull().isNotSameAs(r); // r should be a duplicate while updatedInfo should be a live object
        assertThat(r.get().setHostId(UUID.randomUUID())).isNotEqualTo(nodes.getLocal().get()); // mutation on duplicate should not have effect on live object
        assertThat(nodes.getLocal().get().getHostId()).isEqualTo(hostId);
        barrier.reset();
    }

    @Test
    public void updateLocalWithSomeChangeBlocking() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        ForkJoinTask<LocalInfo> r = updateLocalInfo(hostId, true, false);
        await().untilAsserted(() -> assertThat(barrier.getNumberWaiting()).isOne());
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        assertThat(r).isNotDone();
        barrier.await();
        await().untilAsserted(() -> assertThat(r).isDone());
        assertThat(r.get().getHostId()).isEqualTo(hostId);
        assertThat(localInfoRef.get()).isNotNull().isNotSameAs(r); // r should be a duplicate while updatedInfo should be a live object
        assertThat(r.get().setHostId(UUID.randomUUID())).isNotEqualTo(nodes.getLocal().get()); // mutation on duplicate should not have effect on live object
        assertThat(nodes.getLocal().get().getHostId()).isEqualTo(hostId);
    }

    @Test
    public void updateLocalWithForce() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        nodes.getLocal().update(current -> current.setHostId(hostId), true, true);
        ForkJoinTask<LocalInfo> r = updateLocalInfo(hostId, false, true);
        await().untilAsserted(() -> assertThat(r).isDone());
        await().untilAsserted(() -> assertThat(barrier.getNumberWaiting()).isOne());
        barrier.await();
        assertThat(r.get().getHostId()).isEqualTo(hostId);
        assertThat(localInfoRef.get()).isNotNull().isNotSameAs(r); // r should be a duplicate while updatedInfo should be a live object
        assertThat(r.get().setHostId(UUID.randomUUID())).isNotEqualTo(nodes.getLocal().get()); // mutation on duplicate should not have effect on live object
        assertThat(nodes.getLocal().get().getHostId()).isEqualTo(hostId);
    }

    @Test
    public void updatePeerNoChanges() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        nodes.getPeers().update(addr1, current -> current.setHostId(hostId), true, true);

        ForkJoinTask<PeerInfo> r = updatePeerInfo(hostId, false, false);
        await().untilAsserted(() -> assertThat(r).isDone());
        assertThat(r.get().getHostId()).isEqualTo(hostId);
        assertThat(barrier.getNumberWaiting()).isZero(); // force == false and we did not change anything
        assertThat(peerInfoRef.get()).isNotNull().isNotSameAs(r); // r should be a duplicate while updatedInfo should be a live object
        assertThat(r.get().setHostId(UUID.randomUUID())).isNotEqualTo(nodes.getPeers().get(addr1)); // mutation on duplicate should not have effect on live object
    }

    @Test
    public void updatePeerWithSomeChange() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        ForkJoinTask<PeerInfo> r = updatePeerInfo(hostId, false, false);
        await().untilAsserted(() -> assertThat(r).isDone());
        assertThat(r.get().getHostId()).isEqualTo(hostId);
        await().untilAsserted(() -> assertThat(barrier.getNumberWaiting()).isOne());
        assertThat(peerInfoRef.get()).isNotNull().isNotSameAs(r); // r should be a duplicate while updatedInfo should be a live object
        assertThat(r.get().setHostId(UUID.randomUUID())).isNotEqualTo(nodes.getPeers().get(addr1)); // mutation on duplicate should not have effect on live object
        assertThat(nodes.getPeers().get(addr1).getHostId()).isEqualTo(hostId);
        barrier.reset();
    }

    @Test
    public void updatePeerWithSomeChangeBlocking() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        ForkJoinTask<PeerInfo> r = updatePeerInfo(hostId, true, false);
        await().untilAsserted(() -> assertThat(barrier.getNumberWaiting()).isOne());
        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        assertThat(r).isNotDone();
        barrier.await();
        await().untilAsserted(() -> assertThat(r).isDone());
        assertThat(r.get().getHostId()).isEqualTo(hostId);
        assertThat(peerInfoRef.get()).isNotNull().isNotSameAs(r); // r should be a duplicate while updatedInfo should be a live object
        assertThat(r.get().setHostId(UUID.randomUUID())).isNotEqualTo(nodes.getPeers().get(addr1)); // mutation on duplicate should not have effect on live object
        assertThat(nodes.getPeers().get(addr1).getHostId()).isEqualTo(hostId);
    }

    @Test
    public void updatePeerWithForce() throws Exception
    {
        UUID hostId = UUID.randomUUID();
        nodes.getPeers().update(addr1, current -> current.setHostId(hostId), true, true);
        ForkJoinTask<PeerInfo> r = updatePeerInfo(hostId, false, true);
        await().untilAsserted(() -> assertThat(r).isDone());
        await().untilAsserted(() -> assertThat(barrier.getNumberWaiting()).isOne());
        barrier.await();
        assertThat(r.get().getHostId()).isEqualTo(hostId);
        assertThat(peerInfoRef.get()).isNotNull().isNotSameAs(r); // r should be a duplicate while updatedInfo should be a live object
        assertThat(r.get().setHostId(UUID.randomUUID())).isNotEqualTo(nodes.getPeers().get()); // mutation on duplicate should not have effect on live object
        assertThat(nodes.getPeers().get(addr1).getHostId()).isEqualTo(hostId);
    }

    private ConditionFactory await()
    {
        return Awaitility.await().pollDelay(10, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS);
    }

    private ForkJoinTask<LocalInfo> updateLocalInfo(UUID hostId, boolean blocking, boolean force)
    {
        doAnswer(inv -> {
            barrier.await();
            return null;
        }).when(persistence).saveLocal(localInfoCaptor.capture());

        return ForkJoinPool.commonPool().submit(() -> nodes.getLocal().update(previous -> {
            localInfoRef.set(previous);
            previous.setHostId(hostId);
            return previous;
        }, blocking, force));
    }

    private ForkJoinTask<PeerInfo> updatePeerInfo(UUID hostId, boolean blocking, boolean force)
    {
        doAnswer(inv -> {
            barrier.await();
            return null;
        }).when(persistence).savePeer(peerInfoCaptor.capture());

        return ForkJoinPool.commonPool().submit(() -> nodes.getPeers().update(addr1, previous -> {
            peerInfoRef.set(previous);
            previous.setHostId(hostId);
            return previous;
        }, blocking, force));
    }
}