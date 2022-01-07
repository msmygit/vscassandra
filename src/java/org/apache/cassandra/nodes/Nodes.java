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

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.concurrent.LoadingMap;

/**
 * Provides access and updates the locally stored information about this and other nodes. The information is cached in
 * memory in a thread-safe way and additionally stored using the provided implementation of {@link INodesPersistence},
 * which is {@link NodesPersistence} by default and stores everything in system keyspace.
 */
public class Nodes
{
    private static final Logger logger = LoggerFactory.getLogger(Nodes.class);

    @VisibleForTesting
    private final ExecutorService updateExecutor;

    private final INodesPersistence nodesPersistence;

    private final Peers peers;
    private final Local local;

    private static class InstanceHolder
    {
        // put into subclass for lazy initialization
        private static final Nodes instance = new Nodes();
    }

    /**
     * Returns the singleton instance
     */
    public static Nodes getInstance()
    {
        return InstanceHolder.instance;
    }

    /**
     * Returns the singleton instance of {@link Peers}
     */
    public static Peers peers()
    {
        return getInstance().getPeers();
    }

    /**
     * Returns the singleton instance of {@link Local}
     */
    public static Local local()
    {
        return getInstance().getLocal();
    }

    /**
     * Returns information about the node with the given address - if the node address matches the local (broadcast)
     * address, the returned object is a {@link LocalInfo}. Otherwise, it is {@link PeerInfo} (or {@code null} if no
     * informatino is available).
     */
    public static NodeInfo<?> localOrPeerInfo(InetAddressAndPort endpoint)
    {
        return Objects.equals(endpoint, FBUtilities.getBroadcastAddressAndPort()) ? local().get() : peers().get(endpoint);
    }

    /**
     * @see #updateLocalOrPeer(InetAddressAndPort, UnaryOperator, boolean, boolean)
     */
    public static NodeInfo<?> updateLocalOrPeer(InetAddressAndPort endpoint, UnaryOperator<NodeInfo<?>> update)
    {
        return updateLocalOrPeer(endpoint, update, false);
    }

    /**
     * @see #updateLocalOrPeer(InetAddressAndPort, UnaryOperator, boolean, boolean)
     */
    public static NodeInfo<?> updateLocalOrPeer(InetAddressAndPort endpoint, UnaryOperator<NodeInfo<?>> update, boolean blocking)
    {
        return updateLocalOrPeer(endpoint, update, blocking, false);
    }

    /**
     * Updates either local or peer information in a thread-safe way, depeending on whether the provided address matches
     * the local (broadcast) address.
     *
     * @see Local#updateLocalOrPeer(InetAddressAndPort, UnaryOperator, boolean, boolean)
     * @see Peers#updateLocalOrPeer(InetAddressAndPort, UnaryOperator, boolean, boolean)
     */
    public static NodeInfo<?> updateLocalOrPeer(InetAddressAndPort endpoint, UnaryOperator<NodeInfo<?>> update, boolean blocking, boolean force)
    {
        if (Objects.equals(endpoint, FBUtilities.getBroadcastAddressAndPort()))
            return local().update(info -> (LocalInfo) update.apply(info), blocking, force);
        else
            return peers().update(endpoint, info -> (PeerInfo) update.apply(info), blocking, force);
    }

    /**
     * Checks whether the provided address is local or known peer address.
     */
    public static boolean isKnownEndpoint(InetAddressAndPort endpoint)
    {
        return localOrPeerInfo(endpoint) != null;
    }

    /**
     * Initializes singleton instance of {@link Nodes}. If it is not a Cassandra server process or
     * {@code cassandra.nodes.disablePersitingToSystemKeyspace} is set to {@code true}, the instance does not persist
     * stored information anywhere.
     */
    private Nodes()
    {
        this(!DatabaseDescriptor.isDaemonInitialized() || Boolean.getBoolean("cassandra.nodes.disablePersitingToSystemKeyspace")
             ? INodesPersistence.NO_NODES_PERSISTENCE
             : new NodesPersistence(),
             Executors.newSingleThreadExecutor(new NamedThreadFactory("nodes-info-persistence")));
    }

    @VisibleForTesting
    public Nodes(INodesPersistence nodesPersistence, ExecutorService updateExecutor)
    {
        this.updateExecutor = updateExecutor;
        this.nodesPersistence = nodesPersistence;
        this.local = new Local().load();
        this.peers = new Peers().load();
    }

    public Peers getPeers()
    {
        return peers;
    }

    public Local getLocal()
    {
        return local;
    }

    private Runnable wrapPersistenceTask(String name, Runnable task)
    {
        return () -> {
            try
            {
                task.run();
            }
            catch (RuntimeException ex)
            {
                logger.error("Unexpected exception - " + name, ex);
                throw ex;
            }
        };
    }

    public class Peers
    {
        private final LoadingMap<InetAddressAndPort, PeerInfo> internalMap = new LoadingMap<>();

        public PeerInfo update(InetAddressAndPort peer, UnaryOperator<PeerInfo> update)
        {
            return update(peer, update, false);
        }

        public PeerInfo update(InetAddressAndPort peer, UnaryOperator<PeerInfo> update, boolean blocking)
        {
            return update(peer, update, blocking, false);
        }

        /**
         * Updates peer information in a thread-safe way.
         *
         * @param peer     address of a peer to be updated
         * @param update   update function, which receives a copy of the current {@link PeerInfo} and is expected to
         *                 return the updated {@link PeerInfo}; the function may apply updates directly on the received
         *                 copy and return it
         * @param blocking if set, the method will block until the changes are persisted
         * @param force    the update will be persisted even if no changes are made
         * @return the updated object
         */
        public PeerInfo update(InetAddressAndPort peer, UnaryOperator<PeerInfo> update, boolean blocking, boolean force)
        {
            return internalMap.compute(peer, (key, existingPeerInfo) -> {
                PeerInfo updated = existingPeerInfo == null
                                   ? update.apply(new PeerInfo().setPeerAddressAndPort(peer))
                                   : update.apply(existingPeerInfo.duplicate()); // since we operate on mutable objects, we don't want to let the update function to operate on the live object

                if (updated.getPeerAddressAndPort() == null)
                    updated.setPeerAddressAndPort(peer);
                else
                    Preconditions.checkArgument(Objects.equals(updated.getPeerAddressAndPort(), peer));

                save(existingPeerInfo, updated, blocking, force);
                return updated;
            }).duplicate(); // we don't want to return the live object thus we duplicate
        }

        public PeerInfo remove(InetAddressAndPort peer, boolean blocking)
        {
            AtomicReference<PeerInfo> removed = new AtomicReference<>();
            internalMap.computeIfPresent(peer, (key, existingPeerInfo) -> {
                delete(peer, blocking);
                removed.set(existingPeerInfo);
                return null;
            });
            return removed.get();
        }

        /**
         * Returns a peer information for a given address if the peer is known. Otherwise returns {@code null}.
         */
        public PeerInfo get(InetAddressAndPort peer)
        {
            PeerInfo info = internalMap.get(peer);
            return info != null ? info.duplicate() : null;
        }

        /**
         * Returns a stream of all known peers.
         */
        public Stream<PeerInfo> get()
        {
            return internalMap.valuesStream().map(PeerInfo::duplicate);
        }

        private void save(PeerInfo previousInfo, PeerInfo newInfo, boolean blocking, boolean force)
        {
            if (!force && Objects.equals(previousInfo, newInfo))
                return;

            Future<?> f = updateExecutor.submit(wrapPersistenceTask("saving peer information: " + newInfo, () -> {
                nodesPersistence.savePeer(newInfo);
                if (blocking)
                    nodesPersistence.syncPeers();
            }));
            if (blocking)
                FBUtilities.waitOnFuture(f);
        }

        private Peers load()
        {
            nodesPersistence.loadPeers().forEach(info -> internalMap.compute(info.getPeerAddressAndPort(), (key, existingPeerInfo) -> info));
            return this;
        }

        private void delete(InetAddressAndPort peer, boolean blocking)
        {
            Future<?> f = updateExecutor.submit(wrapPersistenceTask("deleting peer information: " + peer, () -> {
                nodesPersistence.deletePeer(peer);
                if (blocking)
                    nodesPersistence.syncPeers();
            }));

            if (blocking)
                FBUtilities.waitOnFuture(f);
        }
    }

    public class Local
    {
        private final LoadingMap<InetAddressAndPort, LocalInfo> internalMap = new LoadingMap<>(1);
        private final InetAddressAndPort localInfoKey = FBUtilities.getBroadcastAddressAndPort();

        /**
         * @see #update(UnaryOperator, boolean, boolean)
         */
        public LocalInfo update(UnaryOperator<LocalInfo> update)
        {
            return update(update, false);
        }

        /**
         * @see #update(UnaryOperator, boolean, boolean)
         */
        public LocalInfo update(UnaryOperator<LocalInfo> update, boolean blocking)
        {
            return update(update, blocking, false);
        }

        /**
         * Updates local node information in a thread-safe way.
         *
         * @param update   update function, which receives a copy of the current {@link LocalInfo} and is expected to
         *                 return the updated {@link LocalInfo}; the function may apply updates directly on the received
         *                 copy and return it
         * @param blocking if set, the method will block until the changes are persisted
         * @param force    the update will be persisted even if no changes are made
         * @return a copy of updated object
         */
        public LocalInfo update(UnaryOperator<LocalInfo> update, boolean blocking, boolean force)
        {
            return internalMap.compute(localInfoKey, (key, existingLocalInfo) -> {
                LocalInfo updated = existingLocalInfo == null
                                    ? update.apply(new LocalInfo())
                                    : update.apply(existingLocalInfo.duplicate()); // since we operate on mutable objects, we don't want to let the update function to operate on the live object
                save(existingLocalInfo, updated, blocking, force);
                return updated;
            }).duplicate(); // we don't want to return the live object thus we duplicate
        }

        /**
         * Returns information about the local node (if present).
         */
        public LocalInfo get()
        {
            LocalInfo info = internalMap.get(localInfoKey);
            return info != null ? info.duplicate() : null;
        }

        private void save(LocalInfo previousInfo, LocalInfo newInfo, boolean blocking, boolean force)
        {
            if (!force && Objects.equals(previousInfo, newInfo))
                return;

            Future<?> f = updateExecutor.submit(wrapPersistenceTask("saving local node information: " + newInfo, () -> {
                nodesPersistence.saveLocal(newInfo);
                if (blocking)
                    nodesPersistence.syncLocal();
            }));

            if (blocking)
                FBUtilities.waitOnFuture(f);
        }

        private Local load()
        {
            internalMap.compute(localInfoKey, (key, existingLocalInfo) -> {
                LocalInfo info = nodesPersistence.loadLocal();
                return info != null ? info : new LocalInfo();
            });
            return this;
        }
    }
}
