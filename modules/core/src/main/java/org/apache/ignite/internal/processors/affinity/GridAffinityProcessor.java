/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.affinity;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridClosureCallMode.*;
import static org.apache.ignite.internal.processors.affinity.GridAffinityUtils.*;

/**
 * Data affinity processor.
 */
public class GridAffinityProcessor extends GridProcessorAdapter {
    /** Affinity map cleanup delay (ms). */
    private static final long AFFINITY_MAP_CLEAN_UP_DELAY = 3000;

    /** Retries to get affinity in case of error. */
    private static final int ERROR_RETRIES = 3;

    /** Time to wait between errors (in milliseconds). */
    private static final long ERROR_WAIT = 500;

    /** Null cache name. */
    private static final String NULL_NAME = U.id8(UUID.randomUUID());

    /** Affinity map. */
    private final ConcurrentMap<AffinityAssignmentKey, IgniteInternalFuture<AffinityInfo>> affMap = new ConcurrentHashMap8<>();

    /** Listener. */
    private final GridLocalEventListener lsnr = new GridLocalEventListener() {
        @Override public void onEvent(Event evt) {
            int evtType = evt.type();

            assert evtType == EVT_NODE_FAILED || evtType == EVT_NODE_LEFT || evtType == EVT_NODE_JOINED;

            if (affMap.isEmpty())
                return; // Skip empty affinity map.

            final DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

            // Clean up affinity functions if such cache no more exists.
            if (evtType == EVT_NODE_FAILED || evtType == EVT_NODE_LEFT) {
                final Collection<String> caches = new HashSet<>();

                for (ClusterNode clusterNode : ((DiscoveryEvent)evt).topologyNodes())
                    caches.addAll(U.cacheNames(clusterNode));

                final Collection<AffinityAssignmentKey> rmv = new HashSet<>();

                for (AffinityAssignmentKey key : affMap.keySet()) {
                    if (!caches.contains(key.cacheName) || key.topVer < discoEvt.topologyVersion() - 10)
                        rmv.add(key);
                }

                if (!rmv.isEmpty()) {
                    ctx.timeout().addTimeoutObject(
                        new GridTimeoutObjectAdapter(
                            IgniteUuid.fromUuid(ctx.localNodeId()),
                            AFFINITY_MAP_CLEAN_UP_DELAY) {
                                @Override public void onTimeout() {
                                    affMap.keySet().removeAll(rmv);
                                }
                            });
                }
            }
        }
    };

    /**
     * @param ctx Context.
     */
    public GridAffinityProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.event().addLocalEventListener(lsnr, EVT_NODE_FAILED, EVT_NODE_LEFT, EVT_NODE_JOINED);
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        ctx.event().removeLocalEventListener(lsnr);
    }

    /**
     * Maps keys to nodes for given cache.
     *
     * @param cacheName Cache name.
     * @param keys Keys to map.
     * @return Map of nodes to keys.
     * @throws IgniteCheckedException If failed.
     */
    public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable String cacheName,
        @Nullable Collection<? extends K> keys) throws IgniteCheckedException {
        return keysToNodes(cacheName, keys);
    }

    /**
     * Maps keys to nodes on default cache.
     *
     * @param keys Keys to map.
     * @return Map of nodes to keys.
     * @throws IgniteCheckedException If failed.
     */
    public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable Collection<? extends K> keys)
        throws IgniteCheckedException {
        return keysToNodes(null, keys);
    }

    /**
     * Maps single key to a node.
     *
     * @param cacheName Cache name.
     * @param key Key to map.
     * @return Picked node.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key) throws IgniteCheckedException {
        Map<ClusterNode, Collection<K>> map = keysToNodes(cacheName, F.asList(key));

        return map != null ? F.first(map.keySet()) : null;
    }

    /**
     * Maps single key to a node.
     *
     * @param cacheName Cache name.
     * @param key Key to map.
     * @return Picked node.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key, long topVer) throws IgniteCheckedException {
        Map<ClusterNode, Collection<K>> map = keysToNodes(cacheName, F.asList(key), topVer);

        return map != null ? F.first(map.keySet()) : null;
    }

    /**
     * Map single key to primary and backup nodes.
     *
     * @param cacheName Cache name.
     * @param key Key to map.
     * @return Affinity nodes, primary first.
     * @throws IgniteCheckedException If failed.
     */
    public <K> List<ClusterNode> mapKeyToPrimaryAndBackups(@Nullable String cacheName, K key) throws IgniteCheckedException {
        A.notNull(key, "key");

        ClusterNode loc = ctx.discovery().localNode();

        if (U.hasCache(loc, cacheName) && ctx.cache().cache(cacheName).configuration().getCacheMode() == LOCAL)
            return Collections.singletonList(loc);

        long topVer = ctx.discovery().topologyVersion();

        AffinityInfo affInfo = affinityCache(cacheName, topVer);

        return primaryAndBackups(affInfo, key);
    }

    /**
     * Maps single key to a node on default cache.
     *
     * @param key Key to map.
     * @return Picked node.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <K> ClusterNode mapKeyToNode(K key) throws IgniteCheckedException {
        return mapKeyToNode(null, key);
    }

    /**
     * Gets affinity key for cache key.
     *
     * @param cacheName Cache name.
     * @param key Cache key.
     * @return Affinity key.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("unchecked")
    @Nullable public Object affinityKey(@Nullable String cacheName, @Nullable Object key) throws IgniteCheckedException {
        if (key == null)
            return null;

        AffinityInfo affInfo = affinityCache(cacheName, ctx.discovery().topologyVersion());

        if (affInfo == null || affInfo.mapper == null)
            return null;

        if (key instanceof CacheObject)
            key = ((CacheObject)key).value(affInfo.cacheObjCtx, false);

        return affInfo.mapper.affinityKey(key);
    }

    /**
     * @param cacheName Cache name.
     * @return Cache affinity.
     */
    public <K> CacheAffinityProxy<K> affinityProxy(String cacheName) {
        return new CacheAffinityProxy<>(cacheName);
    }

    /**
     * @param cacheName Cache name.
     * @return Non-null cache name.
     */
    private String maskNull(@Nullable String cacheName) {
        return cacheName == null ? NULL_NAME : cacheName;
    }

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @return Affinity map.
     * @throws IgniteCheckedException If failed.
     */
    private <K> Map<ClusterNode, Collection<K>> keysToNodes(@Nullable final String cacheName,
        Collection<? extends K> keys) throws IgniteCheckedException {
        return keysToNodes(cacheName, keys, ctx.discovery().topologyVersion());
    }

    /**
     * @param cacheName Cache name.
     * @param keys Keys.
     * @param topVer Topology version.
     * @return Affinity map.
     * @throws IgniteCheckedException If failed.
     */
    private <K> Map<ClusterNode, Collection<K>> keysToNodes(@Nullable final String cacheName,
        Collection<? extends K> keys, long topVer) throws IgniteCheckedException {
        if (F.isEmpty(keys))
            return Collections.emptyMap();

        ClusterNode loc = ctx.discovery().localNode();

        if (U.hasCache(loc, cacheName) && ctx.cache().cache(cacheName).configuration().getCacheMode() == LOCAL)
            return F.asMap(loc, (Collection<K>)keys);

        AffinityInfo affInfo = affinityCache(cacheName, topVer);

        return affInfo != null ? affinityMap(affInfo, keys) : Collections.<ClusterNode, Collection<K>>emptyMap();
    }

    /**
     * @param cacheName Cache name.
     * @param topVer Topology version.
     * @return Affinity cache.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("ErrorNotRethrown")
    private AffinityInfo affinityCache(@Nullable final String cacheName, long topVer) throws IgniteCheckedException {
        AffinityAssignmentKey key = new AffinityAssignmentKey(cacheName, topVer);

        IgniteInternalFuture<AffinityInfo> fut = affMap.get(key);

        if (fut != null)
            return fut.get();

        ClusterNode loc = ctx.discovery().localNode();

        // Check local node.
        if (U.hasCache(loc, cacheName)) {
            GridCacheContext<Object,Object> cctx = ctx.cache().internalCache(cacheName).context();

            AffinityInfo info = new AffinityInfo(
                cctx.config().getAffinity(),
                cctx.config().getAffinityMapper(),
                new GridAffinityAssignment(topVer, cctx.affinity().assignments(topVer)),
                cctx.cacheObjectContext());

            IgniteInternalFuture<AffinityInfo> old = affMap.putIfAbsent(key, new GridFinishedFuture<>(info));

            if (old != null)
                info = old.get();

            return info;
        }

        Collection<ClusterNode> cacheNodes = F.view(
            ctx.discovery().remoteNodes(),
            new P1<ClusterNode>() {
                @Override public boolean apply(ClusterNode n) {
                    return U.hasCache(n, cacheName);
                }
            });

        if (F.isEmpty(cacheNodes))
            return null;

        GridFutureAdapter<AffinityInfo> fut0 = new GridFutureAdapter<>();

        IgniteInternalFuture<AffinityInfo> old = affMap.putIfAbsent(key, fut0);

        if (old != null)
            return old.get();

        int max = ERROR_RETRIES;
        int cnt = 0;

        Iterator<ClusterNode> it = cacheNodes.iterator();

        // We are here because affinity has not been fetched yet, or cache mode is LOCAL.
        while (true) {
            cnt++;

            if (!it.hasNext())
                it = cacheNodes.iterator();

            // Double check since we deal with dynamic view.
            if (!it.hasNext())
                // Exception will be caught in this method.
                throw new IgniteCheckedException("No cache nodes in topology for cache name: " + cacheName);

            ClusterNode n = it.next();

            CacheMode mode = U.cacheMode(n, cacheName);

            assert mode != null;

            // Map all keys to a single node, if the cache mode is LOCAL.
            if (mode == LOCAL) {
                fut0.onDone(new IgniteCheckedException("Failed to map keys for LOCAL cache."));

                // Will throw exception.
                fut0.get();
            }

            try {
                // Resolve cache context for remote node.
                // Set affinity function before counting down on latch.
                fut0.onDone(affinityInfoFromNode(cacheName, topVer, n));

                break;
            }
            catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Failed to get affinity from node (will retry) [cache=" + cacheName +
                        ", node=" + U.toShortString(n) + ", msg=" + e.getMessage() + ']');

                if (cnt < max) {
                    U.sleep(ERROR_WAIT);

                    continue;
                }

                affMap.remove(maskNull(cacheName), fut0);

                fut0.onDone(new IgniteCheckedException("Failed to get affinity mapping from node: " + n, e));

                break;
            }
            catch (RuntimeException | Error e) {
                fut0.onDone(new IgniteCheckedException("Failed to get affinity mapping from node: " + n, e));

                break;
            }
        }

        return fut0.get();
    }

    /**
     * Requests {@link CacheAffinityFunction} and
     * {@link CacheAffinityKeyMapper} from remote node.
     *
     * @param cacheName Name of cache on which affinity is requested.
     * @param topVer Topology version.
     * @param n Node from which affinity is requested.
     * @return Affinity cached function.
     * @throws IgniteCheckedException If either local or remote node cannot get deployment for affinity objects.
     */
    private AffinityInfo affinityInfoFromNode(@Nullable String cacheName, long topVer, ClusterNode n)
        throws IgniteCheckedException {
        GridTuple3<GridAffinityMessage, GridAffinityMessage, GridAffinityAssignment> t = ctx.closure()
            .callAsyncNoFailover(BALANCE, affinityJob(cacheName, topVer), F.asList(n), true/*system pool*/).get();

        CacheAffinityFunction f = (CacheAffinityFunction)unmarshall(ctx, n.id(), t.get1());
        CacheAffinityKeyMapper m = (CacheAffinityKeyMapper)unmarshall(ctx, n.id(), t.get2());

        assert m != null;

        // Bring to initial state.
        f.reset();
        m.reset();

        return new AffinityInfo(f, m, t.get3(), ctx.cacheObjects().contextForCache(n, cacheName));
    }

    /**
     * @param aff Affinity function.
     * @param keys Keys.
     * @return Affinity map.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked"})
    private <K> Map<ClusterNode, Collection<K>> affinityMap(AffinityInfo aff, Collection<? extends K> keys)
        throws IgniteCheckedException {
        assert aff != null;
        assert !F.isEmpty(keys);

        try {
            if (keys.size() == 1)
                return Collections.singletonMap(primary(aff, F.first(keys)), (Collection<K>)keys);

            Map<ClusterNode, Collection<K>> map = new GridLeanMap<>();

            for (K k : keys) {
                ClusterNode n = primary(aff, k);

                Collection<K> mapped = map.get(n);

                if (mapped == null)
                    map.put(n, mapped = new LinkedList<>());

                mapped.add(k);
            }

            return map;
        }
        catch (IgniteException e) {
            // Affinity calculation may lead to IgniteException if no cache nodes found for pair cacheName+topVer.
            throw new IgniteCheckedException("Failed to get affinity map for keys: " + keys, e);
        }
    }

    /**
     * Get primary node for cached key.
     *
     * @param aff Affinity function.
     * @param key Key to check.
     * @return Primary node for given key.
     * @throws IgniteCheckedException In case of error.
     */
    private <K> ClusterNode primary(AffinityInfo aff, K key) throws IgniteCheckedException {
        if (key instanceof CacheObject)
            key = ((CacheObject)key).value(aff.cacheObjCtx, false);

        int part = aff.affFunc.partition(aff.mapper.affinityKey(key));

        Collection<ClusterNode> nodes = aff.assignment.get(part);

        if (F.isEmpty(nodes))
            throw new IgniteCheckedException("Failed to get affinity nodes [aff=" + aff + ", key=" + key + ']');

        return nodes.iterator().next();
    }

    /**
     * @param aff Affinity function.
     * @param key Key to check.
     * @return Primary and backup nodes.
     */
    private <K> List<ClusterNode> primaryAndBackups(AffinityInfo aff, K key) {
        if (key instanceof CacheObject)
            key = ((CacheObject) key).value(aff.cacheObjCtx, false);

        int part = aff.affFunc.partition(aff.mapper.affinityKey(key));

        return aff.assignment.get(part);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> Affinity processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   affMapSize: " + affMap.size());
    }

    /**
     *
     */
    private static class AffinityInfo {
        /** Affinity function. */
        private CacheAffinityFunction affFunc;

        /** Mapper */
        private CacheAffinityKeyMapper mapper;

        /** Assignment. */
        private GridAffinityAssignment assignment;

        /** */
        private CacheObjectContext cacheObjCtx;

        /**
         * @param affFunc Affinity function.
         * @param mapper Affinity key mapper.
         * @param assignment Partition assignment.
         * @param cacheObjCtx Cache objects context.
         */
        private AffinityInfo(CacheAffinityFunction affFunc,
            CacheAffinityKeyMapper mapper,
            GridAffinityAssignment assignment,
            CacheObjectContext cacheObjCtx) {
            this.affFunc = affFunc;
            this.mapper = mapper;
            this.assignment = assignment;
            this.cacheObjCtx = cacheObjCtx;
        }

        /**
         * @return Cache affinity function.
         */
        private CacheAffinityFunction affinityFunction() {
            return affFunc;
        }

        /**
         * @return Affinity assignment.
         */
        private GridAffinityAssignment assignment() {
            return assignment;
        }

        private CacheAffinityKeyMapper keyMapper() {
            return mapper;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AffinityInfo.class, this);
        }
    }

    /**
     *
     */
    private static class AffinityAssignmentKey {
        /** */
        private String cacheName;

        /** */
        private long topVer;

        /**
         * @param cacheName Cache name.
         * @param topVer Topology version.
         */
        private AffinityAssignmentKey(String cacheName, long topVer) {
            this.cacheName = cacheName;
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof AffinityAssignmentKey))
                return false;

            AffinityAssignmentKey that = (AffinityAssignmentKey)o;

            return topVer == that.topVer && F.eq(cacheName, that.cacheName);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = cacheName != null ? cacheName.hashCode() : 0;

            res = 31 * res + (int)(topVer ^ (topVer >>> 32));

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(AffinityAssignmentKey.class, this);
        }
    }

    /**
     * Grid cache affinity.
     */
    private class CacheAffinityProxy<K> implements CacheAffinity<K> {
        /** Cache name. */
        private final String cacheName;

        /**
         * @param cacheName Cache name.
         */
        public CacheAffinityProxy(String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            ctx.gateway().readLock();

            try {
                return cache().affinityFunction().partitions();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int partition(K key) {
            ctx.gateway().readLock();

            try {
                return cache().affinityFunction().partition(key);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isPrimary(ClusterNode n, K key) {
            ctx.gateway().readLock();

            try {
                return cache().assignment().primaryPartitions(n.id()).contains(partition(key));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isBackup(ClusterNode n, K key) {
            ctx.gateway().readLock();

            try {
                return cache().assignment().backupPartitions(n.id()).contains(partition(key));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean isPrimaryOrBackup(ClusterNode n, K key) {
            ctx.gateway().readLock();

            try {
                return isPrimary(n, key) || isBackup(n, key);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int[] primaryPartitions(ClusterNode n) {
            ctx.gateway().readLock();

            try {
                Set<Integer> parts = cache().assignment().primaryPartitions(n.id());

                return U.toIntArray(parts);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int[] backupPartitions(ClusterNode n) {
            ctx.gateway().readLock();

            try {
                Set<Integer> parts = cache().assignment().backupPartitions(n.id());

                return U.toIntArray(parts);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public int[] allPartitions(ClusterNode n) {
            ctx.gateway().readLock();

            try {
                GridAffinityAssignment assignment = cache().assignment();

                int[] primary = U.toIntArray(assignment.primaryPartitions(n.id()));
                int[] backup = U.toIntArray(assignment.backupPartitions(n.id()));

                return U.addAll(primary, backup);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Object affinityKey(K key) {
            ctx.gateway().readLock();

            try {
                if (key instanceof CacheObject)
                    key = ((CacheObject)key).value(cache().cacheObjCtx, false);

                return cache().keyMapper().affinityKey(key);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable Collection<? extends K> keys) {
            ctx.gateway().readLock();

            try {
                return GridAffinityProcessor.this.mapKeysToNodes(cacheName, keys);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Nullable @Override public ClusterNode mapKeyToNode(K key) {
            ctx.gateway().readLock();

            try {
                return GridAffinityProcessor.this.mapKeyToNode(cacheName, key);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> mapKeyToPrimaryAndBackups(K key) {
            ctx.gateway().readLock();

            try {
                return cache().assignment().get(partition(key));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public ClusterNode mapPartitionToNode(int part) {
            ctx.gateway().readLock();

            try {
                return F.first(cache().assignment().get(part));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Map<Integer, ClusterNode> mapPartitionsToNodes(Collection<Integer> parts) {
            ctx.gateway().readLock();

            try {
                Map<Integer, ClusterNode> map = new HashMap<>();

                if (!F.isEmpty(parts)) {
                    for (int p : parts)
                        map.put(p, mapPartitionToNode(p));
                }

                return map;
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> mapPartitionToPrimaryAndBackups(int part) {
            ctx.gateway().readLock();

            try {
                return cache().assignment().get(part);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
            finally {
                ctx.gateway().readUnlock();
            }
        }

        /**
         * @return Affinity info for current topology version.
         */
        private AffinityInfo cache() throws IgniteCheckedException {
            return affinityCache(cacheName, topologyVersion());
        }

        /**
         * @return Topology version.
         */
        private long topologyVersion() {
            return ctx.discovery().topologyVersion();
        }
    }
}
