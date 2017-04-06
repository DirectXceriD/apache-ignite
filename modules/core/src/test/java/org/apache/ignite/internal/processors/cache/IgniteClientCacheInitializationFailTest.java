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

package org.apache.ignite.internal.processors.cache;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.GridQueryCancel;
import org.apache.ignite.internal.processors.query.GridQueryFieldsResult;
import org.apache.ignite.internal.processors.query.GridQueryIndexing;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.processors.query.GridQueryTypeDescriptor;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.indexing.IndexingQueryFilter;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Test checks whether cache initialization error on client side
 * doesn't causes hangs and doesn't impact other caches.
 */
public class IgniteClientCacheInitializationFailTest extends GridCommonAbstractTest {
    /** Failed cache name. */
    public static final String CACHE_NAME = "cache";

    /** Near flag. */
    private boolean near;

    /** Atomicity mode. */
    private CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (gridName.contains("server")) {
            CacheConfiguration<Integer, String> ccfg = new CacheConfiguration<>();

            ccfg.setIndexedTypes(Integer.class, String.class);

            ccfg.setName(CACHE_NAME);
            ccfg.setAtomicityMode(atomicityMode);

            if (near)
                ccfg.setNearConfiguration(new NearCacheConfiguration<Integer, String>());

            cfg.setCacheConfiguration(ccfg);
        }
        else {
            GridQueryProcessor.idxCls = FailedIndexing.class;

            cfg.setClientMode(true);
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicCacheInitialization() throws Exception {
        atomicityMode = CacheAtomicityMode.ATOMIC;
        near = false;

        checkCacheInitialization();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionalCacheInitialization() throws Exception {
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
        near = false;

        checkCacheInitialization();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicNearCacheInitialization() throws Exception {
        atomicityMode = CacheAtomicityMode.ATOMIC;
        near = true;

        checkCacheInitialization();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionalNearCacheInitialization() throws Exception {
        atomicityMode = CacheAtomicityMode.TRANSACTIONAL;
        near = true;

        checkCacheInitialization();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkCacheInitialization() throws Exception {
        startGrid("server");

        final Ignite client = startGrid("client");

        checkFailedCache(client);

        checkFineCache(client, CACHE_NAME + 1);

        assertNull(client.cache(CACHE_NAME));
        assertNull(client.getOrCreateCache(CACHE_NAME));

        checkFineCache(client, CACHE_NAME + 2);
    }

    /**
     * @param client Client.
     * @param cacheName Cache name.
     */
    private void checkFineCache(Ignite client, String cacheName) {
        IgniteCache<Integer, String> cache = client.getOrCreateCache(cacheName);

        cache.put(1, "1");

        assertEquals("1", cache.get(1));
    }

    /**
     * @param client Client.
     */
    @SuppressWarnings("ThrowableNotThrown")
    private void checkFailedCache(final Ignite client) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgniteCache<Integer, String> cache = client.cache(CACHE_NAME);

                cache.put(1, "1");

                assertEquals("1", cache.get(1));

                return null;
            }
        }, CacheException.class, null);
    }

    /**
     * To fail on cache start.
     */
    private static class FailedIndexing implements GridQueryIndexing {
        /** {@inheritDoc} */
        @Override public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public void stop() throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public QueryCursor<List<?>> queryTwoStep(GridCacheContext<?, ?> cctx, SqlFieldsQuery qry,
            GridQueryCancel cancel) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> QueryCursor<Cache.Entry<K, V>> queryTwoStep(GridCacheContext<?, ?> cctx,
            SqlQuery qry) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public GridQueryFieldsResult queryLocalSqlFields(@Nullable String spaceName, String qry,
            Collection<Object> params, IndexingQueryFilter filter, boolean enforceJoinOrder, int timeout,
            GridQueryCancel cancel) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalSql(@Nullable String spaceName,
            String qry, String alias, Collection<Object> params, GridQueryTypeDescriptor type,
            IndexingQueryFilter filter) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public <K, V> GridCloseableIterator<IgniteBiTuple<K, V>> queryLocalText(@Nullable String spaceName,
            String qry, GridQueryTypeDescriptor type, IndexingQueryFilter filter) throws IgniteCheckedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void registerCache(GridCacheContext<?, ?> cctx,
            CacheConfiguration<?, ?> ccfg) throws IgniteCheckedException {
            if (CACHE_NAME.equals(cctx.name()) && cctx.kernalContext().clientNode())
                throw new IgniteCheckedException("Test query exception");
        }

        /** {@inheritDoc} */
        @Override public void unregisterCache(CacheConfiguration<?, ?> ccfg) throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public boolean registerType(@Nullable String spaceName,
            GridQueryTypeDescriptor desc) throws IgniteCheckedException {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void unregisterType(@Nullable String spaceName,
            GridQueryTypeDescriptor type) throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public void store(@Nullable String spaceName, GridQueryTypeDescriptor type, CacheObject key,
            CacheObject val, byte[] ver, long expirationTime) throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public void remove(@Nullable String spaceName, CacheObject key,
            CacheObject val) throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public void onSwap(@Nullable String spaceName, CacheObject key) throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public void onUnswap(@Nullable String spaceName, CacheObject key,
            CacheObject val) throws IgniteCheckedException {

        }

        /** {@inheritDoc} */
        @Override public void rebuildIndexes(@Nullable String spaceName, GridQueryTypeDescriptor type) {

        }

        /** {@inheritDoc} */
        @Override public IndexingQueryFilter backupFilter(AffinityTopologyVersion topVer, int[] parts) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void onDisconnected(IgniteFuture<?> reconnectFut) {

        }

        /** {@inheritDoc} */
        @Override public PreparedStatement prepareNativeStatement(String schema, String sql) throws SQLException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public Collection<GridRunningQueryInfo> runningQueries(long duration) {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void cancelQueries(Collection<Long> queries) {

        }

        /** {@inheritDoc} */
        @Override public void cancelAllQueries() {

        }
    }
}
