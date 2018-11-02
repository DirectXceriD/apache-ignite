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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheContinuousQueryOperationP2PTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES = 5;

    /** */
    private static final int UPDATES = 100;

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setCommunicationSpi(communicationSpi());

        cfg.setClientMode(client);
        cfg.setPeerClassLoadingEnabled(true);

        return cfg;
    }

    /**
     * @return Communication SPI to use during a test.
     */
    protected CommunicationSpi communicationSpi() {
        return new TcpCommunicationSpi();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(NODES - 1);

        client = true;

        startGrid(NODES - 1);

        client = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC
        );

        testContinuousQuery(ccfg, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomic() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            ATOMIC
        );

        testContinuousQuery(ccfg, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC
        );

        testContinuousQuery(ccfg, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicReplicatedClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            ATOMIC
        );

        testContinuousQuery(ccfg, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTx() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL
        );

        testContinuousQuery(ccfg, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL
        );

        testContinuousQuery(ccfg, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL
        );

        testContinuousQuery(ccfg, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxReplicatedClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL
        );

        testContinuousQuery(ccfg, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccTx() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL_SNAPSHOT
        );

        testContinuousQuery(ccfg, false);
    }
    /**
     * @throws Exception If failed.
     */
    public void testMvccTxClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(PARTITIONED,
            1,
            TRANSACTIONAL_SNAPSHOT
        );

        testContinuousQuery(ccfg, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccTxReplicated() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL_SNAPSHOT
        );

        testContinuousQuery(ccfg, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMvccTxReplicatedClient() throws Exception {
        CacheConfiguration<Object, Object> ccfg = cacheConfiguration(REPLICATED,
            0,
            TRANSACTIONAL_SNAPSHOT
        );

        testContinuousQuery(ccfg, true);
    }

    /**
     * @param ccfg Cache configuration.
     * @param isClient Client.
     * @throws Exception If failed.
     */
    protected void testContinuousQuery(CacheConfiguration<Object, Object> ccfg, boolean isClient)
        throws Exception {

        ignite(0).createCache(ccfg);

        final Class<Factory<CacheEntryEventFilter>> evtFilterFactoryCls =
            (Class<Factory<CacheEntryEventFilter>>)getExternalClassLoader().
                loadClass("org.apache.ignite.tests.p2p.CacheDeploymentEntryEventFilterFactory");

        testContinuousQuery(ccfg, isClient, false, evtFilterFactoryCls);
        testContinuousQuery(ccfg, isClient, true, evtFilterFactoryCls);
    }

    /**
     * @param ccfg Cache configuration.
     * @param isClient Client.
     * @param joinNode If a node should be added to topology after a query is started.
     * @param evtFilterFactoryCls Remote filter factory class.
     * @throws Exception If failed.
     */
    private void testContinuousQuery(CacheConfiguration<Object, Object> ccfg,
        boolean isClient, boolean joinNode,
        Class<Factory<CacheEntryEventFilter>> evtFilterFactoryCls) throws Exception {

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        final CountDownLatch latch = new CountDownLatch(UPDATES);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        AtomicReference<String> err = new AtomicReference<>();

        TestLocalListener locLsnr = new TestLocalListener() {
            @Override protected void onEvent(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts) {
                    latch.countDown();

                    log.info("Received event: " + evt);

                    int key = evt.getKey();

                    if (key % 2 == 0)
                        err.set("Event received on entry, that doesn't pass a filter: " + key);
                }
            }
        };

        qry.setLocalListener(locLsnr);

        qry.setRemoteFilterFactory(
            (Factory<? extends CacheEntryEventFilter<Integer, Integer>>)(Object)evtFilterFactoryCls.newInstance());

        MutableCacheEntryListenerConfiguration<Integer, Integer> lsnrCfg =
            new MutableCacheEntryListenerConfiguration<>(
                new FactoryBuilder.SingletonFactory<>(locLsnr),
                (Factory<? extends CacheEntryEventFilter<? super Integer, ? super Integer>>)
                    (Object)evtFilterFactoryCls.newInstance(),
                true,
                true
            );

        IgniteCache<Integer, Integer> cache;

        cache = isClient
            ? grid(NODES - 1).cache(ccfg.getName())
            : grid(rnd.nextInt(NODES - 1)).cache(ccfg.getName());

        try (QueryCursor<?> cur = cache.query(qry)) {
            cache.registerCacheEntryListener(lsnrCfg);

            if (joinNode) {
                startGrid(NODES);
                awaitPartitionMapExchange();
            }

            for (int i = 0; i < UPDATES; i++)
                cache.put(i, i);

            assertTrue("Failed to wait for local listener invocations: " + latch.getCount(),
                latch.await(3, TimeUnit.SECONDS));

            assertNull(err.get(), err.get());
        }
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     *
     */
    private abstract static class TestLocalListener implements CacheEntryUpdatedListener<Integer, Integer>,
        CacheEntryCreatedListener<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void onCreated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts)
            throws CacheEntryListenerException {
            onEvent(evts);
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts)
            throws CacheEntryListenerException {
            onEvent(evts);
        }

        /**
         * @param evts Events.
         */
        protected abstract void onEvent(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts);
    }
}
