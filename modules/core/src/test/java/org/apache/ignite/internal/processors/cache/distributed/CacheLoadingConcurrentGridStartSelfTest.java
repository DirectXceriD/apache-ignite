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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests for cache data loading during simultaneous grids start.
 */
public class CacheLoadingConcurrentGridStartSelfTest extends GridCommonAbstractTest {
    /** Grids count */
    private static int GRIDS_CNT = 5;

    /** Keys count */
    private static int KEYS_CNT = 1_000_000;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);

        ccfg.setBackups(1);

        ccfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory(new TestCacheStoreAdapter()));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception if failed
     */
    public void testLoadCacheWithDataStreamer() throws Exception {
        IgniteInClosure<Ignite> f = new IgniteInClosure<Ignite>() {
            @Override public void apply(Ignite grid) {
                try (IgniteDataStreamer<Integer, String> dataStreamer = grid.dataStreamer(null)) {
                    for (int i = 0; i < KEYS_CNT; i++)
                        dataStreamer.addData(i, Integer.toString(i));
                }
            }
        };

        loadCache(f);
    }

    /**
     * @throws Exception if failed
     */
    public void testLoadCacheFromStore() throws Exception {
        loadCache(new IgniteInClosure<Ignite>() {
            @Override public void apply(Ignite grid) {
                grid.cache(null).loadCache(null);
            }
        });
    }

    /**
     * @throws Exception if failed
     */
    public void testLoadCacheWithDataStreamerSequential() throws Exception {
        Ignite g0 = startGrid(0);

        IgniteInternalFuture<Object> fut = runAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 1; i < GRIDS_CNT; i++)
                    startGrid(i);

                return null;
            }
        });

        final HashSet<IgniteFuture> set = new HashSet<>();

        IgniteInClosure<Ignite> f = new IgniteInClosure<Ignite>() {
            @Override public void apply(Ignite grid) {
                try (IgniteDataStreamer<Integer, String> dataStreamer = grid.dataStreamer(null)) {
                    for (int i = 0; i < KEYS_CNT; i++) {
                        set.add(dataStreamer.addData(i, "Data"));

                        if (i % 100000 == 0)
                            log.info("Streaming " + i + "'th entry.");
                    }
                }
            }
        };

        f.apply(g0);

        log.info("Data loaded.");

        fut.get();

        for (IgniteFuture res : set)
            assert res.get() == null;

        IgniteCache<Integer, String> cache = grid(0).cache(null);

        if (cache.size(CachePeekMode.PRIMARY) != KEYS_CNT) {
            Set<Integer> failedKeys = new LinkedHashSet<>();

            for (int i = 0; i < KEYS_CNT; i++)
                if (!cache.containsKey(i)) {
                    for (Ignite ignite : G.allGrids()) {
                        IgniteEx igniteEx = (IgniteEx)ignite;

                        log.info("Missed key info:" +
                            igniteEx.localNode().id() +
                            " primary=" +
                            ignite.affinity(null).isPrimary(igniteEx.localNode(), i) +
                            " backup=" +
                            ignite.affinity(null).isBackup(igniteEx.localNode(), i) +
                            " local peek=" +
                            ignite.cache(null).localPeek(i, CachePeekMode.ONHEAP));
                    }

                    for (int j = i; j < i + 10000; j++)
                        if (!cache.containsKey(j))
                            failedKeys.add(j);

                    break;
                }

            assert failedKeys.isEmpty() : "Some failed keys: " + failedKeys.toString();
        }

        assertCacheSize();
    }

    /**
     * Loads cache using closure and asserts cache size.
     *
     * @param f cache loading closure
     * @throws Exception if failed
     */
    protected void loadCache(IgniteInClosure<Ignite> f) throws Exception {
        Ignite g0 = startGrid(0);

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable<Ignite>() {
            @Override public Ignite call() throws Exception {
                return startGridsMultiThreaded(1, GRIDS_CNT - 1);
            }
        });

        try {
            f.apply(g0);
        }
        finally {
            fut.get();
        }

        assertCacheSize();
    }

    /** Asserts cache size. */
    protected void assertCacheSize() {
        IgniteCache<Integer, String> cache = grid(0).cache(null);

        assertEquals("Data lost.", KEYS_CNT, cache.size(CachePeekMode.PRIMARY));

        int total = 0;

        for (int i = 0; i < GRIDS_CNT; i++)
            total += grid(i).cache(null).localSize(CachePeekMode.PRIMARY);

        assertEquals("Data lost.", KEYS_CNT, total);
    }

    /**
     * Cache store adapter.
     */
    private static class TestCacheStoreAdapter extends CacheStoreAdapter<Integer, String> implements Serializable {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, String> f, Object... args) {
            for (int i = 0; i < KEYS_CNT; i++)
                f.apply(i, Integer.toString(i));
        }

        /** {@inheritDoc} */
        @Nullable @Override public String load(Integer i) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends String> entry)
            throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object o) throws CacheWriterException {
            // No-op.
        }
    }
}