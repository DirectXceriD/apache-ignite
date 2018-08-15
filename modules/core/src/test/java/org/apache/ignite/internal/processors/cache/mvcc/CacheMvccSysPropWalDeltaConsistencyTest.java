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
package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.wal.memtracker.PageMemoryTrackerPluginProvider;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.ReadMode.GET;
import static org.apache.ignite.internal.processors.cache.mvcc.CacheMvccAbstractTest.WriteMode.PUT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Wal delta records consistency check.
 */
public class CacheMvccSysPropWalDeltaConsistencyTest extends CacheMvccAbstractWalDeltaConsistencyTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(PageMemoryTrackerPluginProvider.IGNITE_ENABLE_PAGE_MEMORY_TRACKER, "true");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(PageMemoryTrackerPluginProvider.IGNITE_ENABLE_PAGE_MEMORY_TRACKER);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPluginConfigurations();

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public final void testSimplePartitioned() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 2, DFLT_PARTITION_COUNT);

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, Object> cache0 = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 3_000; i++)
                cache0.put(i, "Cache value " + i);

            tx.commit();
        }

        IgniteEx ignite1 = startGrid(1);

        for (int i = 2_000; i < 5_000; i++)
            cache0.put(i, "Changed cache value " + i);

        for (int i = 1_000; i < 4_000; i++) {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache0.remove(i);

                tx.commit();
            }
        }

        IgniteCache<Integer, Object> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 1_000; i++)
                cache1.put(i, "Cache value " + i);

            tx.commit();
        }

        forceCheckpoint();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public final void testSimpleReplicated() throws Exception {
        ccfg = cacheConfiguration(cacheMode(), FULL_SYNC, 0, DFLT_PARTITION_COUNT)
            .setCacheMode(CacheMode.REPLICATED);

        IgniteEx ignite0 = startGrid(0);

        ignite0.cluster().active(true);

        IgniteCache<Integer, Object> cache0 = ignite0.getOrCreateCache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 3_000; i++)
                cache0.put(i, "Cache value " + i);

            tx.commit();
        }

        IgniteEx ignite1 = startGrid(1);

        for (int i = 2_000; i < 5_000; i++)
            cache0.put(i, "Changed cache value " + i);

        for (int i = 1_000; i < 4_000; i++) {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache0.remove(i);

                tx.commit();
            }
        }

        IgniteCache<Integer, Object> cache1 = ignite1.getOrCreateCache(DEFAULT_CACHE_NAME);

        try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 0; i < 1_000; i++)
                cache1.put(i, "Cache value " + i);

            tx.commit();
        }

        forceCheckpoint();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentWrites() throws Exception {
        int srvs = 4;
        int clients = 2;

        accountsTxReadAll(srvs, clients, 2, 2, null, true, GET, PUT, 5_000, null);

        forceCheckpoint();
    }
}
