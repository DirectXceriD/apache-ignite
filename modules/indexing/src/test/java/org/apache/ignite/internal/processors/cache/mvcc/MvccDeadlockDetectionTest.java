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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.transactions.IgniteTxRollbackCheckedException;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/** */
@RunWith(JUnit4.class)
public class MvccDeadlockDetectionTest extends GridCommonAbstractTest {
    /** */
    private IgniteEx client;

    /** */
    private void setUpGrids(int n, boolean indexed) throws Exception {
        Ignite ign = startGridsMultiThreaded(n);
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
        if (indexed)
            ccfg.setIndexedTypes(Integer.class, Integer.class);

        ign.getOrCreateCache(ccfg);

        G.setClientMode(true);

        client = startGrid(n);
    }

    /** */
    @After
    public void cleanupTest() throws Exception {
        stopAllGrids();
    }

    /** */
    @Test
    public void detectSimpleDeadlock() throws Exception {
        setUpGrids(2, false);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key0, 0);
                b.await();
                cache.put(key1, 1);

                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key1, 1);
                b.await();
                cache.put(key0, 0);

                tx.commit();
            }

            return null;
        });

        assertAtLeastOneAbortedDueDeadlock(fut0, fut1);
    }

    /** */
    @Test
    public void detectSimpleDeadlockFastUpdate() throws Exception {
        setUpGrids(2, true);

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key0, -1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key1, -1));

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key = ?").setArgs(key0));
                b.await();
                cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key = ?").setArgs(key1));

                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 1 where _key = ?").setArgs(key1));
                b.await();
                cache.query(new SqlFieldsQuery("update Integer set _val = 1 where _key = ?").setArgs(key0));

                tx.commit();
            }

            return null;
        });

        assertAtLeastOneAbortedDueDeadlock(fut0, fut1);
    }

    /** */
    @Test
    public void detect3Deadlock() throws Exception {
        setUpGrids(3, false);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));
        Integer key2 = primaryKey(grid(2).cache(DEFAULT_CACHE_NAME));

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(3);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key0, 0);
                b.await();
                cache.put(key1, 1);

                tx.rollback();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key1, 0);
                b.await();
                cache.put(key2, 1);

                tx.rollback();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut2 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key2, 1);
                b.await();
                cache.put(key0, 0);

                tx.rollback();
            }

            return null;
        });

        assertAtLeastOneAbortedDueDeadlock(fut0, fut1, fut2);
    }

    /** */
    @Test
    public void detectMultipleLockWaitDeadlock() throws Exception {
        // T0 -> T1
        //  \-> T2 -> T0
        setUpGrids(3, true);

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));
        Integer key2 = primaryKey(grid(2).cache(DEFAULT_CACHE_NAME));

        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key0, -1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key1, -1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key2, -1));

        CyclicBarrier b = new CyclicBarrier(3);

        IgniteInternalFuture<Object> fut2 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 2 where _key = ?").setArgs(key2));
                b.await();
                cache.query(new SqlFieldsQuery("update Integer set _val = 2 where _key = ?").setArgs(key0));

                // rollback to prevent waiting tx abort due write conflict
                tx.rollback();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 1 where _key = ?").setArgs(key1));
                b.await();
                GridTestUtils.waitForCondition(fut2::isDone, 1000);

                // rollback to prevent waiting tx abort due write conflict
                tx.rollback();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key = ?").setArgs(key0));
                b.await();
                cache.query(
                    new SqlFieldsQuery("update Integer set _val = 0 where _key = ? or _key = ?").setArgs(key2, key1));

                tx.commit();
            }

            return null;
        });

        fut1.get(10, TimeUnit.SECONDS);

        assertAtLeastOneAbortedDueDeadlock(fut0, fut2);
    }

    /** */
    @Test
    public void detectDeadlockLocalEntriesEnlistFuture() throws Exception {
        setUpGrids(1, false);

        List<Integer> keys = primaryKeys(grid(0).cache(DEFAULT_CACHE_NAME), 2);

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(keys.get(0), 11);
                b.await();
                cache.put(keys.get(1), 11);

                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(keys.get(1), 22);
                b.await();
                cache.put(keys.get(0), 22);

                tx.commit();
            }

            return null;
        });

        assertAtLeastOneAbortedDueDeadlock(fut0, fut1);
    }

    /** */
    @Test
    public void detectDeadlockLocalPrimary() throws Exception {
        // Checks that case when near tx does local on enlist on the same node and no dht tx is created

        setUpGrids(2, false);

        IgniteCache<Object, Object> cache0 = grid(0).cache(DEFAULT_CACHE_NAME);
        IgniteCache<Object, Object> cache1 = grid(1).cache(DEFAULT_CACHE_NAME);

        int key0 = primaryKey(cache0);
        int key1 = primaryKey(cache1);

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = grid(0).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache0.put(key1, 11);
                b.await();
                cache0.put(key0, 11);

                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = grid(1).transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache1.put(key0, 22);
                b.await();
                cache1.put(key1, 22);

                tx.commit();
            }

            return null;
        });

        assertAtLeastOneAbortedDueDeadlock(fut0, fut1);
    }

    /** */
    @Test
    public void detectDeadlockLocalQueryEnlistFuture() throws Exception {
        setUpGrids(1, true);

        List<Integer> keys = primaryKeys(grid(0).cache(DEFAULT_CACHE_NAME), 2);

        Collections.sort(keys);

        Integer key0 = keys.get(0), key1 = keys.get(1);

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key0, -1));
        cache.query(new SqlFieldsQuery("insert into Integer(_key, _val) values(?, ?)").setArgs(key1, -1));

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut0 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key <= ?").setArgs(key0));
                b.await();
                cache.query(new SqlFieldsQuery("update Integer set _val = 0 where _key >= ?").setArgs(key1));

                tx.commit();
            }

            return null;
        });

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.query(new SqlFieldsQuery("update Integer set _val = 1 where _key >= ?").setArgs(key1));
                b.await();
                cache.query(new SqlFieldsQuery("update Integer set _val = 1 where _key <= ?").setArgs(key0));

                tx.commit();
            }

            return null;
        });

        assertAtLeastOneAbortedDueDeadlock(fut0, fut1);
    }

    /** */
    private void assertAtLeastOneAbortedDueDeadlock(IgniteInternalFuture<?>... futs) throws IgniteCheckedException {
        assert futs.length > 0;

        int aborted = 0;

        for (IgniteInternalFuture<?> fut : futs) {
            try {
                fut.get(10, TimeUnit.SECONDS);
            }
            catch (IgniteCheckedException e) {
                // TODO check expected exceptions once https://issues.apache.org/jira/browse/IGNITE-9470 is resolved
                if (X.hasCause(e, TransactionRollbackException.class)
                    || X.hasCause(e, IgniteTxRollbackCheckedException.class))
                    aborted++;
                else
                    throw e;
            }
        }

        log.info(aborted + " tx(s) aborted.");

        if (aborted == 0)
            fail("At least one tx is expected to be aborted");
    }
}

