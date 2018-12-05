package org.apache.ignite.internal.processors.cache.mvcc;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

@RunWith(JUnit4.class)
public class MvccDeadlockDetectionTest extends GridCommonAbstractTest {
    private Ignite client;

    private void setUpGrids(int n) throws Exception {
        Ignite ign = startGridsMultiThreaded(n);
        ign.getOrCreateCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT));

        G.setClientMode(true);

        client = startGrid(n);
    }

    @After
    public void tearDown1() throws Exception {
        stopAllGrids();
    }

    @Test
    public void detectSimpleDeadlock() throws Exception {
        // t0d0 ensure test will not hang
        setUpGrids(2);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(2);

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key0, 0);
                b.await();
                cache.put(key1, 1);

                tx.commit();
            }
            return null;
        });

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key1, 1);
            b.await();
            TimeUnit.SECONDS.sleep(2);
            cache.put(key0, 0);

            tx.commit();
        }

        fut.get();
    }

    @Test
    public void detect3Deadlock() throws Exception {
        // t0d0 ensure test will not hang
        setUpGrids(3);

        Integer key0 = primaryKey(grid(0).cache(DEFAULT_CACHE_NAME));
        Integer key1 = primaryKey(grid(1).cache(DEFAULT_CACHE_NAME));
        Integer key2 = primaryKey(grid(2).cache(DEFAULT_CACHE_NAME));

        IgniteCache<Object, Object> cache = client.cache(DEFAULT_CACHE_NAME);

        assert client.configuration().isClientMode();

        CyclicBarrier b = new CyclicBarrier(3);

        IgniteInternalFuture<Object> fut1 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key0, 0);
                b.await();
                cache.put(key1, 1);

                tx.commit();
            }
            return null;
        });

        IgniteInternalFuture<Object> fut2 = GridTestUtils.runAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(key1, 0);
                b.await();
                cache.put(key2, 1);

                tx.commit();
            }
            return null;
        });

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key2, 1);
            b.await();
            cache.put(key0, 0);

            tx.commit();
        }

        fut1.get();
        fut2.get();
    }
}