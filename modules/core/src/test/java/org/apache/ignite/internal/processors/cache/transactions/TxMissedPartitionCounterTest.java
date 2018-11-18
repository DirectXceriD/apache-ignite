package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.pagemem.wal.WALWriteListener;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.eclipse.jetty.util.ConcurrentHashSet;

import static java.util.Collections.max;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test data loss on recovery due to missed partition counter on tx messages reorder.
 */
public class TxMissedPartitionCounterTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static final int MB = 1024 * 1024;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId("node" + igniteInstanceName);
        cfg.setFailureHandler(new StopNodeFailureHandler());

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi());

        boolean client = igniteInstanceName.startsWith("client");

        cfg.setClientMode(client);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().
            setWalSegmentSize(12 * MB).setWalMode(LOG_ONLY).setPageSize(1024).setCheckpointFrequency(10000000000L).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        cfg.setFailureDetectionTimeout(60000);

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(2);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setOnheapCacheEnabled(false);
            ccfg.setAffinity(new RendezvousAffinityFunction(false, 1));

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();

        System.setProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD, "0");

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.cleanup();

        stopAllGrids();

        cleanPersistenceDir();

        System.clearProperty(IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD);

//        if (!walRebalanceInvoked)
//            throw new AssertionError("WAL rebalance hasn't been invoked.");
    }

    /** */
    public void testMissedPartitionCounter() throws Exception {
        IgniteEx client = startGrid("client");

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        int part = 0;

        final int txCnt = 2;

        List<Integer> keys = loadDataToPartition(part, DEFAULT_CACHE_NAME, 5000, 0, txCnt);

        forceCheckpoint();

        Ignite backupNode = backupNode(keys.get(0), DEFAULT_CACHE_NAME);

        Map<UUID, T3<CountDownLatch, Set<Long>, Boolean>> latches = new ConcurrentHashMap<>();

        for (Ignite ignite : G.allGrids()) {
            if (ignite != backupNode)
                continue;

            IgniteEx igniteEx = (IgniteEx)ignite;

            GridCacheSharedContext<Object, Object> ctx = igniteEx.context().cache().context();

            IgniteTxManager tm = ctx.tm();

            tm.walWriteListener(new WALWriteListener() {
                @Override public void beforeWrite(List<DataEntry> entries) {
                    try {
                        long cntr = entries.get(0).partitionCounter();

                        T3<CountDownLatch, Set<Long>, Boolean> val = new T3<>(new CountDownLatch(txCnt), new ConcurrentSkipListSet<>(), Boolean.FALSE);
                        T3<CountDownLatch, Set<Long>, Boolean> oldVal = latches.putIfAbsent(ignite.cluster().localNode().id(), val);

                        if (oldVal != null)
                            val = oldVal;

                        val.get2().add(cntr);

                        val.get1().countDown();
                        assertTrue(U.await(val.get1(), 10, TimeUnit.SECONDS));

                        // Compute max counter and fail nodes with lesser counter before writing to WAL.
                        long maxCntr = max(val.get2());

                        // Fail nodes with lowest counters.
                        if (cntr < maxCntr) {
                            // Wait until max counter is written.
                            synchronized (val) {
                                while (val.get3() != Boolean.TRUE)
                                    U.wait(val);
                            }

                            throw new RuntimeException("Fail node");
                        }
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        fail();
                    }
                }

                @Override public void afterWrite(List<DataEntry> entries) {
                    T3<CountDownLatch, Set<Long>, Boolean> val = latches.get(ignite.cluster().localNode().id());

                    long maxCntr = max(val.get2());

                    // Unblock waiters after write of max counter.
                    if (entries.get(0).partitionCounter() == maxCntr) {
                        synchronized (val) {
                            val.set3(Boolean.TRUE);

                            val.notifyAll();
                        }
                    }
                }
            });
        }

        AtomicInteger id = new AtomicInteger();

        IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
            try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                int idx = id.getAndIncrement();

                client.cache(DEFAULT_CACHE_NAME).put(keys.get(idx), idx);

                tx.commit();
            }
        }, txCnt, "tx-thread");

        fut.get();

        // Wait for backups stop.
        waitForTopology(GRID_CNT);

        awaitPartitionMapExchange();

        for (int i = 0; i < txCnt; i++)
            assertEquals(i, client.cache(DEFAULT_CACHE_NAME).get(keys.get(i)));

        forceCheckpoint();

        stopAllGrids();

        Ignite ex = startGridsMultiThreaded(GRID_CNT);

        awaitPartitionMapExchange();

        Map<Integer, Set<Long>> map = IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.allRebalances();
        boolean walRebalanceInvoked = !map.isEmpty();

        IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.cleanup();

        IdleVerifyResultV2 res = idleVerify(grid(0), DEFAULT_CACHE_NAME);

        if (res.hasConflicts()) {
            StringBuilder b = new StringBuilder();

            res.print(b::append);

            fail(b.toString());
        }
    }

    public void testHistory() throws Exception {
        IgniteEx crd = startGrid(0);
        startGrid(1);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        int part = 0;

        List<Integer> keys = loadDataToPartition(part, DEFAULT_CACHE_NAME, 100, 0, 1);

        forceCheckpoint(); // Prevent IGNITE-10088

        stopGrid(1);

        awaitPartitionMapExchange();

        List<Integer> keys1 = loadDataToPartition(part, DEFAULT_CACHE_NAME, 100, 100, 1);

        startGrid(1);

        awaitPartitionMapExchange();
    }

    public void testNodeRestartWithHolesDuringRebalance() {

    }

    @Override protected long getTestTimeout() {
        return 10000000000000L;
    }
}
