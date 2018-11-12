package org.apache.ignite.internal.processors.cache.transactions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteSystemProperties;
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
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.util.typedef.T2;
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
    private static final int GRID_CNT = 2;

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
//
//        startGridsMultiThreaded(GRID_CNT);
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

        List<Integer> keys = loadDataToPartition(part, DEFAULT_CACHE_NAME, 5000, 0, 2);

        forceCheckpoint();

        TestRecordingCommunicationSpi spi0 = TestRecordingCommunicationSpi.spi(grid(0));

        spi0.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtTxFinishRequest) {
                    GridDhtTxFinishRequest m = (GridDhtTxFinishRequest)msg;

                    return true;
                }

                return false;
            }
        });

        // Start two tx mapped to same primary partition.
        IgniteInternalFuture fut0 = runAsync(new Runnable() {
            @Override public void run() {
                try(Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                    client.cache(DEFAULT_CACHE_NAME).put(keys.get(0), 0);

                    tx.commit();
                }
            }
        });

        IgniteInternalFuture fut1 = runAsync(new Runnable() {
            @Override public void run() {
                try(Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 1)) {
                    client.cache(DEFAULT_CACHE_NAME).put(keys.get(1), 1);

                    tx.commit();
                }
            }
        });

        spi0.waitForBlocked(4);
        spi0.stopBlock();

        fut0.get();
        fut1.get();

        // Wait for backups stop.
        waitForTopology(2);

        awaitPartitionMapExchange();

        assertEquals(0, client.cache(DEFAULT_CACHE_NAME).get(keys.get(0)));
        assertEquals(1, client.cache(DEFAULT_CACHE_NAME).get(keys.get(1)));

        forceCheckpoint();

        stopAllGrids();

        IgniteEx ex = startGrid(0);
        startGrid(1);

        ex.cluster().active(true);

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

        List<Integer> keys = loadDataToPartition(part, DEFAULT_CACHE_NAME, 100, 0, 100);

        forceCheckpoint(); // Prevent IGNITE-10088

        stopGrid(1);

        awaitPartitionMapExchange();

        List<Integer> keys1 = loadDataToPartition(part, DEFAULT_CACHE_NAME, 100, 100, 100);

        assertEquals(200, grid(0).cache(DEFAULT_CACHE_NAME).size());

        stopGrid(0);

        IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.cleanup();

        crd = startGrid(0);
        startGrid(1);

        crd.cluster().active(true);

        awaitPartitionMapExchange();

        Map<Integer, Set<Long>> map = IgniteWalRebalanceTest.WalRebalanceCheckingCommunicationSpi.allRebalances();

        assertTrue(map.size() > 0);
    }

    public void testNodeRestartWithHolesDuringRebalance() {

    }

    @Override protected long getTestTimeout() {
        return 10000000000000L;
    }
}
