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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.db.wal.IgniteWalRebalanceTest;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.configuration.WALMode.LOG_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.retryAssert;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;

/**
 * Mini test framework for ordering transaction's prepares and commits by intercepting messages and releasing then in user defined order.
 */
public abstract class TxPartitionCounterStateAbstractTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int MB = 1024 * 1024;

    /** */
    private int backups;

    /** */
    public static final int TEST_TIMEOUT = 30_000;

    /** */
    private AtomicReference<Throwable> testFailed = new AtomicReference<>();

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
            setWalSegmentSize(8 * MB).setWalMode(LOG_ONLY).setPageSize(1024).setCheckpointFrequency(10000000000L).
            setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true).
                setInitialSize(100 * MB).setMaxSize(100 * MB)));

        cfg.setFailureDetectionTimeout(600000);

        if (!client) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setAtomicityMode(TRANSACTIONAL);
            ccfg.setBackups(backups);
            ccfg.setWriteSynchronizationMode(FULL_SYNC);
            ccfg.setOnheapCacheEnabled(false);
            ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @param partId Partition id.
     * @param nodesCnt Nodes count.
     * @param sizes Sizes.
     * @param cb Callback.
     */
    protected void runOnPartition(int partId, int backups, int nodesCnt, TxCallback cb, int[] sizes) throws Exception {
        this.backups = backups;

        IgniteEx crd = (IgniteEx)startGridsMultiThreaded(nodesCnt);

        assertEquals(0, crd.cache(DEFAULT_CACHE_NAME).size());

        int[][] ranges = new int[sizes.length][2];

        int totalKeys = 0;

        for (int i = 0; i < sizes.length; i++) {
            int size = sizes[i];

            ranges[i] = new int[] {totalKeys, size};

            totalKeys += size;
        }

        List<Integer> keys = partitionKeys(crd.cache(DEFAULT_CACHE_NAME), partId, totalKeys);

        IgniteEx client = startGrid("client");

        Ignite prim = primaryNode(keys.get(0), DEFAULT_CACHE_NAME);

        List<Ignite> backupz = backups == 0 ? null : backups == 1 ? Collections.singletonList(backupNode(keys.get(0), DEFAULT_CACHE_NAME)) : backupNodes(keys.get(0), DEFAULT_CACHE_NAME);

        final TestRecordingCommunicationSpi clientWrappedSpi = TestRecordingCommunicationSpi.spi(client);

        Map<IgniteUuid, GridCacheVersion> futMap = new ConcurrentHashMap<>();
        Map<GridCacheVersion, GridCacheVersion> nearToLocVerMap = new ConcurrentHashMap<>();

        clientWrappedSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                IgniteEx to = IgnitionEx.gridxx(node.id());

                if (msg instanceof GridNearTxPrepareRequest) {
                    GridNearTxPrepareRequest req = (GridNearTxPrepareRequest)msg;

                    futMap.put(req.futureId(), req.version());

                    return cb.beforePrimaryPrepare(to, req.version().asGridUuid(), createSendFuture(clientWrappedSpi, msg));
                }
                else if (msg instanceof GridNearTxFinishRequest) {
                    GridNearTxFinishRequest req = (GridNearTxFinishRequest)msg;

                    futMap.put(req.futureId(), req.version());

                    IgniteInternalTx tx = findTx(to, req.version(), true);

                    assertNotNull(tx);

                    return cb.beforePrimaryFinish(to, tx, createSendFuture(clientWrappedSpi, msg));
                }

                return false;
            }
        });

        TestRecordingCommunicationSpi primWrapperSpi = TestRecordingCommunicationSpi.spi(prim);

        primWrapperSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                IgniteEx to = IgnitionEx.gridxx(node.id());

                if (msg instanceof GridDhtTxPrepareRequest) {
                    GridDhtTxPrepareRequest req = (GridDhtTxPrepareRequest)msg;

                    futMap.put(req.futureId(), req.nearXidVersion());
                    nearToLocVerMap.put(req.version(), req.nearXidVersion());

                    IgniteEx from = fromNode(primWrapperSpi);

                    IgniteInternalTx primTx = findTx(from, req.nearXidVersion(), true);

                    return cb.beforeBackupPrepare(from, to, primTx, createSendFuture(primWrapperSpi, msg));
                }
                else if (msg instanceof GridDhtTxFinishRequest) {
                    GridDhtTxFinishRequest req = (GridDhtTxFinishRequest)msg;

                    GridCacheVersion nearVer = nearToLocVerMap.get(req.version());
                    futMap.put(req.futureId(), nearVer);

                    IgniteEx from = fromNode(primWrapperSpi);

                    IgniteInternalTx primTx = findTx(from, nearVer, true);
                    IgniteInternalTx backupTx = findTx(to, nearVer, false);

                    return cb.beforeBackupFinish(from, to, primTx, backupTx, nearVer.asGridUuid(), createSendFuture(primWrapperSpi, msg));
                }
                else if (msg instanceof GridNearTxPrepareResponse) {
                    GridNearTxPrepareResponse resp = (GridNearTxPrepareResponse)msg;

                    IgniteEx from = fromNode(primWrapperSpi);

                    IgniteInternalTx primTx = findTx(from, futMap.get(resp.futureId()), true);

                    return cb.afterPrimaryPrepare(from, primTx, createSendFuture(primWrapperSpi, msg));
                }
                else if (msg instanceof GridNearTxFinishResponse) {
                    GridNearTxFinishResponse req = (GridNearTxFinishResponse)msg;

                    IgniteEx from = fromNode(primWrapperSpi);

                    IgniteUuid nearVer = futMap.get(req.futureId()).asGridUuid();

                    return cb.afterPrimaryFinish(from, nearVer, createSendFuture(primWrapperSpi, msg));
                }

                return false;
            }
        });

        if (backupz != null) {
            for (Ignite backup : backupz) {
                TestRecordingCommunicationSpi backupWrapperSpi = TestRecordingCommunicationSpi.spi(backup);

                backupWrapperSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                    @Override public boolean apply(ClusterNode node, Message msg) {
                        IgniteEx from = IgnitionEx.gridxx(backupWrapperSpi.getSpiContext().localNode().id());
                        IgniteEx to = IgnitionEx.gridxx(node.id());

                        if (msg instanceof GridDhtTxPrepareResponse) {
                            GridDhtTxPrepareResponse resp = (GridDhtTxPrepareResponse)msg;

                            IgniteInternalTx tx = findTx(from, futMap.get(resp.futureId()), false);

                            return cb.afterBackupPrepare(from, tx, createSendFuture(backupWrapperSpi, msg));
                        }
                        else if (msg instanceof GridDhtTxFinishResponse) {
                            GridDhtTxFinishResponse resp = (GridDhtTxFinishResponse)msg;

                            GridCacheVersion ver = futMap.get(resp.futureId());

                            return cb.afterBackupFinish(from, ver.asGridUuid(), createSendFuture(backupWrapperSpi, msg));
                        }

                        return false;
                    }
                });
            }
        }

        assertNotNull(client.cache(DEFAULT_CACHE_NAME));

        AtomicInteger idx = new AtomicInteger();

        CyclicBarrier b = new CyclicBarrier(sizes.length);

        IgniteInternalFuture<Long> fut = runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int txIdx = idx.getAndIncrement();

                int[] range = ranges[txIdx];

                String lb = "t" + idx;

                try (Transaction tx = client.transactions().withLabel(lb).txStart()) {
                    cb.onTxStart(tx, txIdx);

                    U.awaitQuiet(b); // Wait should always success.

                    for (Integer key : keys.subList(range[0], range[0] + range[1]))
                        client.cache(DEFAULT_CACHE_NAME).put(key, 0);

                    tx.commit();
                }
                catch (Exception ignored) {
                    // No-op.
                }

                // TODO FIXME expect rollback for some scenarios.
            }
        }, sizes.length, "tx-thread");

        try {
            fut.get(TEST_TIMEOUT);
        }
        catch (IgniteCheckedException e) {
            Throwable err = testFailed.get();

            if (err != null)
                log.error("Test execution failed", err);

            fail("Test is timed out");
        }
    }

    /**
     * @param wrapperSpi Wrapper spi.
     * @param msg Message.
     */
    private GridFutureAdapter<?> createSendFuture(TestRecordingCommunicationSpi wrapperSpi, Message msg) {
        GridFutureAdapter<?> fut = new GridFutureAdapter<Object>();

        fut.listen(new IgniteInClosure<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> fut) {
                wrapperSpi.stopBlock(true, new IgnitePredicate<T2<ClusterNode, GridIoMessage>>() {
                    @Override public boolean apply(T2<ClusterNode, GridIoMessage> objects) {
                        return objects.get2().message() == msg;
                    }
                }, false);
            }
        });

        return fut;
    }

    protected static interface TxCallback {
        public boolean beforePrimaryPrepare(IgniteEx node, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut);

        /**
         * @param prim Node.
         * @param backup Backup prim.
         * @param primaryTx Primary tx.
         * @param proceedFut Proceed future.
         */
        public boolean beforeBackupPrepare(IgniteEx prim, IgniteEx backup, IgniteInternalTx primaryTx,
            GridFutureAdapter<?> proceedFut);

        boolean beforePrimaryFinish(IgniteEx primaryNode, IgniteInternalTx tx, GridFutureAdapter<?> proceedFut);

        boolean afterPrimaryFinish(IgniteEx primaryNode, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut);

        boolean afterBackupPrepare(IgniteEx n, IgniteInternalTx tx, GridFutureAdapter<?> fut);

        boolean afterBackupFinish(IgniteEx n, IgniteUuid nearXidVer, GridFutureAdapter<?> fut);

        /**
         * @param prim Prim.
         * @param backup Backup.
         * @param primTx Prim tx. Null for 2pc.
         * @param backupTx Backup tx.
         * @param nearXidVer
         * @param future Future.
         */
        boolean beforeBackupFinish(IgniteEx prim, IgniteEx backup, @Nullable IgniteInternalTx primTx,
            IgniteInternalTx backupTx,
            IgniteUuid nearXidVer, GridFutureAdapter<?> future);

        boolean afterPrimaryPrepare(IgniteEx from, IgniteInternalTx tx, GridFutureAdapter<?> fut);

        /**
         * @param tx Tx.
         * @param idx Index.
         */
        void onTxStart(Transaction tx, int idx);
    }

    /** */
    protected class TxCallbackAdapter implements TxCallback {
        private Map<Integer, IgniteUuid> txMap = new ConcurrentHashMap<>();
        private Map<IgniteUuid, Integer> revTxMap = new ConcurrentHashMap<>();

        @Override public boolean beforePrimaryPrepare(IgniteEx node, IgniteUuid nearXidVer, GridFutureAdapter<?> proceedFut) {
            return false;
        }

        @Override public boolean beforeBackupPrepare(IgniteEx prim, IgniteEx backup, IgniteInternalTx primaryTx,
            GridFutureAdapter<?> proceedFut) {
            return false;
        }

        @Override public boolean beforePrimaryFinish(IgniteEx primaryNode, IgniteInternalTx tx, GridFutureAdapter<?> proceedFut) {
            return false;
        }

        @Override public boolean afterPrimaryFinish(IgniteEx primaryNode, IgniteUuid nearXidVer,
            GridFutureAdapter<?> proceedFut) {
            return false;
        }

        @Override public boolean afterBackupPrepare(IgniteEx n, IgniteInternalTx tx, GridFutureAdapter<?> fut) {
            return false;
        }

        @Override public boolean afterBackupFinish(IgniteEx n, IgniteUuid nearXidVer, GridFutureAdapter<?> fut) {
            return false;
        }

        @Override public boolean beforeBackupFinish(IgniteEx prim, IgniteEx backup, IgniteInternalTx primTx,
            IgniteInternalTx backupTx,
            IgniteUuid nearXidVer, GridFutureAdapter<?> future) {
            return false;
        }

        @Override public boolean afterPrimaryPrepare(IgniteEx from, IgniteInternalTx tx, GridFutureAdapter<?> fut) {
            return false;
        }

        protected IgniteUuid version(int order) {
            return txMap.get(order);
        }

        protected int order(IgniteUuid ver) {
            return revTxMap.get(ver);
        }

        @Override public void onTxStart(Transaction tx, int idx) {
            txMap.put(idx, tx.xid());
            revTxMap.put(tx.xid(), idx);
        }
    }



    /**
     * Find a tx by near xid version.
     *
     * @param n Node.
     * @param nearVer Near version.
     * @param primary {@code True} to search primary tx.
     */
    private IgniteInternalTx findTx(IgniteEx n, GridCacheVersion nearVer, boolean primary) {
        return n.context().cache().context().tm().activeTransactions().stream().filter(new Predicate<IgniteInternalTx>() {
            @Override public boolean test(IgniteInternalTx tx) {
                return nearVer.equals(tx.nearXidVersion()) && tx.local() == primary;
            }
        }).findAny().orElse(null);
    }

    /**
     * @param r Runnable.
     */
    public void runAsync(Runnable r) {
        IgniteInternalFuture fut = GridTestUtils.runAsync(r);

        // Fail test if future failed to finish normally.
        fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
            @Override public void apply(IgniteInternalFuture fut0) {
                try {
                    fut0.get();
                }
                catch (Throwable t) {
                    testFailed.set(t);
                }
            }
        });
    }

    /**
     * @param partId Partition id.
     */
    protected PartitionUpdateCounter counter(int partId) {
        return internalCache(0).context().topology().localPartition(partId).dataStore().partUpdateCounter();
    }

    /**
     * @param partId Partition id.
     */
    protected PartitionUpdateCounter counter(int partId, String gridName) {
        return internalCache(grid(gridName).cache(DEFAULT_CACHE_NAME)).context().topology().localPartition(partId).dataStore().partUpdateCounter();
    }


    /**
     * @param skipCheckpointOnStop Skip checkpoint on stop.
     * @param name Grid instance.
     */
    protected void stopGrid(boolean skipCheckpointOnStop, String name) {
        IgniteEx grid = grid(name);

        if (skipCheckpointOnStop) {
            GridCacheDatabaseSharedManager db =
                (GridCacheDatabaseSharedManager)grid.context().cache().context().database();

            db.enableCheckpoints(false);
        }

        stopGrid(grid.name(), skipCheckpointOnStop);
    }

    private IgniteEx fromNode(TestRecordingCommunicationSpi primWrapperSpi) {
        return IgnitionEx.gridxx(primWrapperSpi.getSpiContext().localNode().id());
    }

    /**
     * @param res Response.
     */
    protected void assertPartitionsSame(IdleVerifyResultV2 res) throws AssertionFailedError {
        if (res.hasConflicts()) {
            StringBuilder b = new StringBuilder();

            res.print(b::append);

            fail(b.toString());
        }
    }
}
