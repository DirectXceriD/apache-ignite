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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataRow;
import org.apache.ignite.internal.processors.cache.tree.mvcc.search.MvccLinkAwareSearchRow;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.worker.GridWorker;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.compare;
import static org.apache.ignite.internal.processors.cache.mvcc.MvccUtils.hasNewMvccVersionFast;
import static org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter.RowData.KEY_ONLY;

/**
 * Vacuum worker.
 */
public class VacuumWorker  extends GridWorker {
    /** */
    private final BlockingQueue<VacuumTask> cleanupQueue;

    /**
     * @param ctx Kernal context.
     * @param log Logger.
     * @param cleanupQueue Cleanup tasks queue.
     */
    VacuumWorker(GridKernalContext ctx, IgniteLogger log, BlockingQueue<VacuumTask> cleanupQueue) {
        super(ctx.igniteInstanceName(), "vacuum-cleaner", log);

        this.cleanupQueue = cleanupQueue;
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        while (!isCancelled()) {
            VacuumTask task = cleanupQueue.take();

            try {
                processPartition(task);
            }
            catch (IgniteCheckedException e) {
                task.onDone(e);
            }
            catch (RuntimeException | Error e) {
                task.onDone(e);

                throw e;
            }
        }
    }

    /**
     * Process partition.
     *
     * @param task VacuumTask.
     * @throws IgniteCheckedException If failed.
     */
    private void processPartition(VacuumTask task) throws IgniteCheckedException {
        long startNanoTime = System.nanoTime();

        GridDhtLocalPartition part = task.part();

        if (part == null || part.state() != OWNING || !part.reserve()) {
            task.onDone(new VacuumMetrics());

            return;
        }

        try {
            GridCursor<? extends CacheDataRow> cursor = part.dataStore().cursor(KEY_ONLY);

            VacuumMetrics metrics = new VacuumMetrics();
            GridCacheContext cctx = null;
            boolean shared = part.group().sharedGroup();

            int curCacheId = CU.UNDEFINED_CACHE_ID;

            if (!shared)
                cctx = part.group().singleCacheContext();

            KeyCacheObject prevKey = null;
            List<MvccLinkAwareSearchRow> cleanupRows = null;

            MvccVersion cleanupVer = task.cleanupVer();

            while (!isCancelled() && cursor.next()){
                MvccDataRow row = (MvccDataRow)cursor.get();

                if (prevKey == null)
                    prevKey = row.key();

                if (cctx == null) {
                    assert shared;

                    curCacheId = row.cacheId();
                    cctx = part.group().shared().cacheContext(curCacheId);
                }

                if (!prevKey.equals(row.key())) {
                    if (cleanupRows != null && !cleanupRows.isEmpty()) {
                        long cleanupStartNanoTime = System.nanoTime();

                        GridCacheEntryEx entry = cctx.cache().entryEx(prevKey);

                        while (true) {
                            entry.lockEntry();

                            if (!entry.obsolete())
                                break;

                            entry.unlockEntry();

                            entry = cctx.cache().entryEx(prevKey);
                        }

                        cctx.shared().database().checkpointReadLock();

                        try {
                            part.dataStore().cleanup(cctx, cleanupRows);

                            metrics.addCleanupNanoTime(System.nanoTime() - cleanupStartNanoTime);
                            metrics.addCleanupRowsCnt(cleanupRows.size());

                            cleanupRows.clear();
                        }
                        finally {
                            cctx.shared().database().checkpointReadUnlock();

                            entry.unlockEntry();
                            cctx.evicts().touch(entry, AffinityTopologyVersion.NONE);
                        }
                    }

                    if (shared && curCacheId != row.cacheId()) {
                        curCacheId = row.cacheId();
                        cctx = part.group().shared().cacheContext(curCacheId);
                    }

                    prevKey = row.key();
                }

                if (compare(row, cleanupVer) <= 0
                    && hasNewMvccVersionFast(row) && MvccUtils.compareNewVersion(row, cleanupVer) <= 0
                    && cctx.shared().coordinators().state(row.newMvccCoordinatorVersion(), row.newMvccCounter()) == TxState.COMMITTED
                    || cctx.shared().coordinators().state(row.mvccCoordinatorVersion(), row.mvccCounter()) == TxState.ABORTED) {

                    if (cleanupRows == null)
                        cleanupRows = new ArrayList<>();

                    cleanupRows.add(new MvccLinkAwareSearchRow(row.cacheId(), row.key(), row.mvccCoordinatorVersion(),
                        row.mvccCounter(), row.mvccOperationCounter(), row.link()));
                }

                metrics.addScannedRowsCount(1);
            }

            if (cleanupRows != null && !cleanupRows.isEmpty()) {
                long cleanupStartNanoTime = System.nanoTime();

                GridCacheEntryEx entry = cctx.cache().entryEx(prevKey);

                while (true) {
                    entry.lockEntry();

                    if (!entry.obsolete())
                        break;

                    entry.unlockEntry();

                    entry = cctx.cache().entryEx(prevKey);
                }

                cctx.shared().database().checkpointReadLock();

                try {
                    part.dataStore().cleanup(cctx, cleanupRows);

                    metrics.addCleanupNanoTime(System.nanoTime() - cleanupStartNanoTime);
                    metrics.addCleanupRowsCnt(cleanupRows.size());
                }
                finally {
                    cctx.shared().database().checkpointReadUnlock();

                    entry.unlockEntry();
                    cctx.evicts().touch(entry, AffinityTopologyVersion.NONE);
                }
            }

            metrics.addSearchNanoTime(System.nanoTime() - startNanoTime - metrics.cleanupNanoTime());

            task.onDone(metrics);
        }
        finally {
            part.release();
        }
    }
}
