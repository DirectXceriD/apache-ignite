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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Tracks pending transactions for purposes of consistent cut algorithm.
 */
public class LocalPendingTransactionsTracker {
    /** Tx finish timeout. */
    private static final int TX_FINISH_TIMEOUT = 10_000;
    // todo GG-13416: introduce ignite constant

    /** Cctx. */
    private final GridCacheSharedContext<?, ?> cctx;

    /**
     * @param cctx Cctx.
     */
    public LocalPendingTransactionsTracker(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;
    }

    /** Currently pending transactions. */
    private volatile ConcurrentHashMap<GridCacheVersion, WALPointer> currentlyPreparedTxs = new ConcurrentHashMap<>();

    /**
     * Transactions that were transitioned to pending state since last {@link #startTrackingPrepared()} call.
     * Transaction remains in this map after commit/rollback.
     */
    private volatile ConcurrentHashMap<GridCacheVersion, WALPointer> trackedPreparedTxs = new ConcurrentHashMap<>();

    /**
     * Transactions that were transitioned to commited state since last {@link #startTrackingCommited()} call.
     */
    private volatile ConcurrentHashMap<GridCacheVersion, WALPointer> trackedCommitedTxs = new ConcurrentHashMap<>();

    /** Written keys to near xid version. */
    private volatile ConcurrentHashMap<KeyCacheObject, List<GridCacheVersion>> writtenKeysToNearXidVer = new ConcurrentHashMap<>();

    /** Dependent transactions graph. */
    private volatile ConcurrentHashMap<GridCacheVersion, Set<GridCacheVersion>> dependentTransactionsGraph = new ConcurrentHashMap<>();
    // todo GG-13416: maybe handle local sequential consistency with threadId

    /** State rw-lock. */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    /** Track prepared flag. */
    private final AtomicBoolean trackPrepared = new AtomicBoolean(false);

    /** Track commited flag. */
    private final AtomicBoolean trackCommited = new AtomicBoolean(false);

    /** Failed to finish in timeout txs. */
    private volatile ConcurrentHashMap<GridCacheVersion, WALPointer> failedToFinishInTimeoutTxs = null;

    /** Tx finish await future. */
    private volatile GridFutureAdapter<List<GridCacheVersion>> txFinishAwaitFut = null;
    // todo GG-13416: handle timeout for hang in PREPARED txs

    /**
     *
     */
    public Map<GridCacheVersion, WALPointer> currentPendingTxs() {
        assert stateLock.writeLock().isHeldByCurrentThread();

        return U.sealMap(currentlyPreparedTxs);
    }

    public void startTrackingPrepared() {
        assert stateLock.writeLock().isHeldByCurrentThread();

        trackPrepared.set(true);
    }

    /**
     * @return nearXidVer -> prepared WAL ptr
     */
    public Map<GridCacheVersion, WALPointer> stopTrackingPrepared() {
        assert stateLock.writeLock().isHeldByCurrentThread();

        trackPrepared.set(false);

        Map<GridCacheVersion, WALPointer> res = U.sealMap(trackedPreparedTxs);

        trackedPreparedTxs = new ConcurrentHashMap<>();

        return res;
    }

    /**
     *
     */
    public void startTrackingCommited() {
        assert stateLock.writeLock().isHeldByCurrentThread();

        trackCommited.set(true);
    }

    /**
     * @return nearXidVer -> prepared WAL ptr
     */
    public Map<GridCacheVersion, WALPointer> stopTrackingCommited() {
        assert stateLock.writeLock().isHeldByCurrentThread();

        trackCommited.set(false);

        Map<GridCacheVersion, WALPointer> res = U.sealMap(trackedCommitedTxs);

        trackedCommitedTxs = new ConcurrentHashMap<>();

        return res;
    }

    /**
     * @return Future with collection of transactions that failed to finish within timeout.
     */
    public IgniteInternalFuture<List<GridCacheVersion>> awaitFinishOfPreparedTxs() {
        assert stateLock.writeLock().isHeldByCurrentThread();

        assert txFinishAwaitFut == null : txFinishAwaitFut;

        if (currentlyPreparedTxs.isEmpty())
            return new GridFinishedFuture<>(Collections.emptyList());

        failedToFinishInTimeoutTxs = new ConcurrentHashMap<>(currentlyPreparedTxs);

        final GridFutureAdapter<List<GridCacheVersion>> txFinishAwaitFut0 = new GridFutureAdapter<>();

        txFinishAwaitFut = txFinishAwaitFut0;

        cctx.time().addTimeoutObject(new GridTimeoutObjectAdapter(TX_FINISH_TIMEOUT) {
            @Override public void onTimeout() {
                if (txFinishAwaitFut0 == txFinishAwaitFut && !txFinishAwaitFut0.isDone())
                    txFinishAwaitFut0.onDone(U.sealList(failedToFinishInTimeoutTxs.keySet()));
            }
        });

        return txFinishAwaitFut;
    }

    /**
     * Freezes state of all tracker collections. Any active transactions that modify collections will
     * wait on readLock().
     * Can be used to obtain consistent snapshot of several collections.
     */
    public void writeLockState() {
        stateLock.writeLock().lock();
    }

    /**
     * Unfreezes state of all tracker collections, releases waiting transactions.
     */
    public void writeUnlockState() {
        stateLock.writeLock().unlock();
    }

    /**
     * @param nearXidVer Near xid version.
     * @param preparedMarkerPtr Prepared marker ptr.
     */
    public void onTxPrepared(GridCacheVersion nearXidVer, WALPointer preparedMarkerPtr) {
        stateLock.readLock().lock();

        try {
            currentlyPreparedTxs.put(nearXidVer, preparedMarkerPtr);

            if (trackPrepared.get())
                trackedPreparedTxs.put(nearXidVer, preparedMarkerPtr);
        }
        finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * @param nearXidVer Near xid version.
     */
    public void onTxCommited(GridCacheVersion nearXidVer) {
        stateLock.readLock().lock();

        try {
            WALPointer preparedPtr = currentlyPreparedTxs.remove(nearXidVer);

            assert preparedPtr != null;

            if (trackCommited.get())
                trackedCommitedTxs.put(nearXidVer, preparedPtr);

            checkTxFinishFutureDone(nearXidVer);
        }
        finally {
            stateLock.readLock().unlock();
        }

    }

    /**
     * @param nearXidVer Near xid version.
     */
    public void onTxRolledBack(GridCacheVersion nearXidVer) {
        stateLock.readLock().lock();

        try {
            currentlyPreparedTxs.remove(nearXidVer);

            if (trackPrepared.get())
                trackedPreparedTxs.remove(nearXidVer);

            checkTxFinishFutureDone(nearXidVer);
        }
        finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * @param nearXidVer Near xid version.
     * @param keys Keys.
     */
    public void onKeysWritten(GridCacheVersion nearXidVer, List<KeyCacheObject> keys) {
        stateLock.readLock().lock();

        try {
            if (!trackCommited.get())
                return;

            for (KeyCacheObject key : keys) {
                List<GridCacheVersion> keyTxs = writtenKeysToNearXidVer.computeIfAbsent(key, k -> new ArrayList<>());

                for (GridCacheVersion previousTx : keyTxs) {
                    Set<GridCacheVersion> dependentTxs = dependentTransactionsGraph.computeIfAbsent(previousTx, k -> new HashSet<>());

                    dependentTxs.add(nearXidVer);
                }

                keyTxs.add(nearXidVer);
            }
        }
        finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * @param nearXidVer Near xid version.
     * @param keys Keys.
     */
    public void onKeysRead(GridCacheVersion nearXidVer, List<KeyCacheObject> keys) {
        stateLock.readLock().lock();

        try {
            if (!trackCommited.get())
                return;

            for (KeyCacheObject key : keys) {
                List<GridCacheVersion> keyTxs = writtenKeysToNearXidVer.getOrDefault(key, Collections.emptyList());

                for (GridCacheVersion previousTx : keyTxs) {
                    Set<GridCacheVersion> dependentTxs = dependentTransactionsGraph.computeIfAbsent(previousTx, k -> new HashSet<>());

                    dependentTxs.add(nearXidVer);
                }
            }
        }
        finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * @param nearXidVer Near xid version.
     */
    private void checkTxFinishFutureDone(GridCacheVersion nearXidVer) {
        GridFutureAdapter<List<GridCacheVersion>> txFinishAwaitFut0 = txFinishAwaitFut;

        if (txFinishAwaitFut0 != null) {
            failedToFinishInTimeoutTxs.remove(nearXidVer);

            if (failedToFinishInTimeoutTxs.isEmpty()) {
                txFinishAwaitFut0.onDone(Collections.emptyList());

                txFinishAwaitFut = null;
            }
        }
    }
}
