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

package org.apache.ignite.internal.processors.cache.database;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageInitRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdateNextSnapshotId;
import org.apache.ignite.internal.pagemem.wal.record.delta.MetaPageUpdatePartitionDataRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManagerImpl;
import org.apache.ignite.internal.processors.cache.IgniteRebalanceIterator;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.database.freelist.FreeListImpl;
import org.apache.ignite.internal.processors.cache.database.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PageMetaIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.database.tree.reuse.ReuseListImpl;
import org.apache.ignite.internal.processors.cache.database.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Used when persistence enabled.
 */
public class GridCacheOffheapManager extends IgniteCacheOffheapManagerImpl implements DbCheckpointListener {
    /** */
    private MetaStore metaStore;

    /** */
    private ReuseListImpl reuseList;

    /** {@inheritDoc} */
    @Override protected void initDataStructures() throws IgniteCheckedException {
        Metas metas = getOrAllocateCacheMetas();

        RootPage reuseListRoot = metas.reuseListRoot;

        reuseList = new ReuseListImpl(cctx.cacheId(),
            cctx.name(),
            cctx.memoryPolicy().pageMemory(),
            cctx.shared().wal(),
            reuseListRoot.pageId().pageId(),
            reuseListRoot.isAllocated());

        RootPage metastoreRoot = metas.treeRoot;

        metaStore = new MetadataStorage(cctx.memoryPolicy().pageMemory(),
            cctx.shared().wal(),
            globalRemoveId(),
            cctx.cacheId(),
            PageIdAllocator.INDEX_PARTITION,
            PageIdAllocator.FLAG_IDX,
            reuseList,
            metastoreRoot.pageId().pageId(),
            metastoreRoot.isAllocated());

        if (cctx.shared().ttl().eagerTtlEnabled()) {
            final String name = "PendingEntries";

            RootPage pendingRootPage = metaStore.getOrAllocateForTree(name);

            pendingEntries = new PendingEntriesTree(
                cctx,
                name,
                cctx.memoryPolicy().pageMemory(),
                pendingRootPage.pageId().pageId(),
                reuseList,
                pendingRootPage.isAllocated()
            );
        }

        ((GridCacheDatabaseSharedManager)cctx.shared().database()).addCheckpointListener(this);
    }

    /** {@inheritDoc} */
    @Override protected CacheDataStore createCacheDataStore0(final int p)
        throws IgniteCheckedException {
        GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)cctx.shared().database();

        if (!cctx.allowFastEviction())
            dbMgr.cancelOrWaitPartitionDestroy(cctx, p);

        boolean exists = cctx.shared().pageStore() != null
            && cctx.shared().pageStore().exists(cctx.cacheId(), p);

        return new GridCacheDataStore(p, exists);
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
        assert cctx.memoryPolicy().pageMemory() instanceof PageMemoryEx;

        reuseList.saveMetadata();

        boolean metaWasUpdated = false;

        for (CacheDataStore store : partDataStores.values()) {
            RowStore rowStore = store.rowStore();

            if (rowStore == null)
                continue;

            metaWasUpdated |= saveStoreMetadata(store, ctx, !metaWasUpdated, false);
        }
    }

    /**
     * @param store Store to save metadata.
     * @throws IgniteCheckedException If failed.
     */
    private boolean saveStoreMetadata(CacheDataStore store, Context ctx, boolean saveMeta,
        boolean beforeDestroy) throws IgniteCheckedException {
        RowStore rowStore0 = store.rowStore();

        boolean beforeSnapshot = ctx != null && ctx.nextSnapshot();

        boolean wasSaveToMeta = false;

        if (rowStore0 != null) {
            FreeListImpl freeList = (FreeListImpl)rowStore0.freeList();

            freeList.saveMetadata();

            long updCntr = store.updateCounter();
            int size = store.size();
            long rmvId = globalRemoveId().get();

            PageMemoryEx pageMem = (PageMemoryEx)cctx.memoryPolicy().pageMemory();
            IgniteWriteAheadLogManager wal = cctx.shared().wal();

            if (size > 0 || updCntr > 0) {
                int state = -1;

                if (beforeDestroy)
                    state = GridDhtPartitionState.EVICTED.ordinal();
                else {
                    // localPartition will not acquire writeLock here because create=false.
                    GridDhtLocalPartition part = cctx.topology().localPartition(store.partId(),
                        AffinityTopologyVersion.NONE, false);

                    if (part != null && part.state() != GridDhtPartitionState.EVICTED)
                        state = part.state().ordinal();
                }

                // Do not save meta for evicted partitions on next checkpoints.
                if (state == -1)
                    return false;

                int cacheId = cctx.cacheId();
                long partMetaId = pageMem.partitionMetaPageId(cacheId, store.partId());
                long partMetaPage = pageMem.acquirePage(cacheId, partMetaId);

                try {
                    long pageAddr = pageMem.writeLock(cacheId, partMetaId, partMetaPage);

                    try {
                        PagePartitionMetaIO io = PageIO.getPageIO(pageAddr);

                        io.setUpdateCounter(pageAddr, updCntr);
                        io.setGlobalRemoveId(pageAddr, rmvId);
                        io.setSize(pageAddr, size);

                        io.setPartitionState(pageAddr, (byte)state);

                        int pageCount;

                        if (beforeSnapshot) {
                            pageCount = cctx.shared().pageStore().pages(cctx.cacheId(), store.partId());
                            io.setCandidatePageCount(pageAddr, pageCount);

                            if (saveMeta) {
                                long metaPageId = pageMem.metaPageId(cctx.cacheId());
                                long metaPage = pageMem.acquirePage(cctx.cacheId(), metaPageId);

                                try {
                                    long metaPageAddr = pageMem.writeLock(cctx.cacheId(), metaPageId, metaPage);

                                    try {
                                        long nextSnapshotTag = io.getNextSnapshotTag(metaPageAddr);
                                        io.setNextSnapshotTag(metaPageAddr, nextSnapshotTag + 1);

                                        if (PageHandler.isWalDeltaRecordNeeded(pageMem, cctx.cacheId(), metaPageId,
                                            metaPage, wal, null))
                                            wal.log(new MetaPageUpdateNextSnapshotId(cctx.cacheId(), metaPageId,
                                                nextSnapshotTag + 1));

                                        addPartition(ctx.partitionStatMap(), metaPageAddr, io, cctx.cacheId(), PageIdAllocator.INDEX_PARTITION,
                                            cctx.kernalContext().cache().context().pageStore().pages(cacheId, PageIdAllocator.INDEX_PARTITION));
                                    }
                                    finally {
                                        pageMem.writeUnlock(cctx.cacheId(), metaPageId, metaPage, null, true);
                                    }
                                }
                                finally {
                                    pageMem.releasePage(cctx.cacheId(), metaPageId, metaPage);
                                }

                                wasSaveToMeta = true;
                            }

                            GridDhtPartitionMap partMap = cctx.topology().localPartitionMap();

                            if (partMap.containsKey(store.partId()) &&
                                partMap.get(store.partId()) == GridDhtPartitionState.OWNING)
                                addPartition(ctx.partitionStatMap(), pageAddr, io, cctx.cacheId(), store.partId(),
                                    cctx.kernalContext().cache().context().pageStore().pages(cctx.cacheId(), store.partId()));
                        }
                        else
                            pageCount = io.getCandidatePageCount(pageAddr);

                        if (PageHandler.isWalDeltaRecordNeeded(pageMem, cacheId, partMetaId, partMetaPage, wal, null))
                            wal.log(new MetaPageUpdatePartitionDataRecord(
                                cacheId,
                                partMetaId,
                                updCntr,
                                rmvId,
                                size,
                                (byte)state,
                                pageCount
                            ));
                    }
                    finally {
                        pageMem.writeUnlock(cacheId, partMetaId, partMetaPage, null, true);
                    }
                }
                finally {
                    pageMem.releasePage(cacheId, partMetaId, partMetaPage);
                }
            }
        }

        return wasSaveToMeta;
    }

    /**
     * @param map Map to add values to.
     * @param pageAddr page address
     * @param io Page Meta IO
     * @param cacheId Cache ID.
     * @param partition Partition ID.
     * @param pages Number of pages to add.
     */
    private static void addPartition(
        Map<T2<Integer, Integer>, T2<Integer, Integer>> map,
        long pageAddr,
        PagePartitionMetaIO io,
        int cacheId,
        int partition,
        int pages
    ) {
        if (pages <= 1)
            return;

        assert PageIO.getPageId(pageAddr) != 0;

        int lastAllocatedIdx = io.getLastPageCount(pageAddr);
        map.put(new T2<>(cacheId, partition), new T2<>(lastAllocatedIdx, pages));
    }

    /** {@inheritDoc} */
    @Override protected void destroyCacheDataStore0(CacheDataStore store) throws IgniteCheckedException {
        cctx.shared().database().checkpointReadLock();

        try {
            int p = store.partId();

            saveStoreMetadata(store, null, false, true);

            PageMemoryEx pageMemory = (PageMemoryEx)cctx.memoryPolicy().pageMemory();

            int tag = pageMemory.invalidate(cctx.cacheId(), p);

            cctx.shared().pageStore().onPartitionDestroyed(cctx.cacheId(), p, tag);
        }
        finally {
            cctx.shared().database().checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onPartitionCounterUpdated(int part, long cntr) {
        CacheDataStore store = partDataStores.get(part);

        assert store != null;

        long oldCnt = store.updateCounter();

        if (oldCnt < cntr)
            store.updateCounter(cntr);
    }

    /** {@inheritDoc} */
    @Override public void onPartitionInitialCounterUpdated(int part, long cntr) {
        CacheDataStore store = partDataStores.get(part);

        assert store != null;

        long oldCnt = store.initialUpdateCounter();

        if (oldCnt < cntr)
            store.updateInitialCounter(cntr);
    }

    /** {@inheritDoc} */
    @Override public long lastUpdatedPartitionCounter(int part) {
        return partDataStores.get(part).updateCounter();
    }

    /** {@inheritDoc} */
    @Override public RootPage rootPageForIndex(String idxName) throws IgniteCheckedException {
        return metaStore.getOrAllocateForTree(idxName);
    }

    /** {@inheritDoc} */
    @Override public void dropRootPageForIndex(String idxName) throws IgniteCheckedException {
        metaStore.dropRootPage(idxName);
    }

    /** {@inheritDoc} */
    @Override public ReuseList reuseListForIndex(String idxName) {
        return reuseList;
    }

    /** {@inheritDoc} */
    @Override protected void destroyCacheDataStructures() {
        assert cctx.affinityNode();

        ((GridCacheDatabaseSharedManager)cctx.shared().database()).removeCheckpointListener(this);
    }

    /**
     * @return Meta root pages info.
     * @throws IgniteCheckedException If failed.
     */
    private Metas getOrAllocateCacheMetas() throws IgniteCheckedException {
        PageMemoryEx pageMem = (PageMemoryEx) cctx.memoryPolicy().pageMemory();
        IgniteWriteAheadLogManager wal = cctx.shared().wal();

        int cacheId = cctx.cacheId();
        long metaId = pageMem.metaPageId(cacheId);
        long metaPage = pageMem.acquirePage(cacheId, metaId);
        try {
            final long pageAddr = pageMem.writeLock(cacheId, metaId, metaPage);

            boolean allocated = false;

            try {
                long metastoreRoot, reuseListRoot;

                if (PageIO.getType(pageAddr) != PageIO.T_META) {
                    PageMetaIO pageIO = PageMetaIO.VERSIONS.latest();

                    pageIO.initNewPage(pageAddr, metaId, pageMem.pageSize());

                    metastoreRoot = pageMem.allocatePage(cacheId, PageIdAllocator.INDEX_PARTITION, PageMemory.FLAG_IDX);
                    reuseListRoot = pageMem.allocatePage(cacheId, PageIdAllocator.INDEX_PARTITION, PageMemory.FLAG_IDX);

                    pageIO.setTreeRoot(pageAddr, metastoreRoot);
                    pageIO.setReuseListRoot(pageAddr, reuseListRoot);

                    if (PageHandler.isWalDeltaRecordNeeded(pageMem, cacheId, metaId, metaPage, wal, null))
                        wal.log(new MetaPageInitRecord(
                            cacheId,
                            metaId,
                            pageIO.getType(),
                            pageIO.getVersion(),
                            metastoreRoot,
                            reuseListRoot
                        ));

                    allocated = true;
                }
                else {
                    PageMetaIO pageIO = PageIO.getPageIO(pageAddr);

                    metastoreRoot = pageIO.getTreeRoot(pageAddr);
                    reuseListRoot = pageIO.getReuseListRoot(pageAddr);

                    assert reuseListRoot != 0L;
                }

                return new Metas(
                    new RootPage(new FullPageId(metastoreRoot, cacheId), allocated),
                    new RootPage(new FullPageId(reuseListRoot, cacheId), allocated));
            }
            finally {
                pageMem.writeUnlock(cacheId, metaId, metaPage, null, allocated);
            }
        }
        finally {
            pageMem.releasePage(cacheId, metaId, metaPage);
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteRebalanceIterator rebalanceIterator(int part, AffinityTopologyVersion topVer,
        Long partCntrSince) throws IgniteCheckedException {
        if (partCntrSince == null)
            return super.rebalanceIterator(part, topVer, partCntrSince);

        GridCacheDatabaseSharedManager database = (GridCacheDatabaseSharedManager)cctx.shared().database();

        try {
            WALPointer startPtr = database.searchPartitionCounter(cctx, part, partCntrSince);

            if (startPtr == null) {
                assert false : "partCntr=" + partCntrSince + ", reservations=" + S.toString(Map.class, database.reservedForPreloading());

                return super.rebalanceIterator(part, topVer, partCntrSince);
            }

            WALIterator it = cctx.shared().wal().replay(startPtr);

            return new RebalanceIteratorAdapter(cctx, it, part);
        }
        catch (IgniteCheckedException e) {
            U.warn(log, "Failed to create WAL-based rebalance iterator (a full partition will transferred to a " +
                "remote node) [part=" + part + ", partCntrSince=" + partCntrSince + ", err=" + e + ']');

            return super.rebalanceIterator(part, topVer, partCntrSince);
        }
    }

    /**
     *
     */
    private static class RebalanceIteratorAdapter implements IgniteRebalanceIterator {
        /** Cache context. */
        private GridCacheContext cctx;

        /** WAL iterator. */
        private WALIterator walIt;

        /** Partition to scan. */
        private int part;

        /** */
        private Iterator<DataEntry> entryIt;

        /** */
        private CacheDataRow next;

        /**
         * @param cctx Cache context.
         * @param walIt WAL iterator.
         * @param part Partition ID.
         */
        private RebalanceIteratorAdapter(GridCacheContext cctx, WALIterator walIt, int part) {
            this.cctx = cctx;
            this.walIt = walIt;
            this.part = part;

            advance();
        }

        /** {@inheritDoc} */
        @Override public boolean historical() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void close() throws IgniteCheckedException {
            walIt.close();
        }

        /** {@inheritDoc} */
        @Override public boolean isClosed() {
            return walIt.isClosed();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() {
            return hasNext();
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow nextX() throws IgniteCheckedException {
            return next();
        }

        /** {@inheritDoc} */
        @Override public void removeX() throws IgniteCheckedException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public Iterator<CacheDataRow> iterator() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != null;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow next() {
            if (next == null)
                throw new NoSuchElementException();

            CacheDataRow val = next;

            advance();

            return val;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         *
         */
        private void advance() {
            next = null;

            while (true) {
                if (entryIt != null) {
                    while (entryIt.hasNext()) {
                        DataEntry entry = entryIt.next();

                        if (entry.cacheId() == cctx.cacheId() &&
                            entry.partitionId() == part) {

                            next = new DataEntryRow(entry);

                            return;
                        }
                    }
                }

                entryIt = null;

                while (walIt.hasNext()) {
                    IgniteBiTuple<WALPointer, WALRecord> rec = walIt.next();

                    if (rec.get2() instanceof DataRecord) {
                        DataRecord data = (DataRecord)rec.get2();

                        entryIt = data.writeEntries().iterator();
                        // Move on to the next valid data entry.

                        break;
                    }
                }

                if (entryIt == null)
                    return;
            }
        }
    }

    /**
     * Data entry row.
     */
    private static class DataEntryRow implements CacheDataRow {
        /** */
        private final DataEntry entry;

        /**
         * @param entry Data entry.
         */
        private DataEntryRow(DataEntry entry) {
            this.entry = entry;
        }

        /** {@inheritDoc} */
        @Override public KeyCacheObject key() {
            return entry.key();
        }

        /** {@inheritDoc} */
        @Override public void key(KeyCacheObject key) {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public CacheObject value() {
            return entry.value();
        }

        /** {@inheritDoc} */
        @Override public GridCacheVersion version() {
            return entry.writeVersion();
        }

        /** {@inheritDoc} */
        @Override public long expireTime() {
            return entry.expireTime();
        }

        /** {@inheritDoc} */
        @Override public int partition() {
            return entry.partitionId();
        }

        /** {@inheritDoc} */
        @Override public long link() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void link(long link) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public int hash() {
            return entry.key().hashCode();
        }

        /** {@inheritDoc} */
        @Override public int cacheId() {
            return entry.cacheId();
        }
    }

    /**
     *
     */
    private static class Metas {
        /** */
        @GridToStringInclude
        private final RootPage reuseListRoot;

        /** */
        @GridToStringInclude
        private final RootPage treeRoot;

        /**
         * @param treeRoot Metadata storage root.
         * @param reuseListRoot Reuse list root.
         */
        Metas(RootPage treeRoot, RootPage reuseListRoot) {
            this.treeRoot = treeRoot;
            this.reuseListRoot = reuseListRoot;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(Metas.class, this);
        }
    }

    /**
     *
     */
    private class GridCacheDataStore implements CacheDataStore {
        /** */
        private final int partId;

        /** */
        private String name;

        /** */
        private volatile FreeListImpl freeList;

        /** */
        private volatile CacheDataStore delegate;

        /** */
        private final boolean exists;

        /** */
        private final AtomicBoolean init = new AtomicBoolean();

        /** */
        private final CountDownLatch latch = new CountDownLatch(1);

        /**
         * @param partId Partition.
         * @param exists {@code True} if store for this index exists.
         */
        private GridCacheDataStore(int partId, boolean exists) {
            this.partId = partId;
            this.exists = exists;

            name = treeName(partId);
        }

        /**
         * @return Store delegate.
         * @throws IgniteCheckedException If failed.
         */
        private CacheDataStore init0(boolean checkExists) throws IgniteCheckedException {
            CacheDataStore delegate0 = delegate;

            if (delegate0 != null)
                return delegate0;

            if (checkExists) {
                if (!exists)
                    return null;
            }

            IgniteCacheDatabaseSharedManager dbMgr = cctx.shared().database();

            dbMgr.checkpointReadLock();

            if (init.compareAndSet(false, true)) {
                try {
                    Metas metas = getOrAllocatePartitionMetas();

                    RootPage reuseRoot = metas.reuseListRoot;

                    freeList = new FreeListImpl(
                        cctx.cacheId(),
                        cctx.name() + "-" + partId,
                        (MemoryMetricsImpl)cctx.memoryPolicy().memoryMetrics(),
                        cctx.memoryPolicy(),
                        null,
                        cctx.shared().wal(),
                        reuseRoot.pageId().pageId(),
                        reuseRoot.isAllocated()) {
                        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                            return pageMem.allocatePage(cacheId, partId, PageIdAllocator.FLAG_DATA);
                        }
                    };

                    CacheDataRowStore rowStore = new CacheDataRowStore(cctx, freeList, partId);

                    RootPage treeRoot = metas.treeRoot;

                    CacheDataTree dataTree = new CacheDataTree(
                        name,
                        freeList,
                        rowStore,
                        cctx,
                        treeRoot.pageId().pageId(),
                        treeRoot.isAllocated()) {
                        @Override protected long allocatePageNoReuse() throws IgniteCheckedException {
                            return pageMem.allocatePage(cacheId, partId, PageIdAllocator.FLAG_DATA);
                        }
                    };

                    PageMemoryEx pageMem = (PageMemoryEx)cctx.memoryPolicy().pageMemory();

                    delegate0 = new CacheDataStoreImpl(partId, name, rowStore, dataTree);

                    int cacheId = cctx.cacheId();
                    long partMetaId = pageMem.partitionMetaPageId(cacheId, partId);
                    long partMetaPage = pageMem.acquirePage(cacheId, partMetaId);
                    try {
                        long pageAddr = pageMem.readLock(cacheId, partMetaId, partMetaPage);

                        try {
                            if (PageIO.getType(pageAddr) != 0) {
                                PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.latest();

                                delegate0.init(io.getSize(pageAddr), io.getUpdateCounter(pageAddr));

                                cctx.offheap().globalRemoveId().setIfGreater(io.getGlobalRemoveId(pageAddr));
                            }
                        }
                        finally {
                            pageMem.readUnlock(cacheId, partMetaId, partMetaPage);
                        }
                    }
                    finally {
                        pageMem.releasePage(cacheId, partMetaId, partMetaPage);
                    }

                    delegate = delegate0;
                }
                finally {
                    latch.countDown();

                    dbMgr.checkpointReadUnlock();
                }
            }
            else {
                dbMgr.checkpointReadUnlock();

                U.await(latch);

                delegate0 = delegate;

                if (delegate0 == null)
                    throw new IgniteCheckedException("Cache store initialization failed.");
            }

            return delegate0;
        }

        /**
         * @return Partition metas.
         */
        private Metas getOrAllocatePartitionMetas() throws IgniteCheckedException {
            PageMemoryEx pageMem = (PageMemoryEx)cctx.memoryPolicy().pageMemory();
            IgniteWriteAheadLogManager wal = cctx.shared().wal();

            int cacheId = cctx.cacheId();
            long partMetaId = pageMem.partitionMetaPageId(cacheId, partId);
            long partMetaPage = pageMem.acquirePage(cacheId, partMetaId);
            try {
                boolean allocated = false;
                long pageAddr = pageMem.writeLock(cacheId, partMetaId, partMetaPage);

                try {
                    long treeRoot, reuseListRoot;

                    // Initialize new page.
                    if (PageIO.getType(pageAddr) != PageIO.T_PART_META) {
                        PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.latest();

                        io.initNewPage(pageAddr, partMetaId, pageMem.pageSize());

                        treeRoot = pageMem.allocatePage(cacheId, partId, PageMemory.FLAG_DATA);
                        reuseListRoot = pageMem.allocatePage(cacheId, partId, PageMemory.FLAG_DATA);

                        assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA;
                        assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA;

                        io.setTreeRoot(pageAddr, treeRoot);
                        io.setReuseListRoot(pageAddr, reuseListRoot);

                        if (PageHandler.isWalDeltaRecordNeeded(pageMem, cacheId, partMetaId, partMetaPage, wal, null))
                            wal.log(new MetaPageInitRecord(
                                cctx.cacheId(),
                                partMetaId,
                                io.getType(),
                                io.getVersion(),
                                treeRoot,
                                reuseListRoot
                            ));

                        allocated = true;
                    }
                    else {
                        PagePartitionMetaIO io = PageIO.getPageIO(pageAddr);

                        treeRoot = io.getTreeRoot(pageAddr);
                        reuseListRoot = io.getReuseListRoot(pageAddr);

                        assert PageIdUtils.flag(treeRoot) == PageMemory.FLAG_DATA :
                            U.hexLong(treeRoot) + ", part=" + partId + ", cacheId=" + cacheId;
                        assert PageIdUtils.flag(reuseListRoot) == PageMemory.FLAG_DATA :
                            U.hexLong(reuseListRoot) + ", part=" + partId + ", cacheId=" + cacheId;
                    }

                    return new Metas(
                        new RootPage(new FullPageId(treeRoot, cacheId), allocated),
                        new RootPage(new FullPageId(reuseListRoot, cacheId), allocated));
                }
                finally {
                    pageMem.writeUnlock(cacheId, partMetaId, partMetaPage, null, allocated);
                }
            }
            finally {
                pageMem.releasePage(cacheId, partMetaId, partMetaPage);
            }
        }

        /** {@inheritDoc} */
        @Override public int partId() {
            return partId;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public RowStore rowStore() {
            CacheDataStore delegate0 = delegate;

            return delegate0 == null ? null : delegate0.rowStore();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? 0 : delegate0.size();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long updateCounter() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? 0 : delegate0.updateCounter();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void init(long size, long updCntr) {
            throw new IllegalStateException("Should be never called.");
        }

        /** {@inheritDoc} */
        @Override public void updateCounter(long val) {
            try {
                CacheDataStore delegate0 = init0(true);

                if (delegate0 != null)
                    delegate0.updateCounter(val);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public long nextUpdateCounter() {
            try {
                CacheDataStore delegate0 = init0(false);

                return delegate0 == null ? 0 : delegate0.nextUpdateCounter();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public Long initialUpdateCounter() {
            try {
                CacheDataStore delegate0 = init0(true);

                return delegate0 == null ? 0 : delegate0.initialUpdateCounter();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void updateInitialCounter(long cntr) {
            try {
                CacheDataStore delegate0 = init0(true);

                if (delegate0 != null)
                    delegate0.updateInitialCounter(cntr);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public void update(
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow
        ) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            delegate.update(key, val, ver, expireTime, oldRow);
        }

        /** {@inheritDoc} */
        @Override public void updateIndexes(KeyCacheObject key) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            delegate.updateIndexes(key);
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow createRow(KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            long expireTime,
            @Nullable CacheDataRow oldRow) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            return delegate.createRow(key, val, ver, expireTime, oldRow);
        }

        /** {@inheritDoc} */
        @Override public void invoke(KeyCacheObject key, OffheapInvokeClosure c) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            delegate.invoke(key, c);
        }

        /** {@inheritDoc} */
        @Override public void remove(KeyCacheObject key, int partId) throws IgniteCheckedException {
            CacheDataStore delegate = init0(false);

            delegate.remove(key, partId);
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow find(KeyCacheObject key) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.find(key);

            return null;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor() throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor();

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public GridCursor<? extends CacheDataRow> cursor(KeyCacheObject lower,
            KeyCacheObject upper) throws IgniteCheckedException {
            CacheDataStore delegate = init0(true);

            if (delegate != null)
                return delegate.cursor(lower, upper);

            return EMPTY_CURSOR;
        }

        /** {@inheritDoc} */
        @Override public void destroy() throws IgniteCheckedException {
            // No need to destroy delegate.
        }
    }

    /**
     *
     */
    private static final GridCursor<CacheDataRow> EMPTY_CURSOR = new GridCursor<CacheDataRow>() {
        /** {@inheritDoc} */
        @Override public boolean next() {
            return false;
        }

        /** {@inheritDoc} */
        @Override public CacheDataRow get() {
            return null;
        }
    };
}
