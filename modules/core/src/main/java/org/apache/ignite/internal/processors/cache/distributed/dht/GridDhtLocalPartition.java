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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.GridCacheSwapEntry;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicOffHeapCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cache.extras.GridCacheObsoleteEntryExtras;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.GridCircularBuffer;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.LongAdder8;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.RENTING;

/**
 * Key partition.
 */
public class GridDhtLocalPartition implements Comparable<GridDhtLocalPartition>, GridReservable {
    /** Maximum size for delete queue. */
    public static final int MAX_DELETE_QUEUE_SIZE = Integer.getInteger(IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE,
        200_000);

    /** Static logger to avoid re-creation. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static volatile IgniteLogger log;

    /** Partition ID. */
    private final int id;

    /** State. */
    @GridToStringExclude
    private final AtomicLong state = new AtomicLong((long)MOVING.ordinal() << 32);

    /** Rent future. */
    @GridToStringExclude
    private final GridFutureAdapter<?> rent;

    /** Entries map. */
    private final GridCacheConcurrentMap map;

    /** Context. */
    private final GridCacheContext cctx;

    /** Create time. */
    @GridToStringExclude
    private final long createTime = U.currentTimeMillis();

    /** Eviction history. */
    private volatile Map<KeyCacheObject, GridCacheVersion> evictHist = new HashMap<>();

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Public size counter. */
    private final LongAdder8 mapPubSize = new LongAdder8();

    /** Remove queue. */
    private final GridCircularBuffer<T2<KeyCacheObject, GridCacheVersion>> rmvQueue;

    /** Group reservations. */
    private final CopyOnWriteArrayList<GridDhtPartitionsReservation> reservations = new CopyOnWriteArrayList<>();

    /** Update counter. */
    private final AtomicLong cntr = new AtomicLong();

    /**
     * @param cctx Context.
     * @param id Partition ID.
     */
    @SuppressWarnings("ExternalizableWithoutPublicNoArgConstructor") GridDhtLocalPartition(GridCacheContext cctx,
        int id) {
        assert cctx != null;

        this.id = id;
        this.cctx = cctx;

        log = U.logger(cctx.kernalContext(), logRef, this);

        rent = new GridFutureAdapter<Object>() {
            @Override public String toString() {
                return "PartitionRentFuture [part=" + GridDhtLocalPartition.this + ", map=" + map + ']';
            }
        };

        map = new GridCacheConcurrentMap(cctx, cctx.config().getStartSize() / cctx.affinity().partitions(), null);

        map.setEntryFactory(new GridCacheMapEntryFactory() {
            /** {@inheritDoc} */
            @Override public GridCacheMapEntry create(
                GridCacheContext ctx,
                AffinityTopologyVersion topVer,
                KeyCacheObject key,
                int hash,
                CacheObject val
            ) {
                if (ctx.useOffheapEntry())
                    return new GridDhtAtomicOffHeapCacheEntry(ctx, topVer, key, hash, val);

                return new GridDhtAtomicCacheEntry(ctx, topVer, key, hash, val);
            }
        });

        int delQueueSize = CU.isSystemCache(cctx.name()) ? 100 :
            Math.max(MAX_DELETE_QUEUE_SIZE / cctx.affinity().partitions(), 20);

        rmvQueue = new GridCircularBuffer<>(U.ceilPow2(delQueueSize));
    }

    /**
     * Adds group reservation to this partition.
     *
     * @param r Reservation.
     * @return {@code false} If such reservation already added.
     */
    public boolean addReservation(GridDhtPartitionsReservation r) {
        assert GridDhtPartitionState.fromOrdinal((int)(state.get() >> 32)) != EVICTED :
            "we can reserve only active partitions";
        assert (state.get() & 0xFFFF) != 0 : "partition must be already reserved before adding group reservation";

        return reservations.addIfAbsent(r);
    }

    /**
     * @param r Reservation.
     */
    public void removeReservation(GridDhtPartitionsReservation r) {
        if (!reservations.remove(r))
            throw new IllegalStateException("Reservation was already removed.");
    }

    /**
     * @return Partition ID.
     */
    public int id() {
        return id;
    }

    /**
     * @return Create time.
     */
    long createTime() {
        return createTime;
    }

    /**
     * @return Partition state.
     */
    public GridDhtPartitionState state() {
        return GridDhtPartitionState.fromOrdinal((int)(state.get() >> 32));
    }

    /**
     * @return Reservations.
     */
    public int reservations() {
        return (int)(state.get() & 0xFFFF);
    }

    /**
     * @return Keys belonging to partition.
     */
    public Set<KeyCacheObject> keySet() {
        return map.keySet();
    }

    /**
     * @return Entries belonging to partition.
     */
    public Collection<GridDhtCacheEntry> entries() {
        return map.values();
    }

    /**
     * @return {@code True} if partition is empty.
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * @return Number of entries in this partition (constant-time method).
     */
    public int size() {
        return map.size();
    }

    /**
     * Increments public size of the map.
     */
    public void incrementPublicSize(GridCacheMapEntry e) {
        mapPubSize.increment();

        map.incrementSize(e);
    }

    /**
     * Decrements public size of the map.
     */
    public void decrementPublicSize(GridCacheMapEntry e) {
        mapPubSize.decrement();

        map.decrementSize(e);
    }

    /**
     * @return Number of public (non-internal) entries in this partition.
     */
    public int publicSize() {
        return mapPubSize.intValue();
    }

    /**
     * @return If partition is moving or owning or renting.
     */
    public boolean valid() {
        GridDhtPartitionState state = state();

        return state == MOVING || state == OWNING || state == RENTING;
    }

    @Nullable public GridCacheMapEntry getEntry(Object key) {
        return map.getEntry(key);
    }

    public GridTriple<GridCacheMapEntry> putEntryIfObsoleteOrAbsent(
        AffinityTopologyVersion topVer, KeyCacheObject key,
        @Nullable CacheObject val, boolean create) {
        return map.putEntryIfObsoleteOrAbsent(topVer, key, val, create);
    }

    public boolean removeEntry(GridCacheEntryEx e) {
        return map.removeEntry(e);
    }

    public Set<GridCacheEntryEx> entries0() {
        return map.entries0();
    }

    public Set<GridCacheEntryEx> allEntries0() {
        return map.allEntries0();
    }

    public <K, V> Set<Cache.Entry<K, V>> entriesx(CacheEntryPredicate... filter) {
        return map.entriesx(filter);
    }

    public <K, V> Set<Cache.Entry<K, V>> entries(CacheEntryPredicate... filter) {
        return map.entries(filter);
    }

    public <K, V> Set<K> keySet(CacheEntryPredicate... filter) {
        return map.keySet(filter);
    }

    public <K, V> Set<K> keySetx(CacheEntryPredicate... filter) {
        return map.keySetx(filter);
    }

    @Nullable public GridCacheMapEntry removeEntryIfObsolete(KeyCacheObject key) {
        return map.removeEntryIfObsolete(key);
    }

    public <K, V> Collection<V> values(CacheEntryPredicate... filter) {
        return map.values(filter);
    }

    /**
     * @param entry Entry to add.
     */
    void onAdded(AffinityTopologyVersion topVer, GridDhtCacheEntry entry) {
        GridDhtPartitionState state = state();

        if (state == EVICTED)
            throw new GridDhtInvalidPartitionException(id, "Adding entry to invalid partition " +
                "(often may be caused by inconsistent 'key.hashCode()' implementation) [part=" + id + ']');

        try {
            CacheObject value = entry.hasValue() ? entry.versionedValue(topVer).get2() : null;
            map.putEntryIfObsoleteOrAbsent(topVer, entry.key(), value, false);
        }
        catch (GridCacheEntryRemovedException e) {
            throw new RuntimeException(e);
        }

        if (!entry.isInternal()) {
            assert !entry.deleted() : entry;

            mapPubSize.increment();
        }
    }

    /**
     * @param entry Entry to remove.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter") void onRemoved(GridDhtCacheEntry entry) {
        assert entry.obsolete() : entry;

        // Make sure to remove exactly this entry.
        synchronized (entry) {
            map.removeEntry(entry);

            if (!entry.isInternal() && !entry.deleted())
                mapPubSize.decrement();
        }

        // Attempt to evict.
        tryEvict();
    }

    /**
     * @param key Removed key.
     * @param ver Removed version.
     * @throws IgniteCheckedException If failed.
     */
    public void onDeferredDelete(KeyCacheObject key, GridCacheVersion ver) throws IgniteCheckedException {
        try {
            T2<KeyCacheObject, GridCacheVersion> evicted = rmvQueue.add(new T2<>(key, ver));

            if (evicted != null)
                cctx.dht().removeVersionedEntry(evicted.get1(), evicted.get2());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInterruptedCheckedException(e);
        }
    }

    /**
     * Locks partition.
     */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    public void lock() {
        lock.lock();
    }

    /**
     * Unlocks partition.
     */
    public void unlock() {
        lock.unlock();
    }

    /**
     * @param key Key.
     * @param ver Version.
     */
    public void onEntryEvicted(KeyCacheObject key, GridCacheVersion ver) {
        assert key != null;
        assert ver != null;
        assert lock.isHeldByCurrentThread(); // Only one thread can enter this method at a time.

        if (state() != MOVING)
            return;

        Map<KeyCacheObject, GridCacheVersion> evictHist0 = evictHist;

        if (evictHist0 != null) {
            GridCacheVersion ver0 = evictHist0.get(key);

            if (ver0 == null || ver0.isLess(ver)) {
                GridCacheVersion ver1 = evictHist0.put(key, ver);

                assert ver1 == ver0;
            }
        }
    }

    /**
     * Cache preloader should call this method within partition lock.
     *
     * @param key Key.
     * @param ver Version.
     * @return {@code True} if preloading is permitted.
     */
    public boolean preloadingPermitted(KeyCacheObject key, GridCacheVersion ver) {
        assert key != null;
        assert ver != null;
        assert lock.isHeldByCurrentThread(); // Only one thread can enter this method at a time.

        if (state() != MOVING)
            return false;

        Map<KeyCacheObject, GridCacheVersion> evictHist0 = evictHist;

        if (evictHist0 != null) {
            GridCacheVersion ver0 = evictHist0.get(key);

            // Permit preloading if version in history
            // is missing or less than passed in.
            return ver0 == null || ver0.isLess(ver);
        }

        return false;
    }

    /**
     * Reserves a partition so it won't be cleared.
     *
     * @return {@code True} if reserved.
     */
    @Override public boolean reserve() {
        while (true) {
            long reservations = state.get();

            if ((int)(reservations >> 32) == EVICTED.ordinal())
                return false;

            if (state.compareAndSet(reservations, reservations + 1))
                return true;
        }
    }

    /**
     * Releases previously reserved partition.
     */
    @Override public void release() {
        while (true) {
            long reservations = state.get();

            if ((int)(reservations & 0xFFFF) == 0)
                return;

            assert (int)(reservations >> 32) != EVICTED.ordinal();

            // Decrement reservations.
            if (state.compareAndSet(reservations, --reservations)) {
                tryEvict();

                break;
            }
        }
    }

    /**
     * @param reservations Current aggregated value.
     * @param toState State to switch to.
     * @return {@code true} if cas succeeds.
     */
    private boolean casState(long reservations, GridDhtPartitionState toState) {
        return state.compareAndSet(reservations, (reservations & 0xFFFF) | ((long)toState.ordinal() << 32));
    }

    /**
     * @return {@code True} if transitioned to OWNING state.
     */
    boolean own() {
        while (true) {
            long reservations = state.get();

            int ord = (int)(reservations >> 32);

            if (ord == RENTING.ordinal() || ord == EVICTED.ordinal())
                return false;

            if (ord == OWNING.ordinal())
                return true;

            assert ord == MOVING.ordinal();

            if (casState(reservations, OWNING)) {
                if (log.isDebugEnabled())
                    log.debug("Owned partition: " + this);

                // No need to keep history any more.
                evictHist = null;

                return true;
            }
        }
    }

    /**
     * @param updateSeq Update sequence.
     * @return Future to signal that this node is no longer an owner or backup.
     */
    IgniteInternalFuture<?> rent(boolean updateSeq) {
        while (true) {
            long reservations = state.get();

            int ord = (int)(reservations >> 32);

            if (ord == RENTING.ordinal() || ord == EVICTED.ordinal())
                return rent;

            if (casState(reservations, RENTING)) {
                if (log.isDebugEnabled())
                    log.debug("Moved partition to RENTING state: " + this);

                // Evict asynchronously, as the 'rent' method may be called
                // from within write locks on local partition.
                tryEvictAsync(updateSeq);

                break;
            }
        }

        return rent;
    }

    /**
     * @param updateSeq Update sequence.
     */
    void tryEvictAsync(boolean updateSeq) {
        long reservations = state.get();

        int ord = (int)(reservations >> 32);

        if (map.isEmpty() && !GridQueryProcessor.isEnabled(cctx.config()) &&
            ord == RENTING.ordinal() && (reservations & 0xFFFF) == 0 &&
            casState(reservations, EVICTED)) {
            if (log.isDebugEnabled())
                log.debug("Evicted partition: " + this);

            clearSwap();

            if (cctx.isDrEnabled())
                cctx.dr().partitionEvicted(id);

            cctx.dataStructures().onPartitionEvicted(id);

            rent.onDone();

            ((GridDhtPreloader)cctx.preloader()).onPartitionEvicted(this, updateSeq);

            clearDeferredDeletes();
        }
        else
            cctx.preloader().evictPartitionAsync(this);
    }

    /**
     * @return {@code true} If there is a group reservation.
     */
    private boolean groupReserved() {
        for (GridDhtPartitionsReservation reservation : reservations) {
            if (!reservation.invalidate())
                return true; // Failed to invalidate reservation -> we are reserved.
        }

        return false;
    }

    /**
     *
     */
    public void tryEvict() {
        long reservations = state.get();

        int ord = (int)(reservations >> 32);

        if (ord != RENTING.ordinal() || (reservations & 0xFFFF) != 0 || groupReserved())
            return;

        // Attempt to evict partition entries from cache.
        clearAll();

        if (map.isEmpty() && casState(reservations, EVICTED)) {
            if (log.isDebugEnabled())
                log.debug("Evicted partition: " + this);

            if (!GridQueryProcessor.isEnabled(cctx.config()))
                clearSwap();

            if (cctx.isDrEnabled())
                cctx.dr().partitionEvicted(id);

            cctx.continuousQueries().onPartitionEvicted(id);

            cctx.dataStructures().onPartitionEvicted(id);

            rent.onDone();

            ((GridDhtPreloader)cctx.preloader()).onPartitionEvicted(this, true);

            clearDeferredDeletes();
        }
    }

    /**
     * Clears swap entries for evicted partition.
     */
    private void clearSwap() {
        assert state() == EVICTED;
        assert !GridQueryProcessor.isEnabled(cctx.config()) : "Indexing needs to have unswapped values.";

        try {
            GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry>> it = cctx.swap().iterator(id);

            boolean isLocStore = cctx.store().isLocal();

            if (it != null) {
                // We can safely remove these values because no entries will be created for evicted partition.
                while (it.hasNext()) {
                    Map.Entry<byte[], GridCacheSwapEntry> entry = it.next();

                    byte[] keyBytes = entry.getKey();

                    KeyCacheObject key = cctx.toCacheKeyObject(keyBytes);

                    cctx.swap().remove(key);

                    if (isLocStore)
                        cctx.store().remove(null, key.value(cctx.cacheObjectContext(), false));
                }
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to clear swap for evicted partition: " + this, e);
        }
    }

    /**
     *
     */
    void onUnlock() {
        tryEvict();
    }

    /**
     * @param topVer Topology version.
     * @return {@code True} if local node is primary for this partition.
     */
    public boolean primary(AffinityTopologyVersion topVer) {
        return cctx.affinity().primary(cctx.localNode(), id, topVer);
    }

    /**
     * @param topVer Topology version.
     * @return {@code True} if local node is backup for this partition.
     */
    public boolean backup(AffinityTopologyVersion topVer) {
        return cctx.affinity().backup(cctx.localNode(), id, topVer);
    }

    /**
     * @return Next update index.
     */
    public long nextUpdateCounter() {
        return cntr.incrementAndGet();
    }

    /**
     * @return Current update index.
     */
    public long updateCounter() {
        return cntr.get();
    }

    /**
     * @param val Update index value.
     */
    public void updateCounter(long val) {
        while (true) {
            long val0 = cntr.get();

            if (val0 >= val)
                break;

            if (cntr.compareAndSet(val0, val))
                break;
        }
    }

    /**
     * Clears values for this partition.
     */
    private void clearAll() {
        GridCacheVersion clearVer = cctx.versions().next();

        boolean swap = cctx.isSwapOrOffheapEnabled();

        boolean rec = cctx.events().isRecordable(EVT_CACHE_REBALANCE_OBJECT_UNLOADED);

        Iterator<GridCacheEntryEx> it = map.entries0().iterator();

        GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry>> swapIt = null;

        if (swap && GridQueryProcessor.isEnabled(cctx.config())) { // Indexing needs to unswap cache values.
            Iterator<GridCacheEntryEx> unswapIt = null;

            try {
                swapIt = cctx.swap().iterator(id);
                unswapIt = unswapIterator(swapIt);
            }
            catch (Exception e) {
                U.error(log, "Failed to clear swap for evicted partition: " + this, e);
            }

            if (unswapIt != null)
                it = F.concat(it, unswapIt);
        }

        GridCacheObsoleteEntryExtras extras = new GridCacheObsoleteEntryExtras(clearVer);

        try {
            while (it.hasNext()) {
                GridCacheEntryEx cached = null;

                try {
                    cached = it.next();

                    if (!(cached instanceof GridDhtCacheEntry))
                        continue;

                    if (((GridDhtCacheEntry)cached).clearInternal(clearVer, swap, extras)) {
                        map.removeEntry(cached);

                        if (!cached.isInternal()) {
                            mapPubSize.decrement();

                            if (rec) {
                                cctx.events().addEvent(cached.partition(),
                                    cached.key(),
                                    cctx.localNodeId(),
                                    (IgniteUuid)null,
                                    null,
                                    EVT_CACHE_REBALANCE_OBJECT_UNLOADED,
                                    null,
                                    false,
                                    cached.rawGet(),
                                    cached.hasValue(),
                                    null,
                                    null,
                                    null,
                                    false);
                            }
                        }
                    }
                }
                catch (GridDhtInvalidPartitionException e) {
                    assert map.isEmpty() && state() == EVICTED : "Invalid error [e=" + e + ", part=" + this + ']';
                    assert swapEmpty() : "Invalid error when swap is not cleared [e=" + e + ", part=" + this + ']';

                    break; // Partition is already concurrently cleared and evicted.
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to clear cache entry for evicted partition: " + cached, e);
                }
            }
        }
        finally {
            U.close(swapIt, log);
        }
    }

    /**
     * @return {@code True} if there are no swap entries for this partition.
     */
    private boolean swapEmpty() {
        GridCloseableIterator<?> it0 = null;

        try {
            it0 = cctx.swap().iterator(id);

            return it0 == null || !it0.hasNext();
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to get partition swap iterator: " + this, e);

            return true;
        }
        finally {
            if (it0 != null)
                U.closeQuiet(it0);
        }
    }

    /**
     * @param it Swap iterator.
     * @return Unswapping iterator over swapped entries.
     */
    private Iterator<GridCacheEntryEx> unswapIterator(
        final GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry>> it) {
        if (it == null)
            return null;

        return new Iterator<GridCacheEntryEx>() {
            /** */
            GridDhtCacheEntry lastEntry;

            @Override public boolean hasNext() {
                return it.hasNext();
            }

            @Override public GridDhtCacheEntry next() {
                Map.Entry<byte[], GridCacheSwapEntry> entry = it.next();

                byte[] keyBytes = entry.getKey();

                while (true) {
                    try {
                        KeyCacheObject key = cctx.toCacheKeyObject(keyBytes);

                        lastEntry = (GridDhtCacheEntry)cctx.cache().entryEx(key, false);

                        lastEntry.unswap(true);

                        return lastEntry;
                    }
                    catch (GridCacheEntryRemovedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Got removed entry: " + lastEntry);
                    }
                    catch (IgniteCheckedException e) {
                        throw new CacheException(e);
                    }
                }
            }

            @Override public void remove() {
                map.removeEntry(lastEntry);
            }
        };
    }

    /**
     *
     */
    private void clearDeferredDeletes() {
        rmvQueue.forEach(new CI1<T2<KeyCacheObject, GridCacheVersion>>() {
            @Override public void apply(T2<KeyCacheObject, GridCacheVersion> t) {
                cctx.dht().removeVersionedEntry(t.get1(), t.get2());
            }
        });
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"OverlyStrongTypeCast"})
    @Override public boolean equals(Object obj) {
        return obj instanceof GridDhtLocalPartition && (obj == this || ((GridDhtLocalPartition)obj).id() == id);
    }

    /** {@inheritDoc} */
    @Override public int compareTo(@NotNull GridDhtLocalPartition part) {
        if (part == null)
            return 1;

        return Integer.compare(id, part.id());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtLocalPartition.class, this,
            "state", state(),
            "reservations", reservations(),
            "empty", map.isEmpty(),
            "createTime", U.format(createTime),
            "mapPubSize", mapPubSize);
    }
}
