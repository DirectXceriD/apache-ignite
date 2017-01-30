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

package org.apache.ignite.internal.visor.cache;

import java.io.Serializable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for {@link CacheMetrics}.
 */
public class VisorCacheMetrics implements Serializable, LessNamingBean {
    /** */
    private static final float MICROSECONDS_IN_SECOND = 1_000_000;

    /** */
    private static final long serialVersionUID = 0L;

    /** Cache name. */
    private String name;

    /** Cache mode. */
    private CacheMode mode;

    /** Cache system state. */
    private boolean sys;

    /** Number of non-{@code null} values in the cache. */
    private int size;

    /** Number of primary entries stored in off-heap memory. */
    private long offHeapPrimarySize;

    /** Number of backup entries stored in off-heap memory. */
    private long offHeapBackupSize;

    /** Size of swap. */
    private long swapSize;

    /** Gets number of keys in the cache, possibly with {@code null} values. */
    private int keySize;

//    private int swapKeys;

    /** Total number of reads of the owning entity (either cache or entry). */
    private long reads;

    /** The total number of get requests to the off-heap memory. */
    private long offHeapReads;

    /** The total number of get requests to the swap. */
    private long swapReads;

    /** The mean time to execute gets. */
    private float avgReadTime;

    /** Total number of writes of the owning entity (either cache or entry). */
    private long writes;

    /** Total number of writes of the owning entity (either cache or entry) from the off-heap memory. */
    private long offHeapWrites;

    /** Total number of writes of the owning entity (either cache or entry) from the swap. */
    private long swapWrites;

    /** Total number of hits for the owning entity (either cache or entry). */
    private long hits;

    /** The number of get requests that were satisfied by the off-heap memory. */
    private long offHeapHits;

    /** The number of get requests that were satisfied by the swap. */
    private long swapHits;

    /** Total number of misses for the owning entity (either cache or entry). */
    private long misses;

    /** A miss is a get request that is not satisfied by off-heap memory. */
    private long offHeapMisses;

    /** A miss is a get request that is not satisfied by swap. */
    private long swapMisses;

    /** Total number of transaction commits. */
    private long txCommits;

    /** The mean time to execute tx commit. */
    private float avgTxCommitTime;

    /** Total number of transaction rollbacks. */
    private long txRollbacks;

    /** The mean time to execute tx rollbacks. */
    private float avgTxRollbackTime;

    /** The total number of puts to the cache. */
    private long puts;

    /** The total number of put requests to the off-heap memory. */
    private long offHeapPuts;

    /** The total number of put requests to the swap. */
    private long swapPuts;

    /** The mean time to execute puts. */
    private float avgPutTime;

    /** The total number of removals from the cache. */
    private long removals;

    /** The total number of removals from the off-heap memory. This does not include evictions. */
    private long offHeapRemovals;

    /** The total number of removals from the swap. */
    private long swapRemovals;

    /** The mean time to execute removes. */
    private float avgRemovalTime;

    /** The total number of evictions from the cache. */
    private long evictions;

    /** The total number of evictions from the off-heap memory. */
    private long offHeapEvictions;

    /** Reads per second. */
    private int readsPerSec;

    /** Puts per second. */
    private int putsPerSec;

    /** Removes per second. */
    private int removalsPerSec;

    /** Commits per second. */
    private int commitsPerSec;

    /** Rollbacks per second. */
    private int rollbacksPerSec;

    /** Gets query metrics for cache. */
    private VisorCacheQueryMetrics qryMetrics;

    /** Current size of evict queue used to batch up evictions. */
    private int dhtEvictQueueCurrSize;

    /** Gets transaction per-thread map size. */
    private int txThreadMapSize;

    /** Transaction per-Xid map size. */
    private int txXidMapSize;

    /** Committed transaction queue size. */
    private int txCommitQueueSize;

    /** Prepared transaction queue size. */
    private int txPrepareQueueSize;

    /** Start version counts map size. */
    private int txStartVerCountsSize;

    /** Number of cached committed transaction IDs. */
    private int txCommittedVersionsSize;

    /** Number of cached rolled back transaction IDs. */
    private int txRolledbackVersionsSize;

    /** DHT thread map size */
    private int txDhtThreadMapSize;

    /** Transaction DHT per-Xid map size. */
    private int txDhtXidMapSize;

    /** Committed DHT transaction queue size. */
    private int txDhtCommitQueueSize;

    /** Prepared DHT transaction queue size. */
    private int txDhtPrepareQueueSize;

    /** DHT start version counts map size. */
    private int txDhtStartVerCountsSize;

    /** Number of cached committed DHT transaction IDs. */
    private int txDhtCommittedVersionsSize;

    /** Number of cached rolled back DHT transaction IDs. */
    private int txDhtRolledbackVersionsSize;

    /**
     * Calculate rate of metric per second.
     *
     * @param meanTime Metric mean time.
     * @return Metric per second.
     */
    private static int perSecond(float meanTime) {
        return (meanTime > 0) ? (int)(MICROSECONDS_IN_SECOND / meanTime) : 0;
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     * @return Data transfer object for given cache metrics.
     */
    public VisorCacheMetrics from(IgniteEx ignite, String cacheName) {
        GridCacheProcessor cacheProcessor = ignite.context().cache();

        IgniteCache<Object, Object> c = cacheProcessor.jcache(cacheName);

        name = cacheName;
        mode = cacheProcessor.cacheMode(cacheName);
        sys = cacheProcessor.systemCache(cacheName);

        CacheMetrics m = c.localMetrics();

        size = m.getSize();
        offHeapPrimarySize = m.getOffHeapPrimaryEntriesCount();
        offHeapBackupSize = m.getOffHeapBackupEntriesCount();
        swapSize = m.getSwapSize();
        keySize = m.getKeySize();

        reads = m.getCacheGets();
        offHeapReads = m.getOffHeapGets();
        swapReads = m.getSwapGets();
        writes = m.getCachePuts() + m.getCacheRemovals();
        offHeapWrites = m.getOffHeapPuts() + m.getOffHeapRemovals();
        swapWrites = m.getSwapPuts() + m.getSwapRemovals();
        hits = m.getCacheHits();
        offHeapHits = m.getOffHeapHits();
        swapHits = m.getOffHeapHits();
        misses = m.getCacheMisses();
        offHeapMisses = m.getOffHeapMisses();
        swapMisses = m.getSwapMisses();

        txCommits = m.getCacheTxCommits();
        txRollbacks = m.getCacheTxRollbacks();

        avgTxCommitTime = m.getAverageTxCommitTime();
        avgTxRollbackTime = m.getAverageTxRollbackTime();

        puts = m.getCachePuts();
        offHeapPuts = m.getOffHeapPuts();
        swapPuts = m.getSwapPuts();
        removals = m.getCacheRemovals();
        offHeapRemovals = m.getOffHeapRemovals();
        swapRemovals = m.getSwapRemovals();
        evictions = m.getCacheEvictions();
        offHeapEvictions = m.getOffHeapEvictions();

        avgReadTime = m.getAverageGetTime();
        avgPutTime = m.getAveragePutTime();
        avgRemovalTime = m.getAverageRemoveTime();

        readsPerSec = perSecond(m.getAverageGetTime());
        putsPerSec = perSecond(m.getAveragePutTime());
        removalsPerSec = perSecond(m.getAverageRemoveTime());
        commitsPerSec = perSecond(m.getAverageTxCommitTime());
        rollbacksPerSec = perSecond(m.getAverageTxRollbackTime());

        qryMetrics = VisorCacheQueryMetrics.from(c.queryMetrics());

        dhtEvictQueueCurrSize = m.getDhtEvictQueueCurrentSize();
        txThreadMapSize = m.getTxThreadMapSize();
        txXidMapSize = m.getTxXidMapSize();
        txCommitQueueSize = m.getTxCommitQueueSize();
        txPrepareQueueSize = m.getTxPrepareQueueSize();
        txStartVerCountsSize = m.getTxStartVersionCountsSize();
        txCommittedVersionsSize = m.getTxCommittedVersionsSize();
        txRolledbackVersionsSize = m.getTxRolledbackVersionsSize();
        txDhtThreadMapSize = m.getTxDhtThreadMapSize();
        txDhtXidMapSize = m.getTxDhtXidMapSize();
        txDhtCommitQueueSize = m.getTxDhtCommitQueueSize();
        txDhtPrepareQueueSize = m.getTxDhtPrepareQueueSize();
        txDhtStartVerCountsSize = m.getTxDhtStartVersionCountsSize();
        txDhtCommittedVersionsSize = m.getTxDhtCommittedVersionsSize();
        txDhtRolledbackVersionsSize = m.getTxDhtRolledbackVersionsSize();

        return this;
    }

    /**
     * @return Cache name.
     */
    public String name() {
        return name;
    }

    /**
     * Sets cache name.
     *
     * @param name New value for cache name.
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * @return Cache mode.
     */
    public CacheMode mode() {
        return mode;
    }

    /**
     * @return Cache system state.
     */
    public boolean system() {
        return sys;
    }

    /**
     * @return Total number of reads of the owning entity (either cache or entry).
     */
    public long reads() {
        return reads;
    }

    /**
     * @return The total number of get requests to the off-heap memory.
     */
    public long offHeapReads() {
        return offHeapReads;
    }

    /**
     * @return The total number of get requests to the swap.
     */
    public long swapReads() {
        return swapReads;
    }

    /**
     * @return The mean time to execute gets
     */
    public float avgReadTime() {
        return avgReadTime;
    }

    /**
     * @return Total number of writes of the owning entity (either cache or entry).
     */
    public long writes() {
        return writes;
    }

    /**
     * @return Total number of writes of the owning entity (either cache or entry) from the off-heap memory.
     */
    public long offHeapWrites() {
        return offHeapWrites;
    }

    /**
     * @return Total number of writes of the owning entity (either cache or entry) from the swap.
     */
    public long swapWrites() {
        return swapWrites;
    }

    /**
     * @return Total number of hits for the owning entity (either cache or entry).
     */
    public long hits() {
        return hits;
    }

    /**
     * @return The number of get requests that were satisfied by the off-heap memory.
     */
    public long offHeapHits() {
        return offHeapHits;
    }

    /**
     * @return The number of get requests that were satisfied by the swap.
     */
    public long swapHits() {
        return swapHits;
    }

    /**
     * @return Total number of misses for the owning entity (either cache or entry).
     */
    public long misses() {
        return misses;
    }

    /**
     * @return A miss is a get request that is not satisfied by off-heap memory.
     */
    public long offHeapMisses() {
        return offHeapMisses;
    }

    /**
     * @return A miss is a get request that is not satisfied by swap.
     */
    public long swapMisses() {
        return swapMisses;
    }

    /**
     * @return Total number of transaction commits.
     */
    public long txCommits() {
        return txCommits;
    }

    /**
     * @return avgTxCommitTime
     */
    public float avgTxCommitTime() {
        return avgTxCommitTime;
    }

    /**
     * @return The mean time to execute tx rollbacks.
     */
    public float avgTxRollbackTime() {
        return avgTxRollbackTime;
    }

    /**
     * @return The total number of puts to the cache.
     */
    public long puts() {
        return puts;
    }

    /**
     * @return The total number of put requests to the off-heap memory.
     */
    public long offHeapPuts() {
        return offHeapPuts;
    }

    /**
     * @return The total number of put requests to the swap.
     */
    public long swapPuts() {
        return swapPuts;
    }

    /**
     * @return The mean time to execute puts.
     */
    public float avgPutTime() {
        return avgPutTime;
    }

    /**
     * @return The total number of removals from the cache.
     */
    public long removals() {
        return removals;
    }

    /**
     * @return The total number of removals from the off-heap memory. This does not include evictions.
     */
    public long offHeapRemovals() {
        return offHeapRemovals;
    }

    /**
     * @return The total number of removals from the swap.
     */
    public long swapRemovals() {
        return swapRemovals;
    }

    /**
     * @return The mean time to execute removes.
     */
    public float avgRemovalTime() {
        return avgRemovalTime;
    }

    /**
     * @return The total number of evictions from the cache.
     */
    public long evictions() {
        return evictions;
    }

    /**
     * @return The total number of evictions from the off-heap memory.
     */
    public long offHeapEvictions() {
        return offHeapEvictions;
    }

    /**
     * @return Total number of transaction rollbacks.
     */
    public long txRollbacks() {
        return txRollbacks;
    }

    /**
     * @return Reads per second.
     */
    public int readsPerSecond() {
        return readsPerSec;
    }

    /**
     * @return Puts per second.
     */
    public int putsPerSecond() {
        return putsPerSec;
    }

    /**
     * @return Removes per second.
     */
    public int removalsPerSecond() {
        return removalsPerSec;
    }

    /**
     * @return Commits per second.
     */
    public int commitsPerSecond() {
        return commitsPerSec;
    }

    /**
     * @return Rollbacks per second.
     */
    public int rollbacksPerSecond() {
        return rollbacksPerSec;
    }

    /**
     * @return Number of non-{@code null} values in the cache.
     */
    public int size() {
        return size;
    }

    /**
     * @return Number of primary entries stored in off-heap memory.
     */
    public long offHeapPrimarySize() {
        return offHeapPrimarySize;
    }

    /**
     * @return Number of backup entries stored in off-heap memory.
     */
    public long offHeapBackupSize() {
        return offHeapBackupSize;
    }

    /**
     * @return Size of swap.
     */
    public long swapSize() {
        return swapSize;
    }

    /**
     * @return Gets number of keys in the cache, possibly with {@code null} values.
     */
    public int keySize() {
        return keySize;
    }

    /**
     * @return Gets query metrics for cache.
     */
    public VisorCacheQueryMetrics queryMetrics() {
        return qryMetrics;
    }

    /**
     * @return Current size of evict queue used to batch up evictions.
     */
    public int dhtEvictQueueCurrentSize() {
        return dhtEvictQueueCurrSize;
    }

    /**
     * @return Gets transaction per-thread map size.
     */
    public int txThreadMapSize() {
        return txThreadMapSize;
    }

    /**
     * @return Transaction per-Xid map size.
     */
    public int txXidMapSize() {
        return txXidMapSize;
    }

    /**
     * @return Committed transaction queue size.
     */
    public int txCommitQueueSize() {
        return txCommitQueueSize;
    }

    /**
     * @return Prepared transaction queue size.
     */
    public int txPrepareQueueSize() {
        return txPrepareQueueSize;
    }

    /**
     * @return Start version counts map size.
     */
    public int txStartVersionCountsSize() {
        return txStartVerCountsSize;
    }

    /**
     * @return Number of cached committed transaction IDs.
     */
    public int txCommittedVersionsSize() {
        return txCommittedVersionsSize;
    }

    /**
     * @return Number of cached rolled back transaction IDs.
     */
    public int txRolledbackVersionsSize() {
        return txRolledbackVersionsSize;
    }

    /**
     * @return DHT thread map size
     */
    public int txDhtThreadMapSize() {
        return txDhtThreadMapSize;
    }

    /**
     * @return Transaction DHT per-Xid map size.
     */
    public int txDhtXidMapSize() {
        return txDhtXidMapSize;
    }

    /**
     * @return Committed DHT transaction queue size.
     */
    public int txDhtCommitQueueSize() {
        return txDhtCommitQueueSize;
    }

    /**
     * @return Prepared DHT transaction queue size.
     */
    public int txDhtPrepareQueueSize() {
        return txDhtPrepareQueueSize;
    }

    /**
     * @return DHT start version counts map size.
     */
    public int txDhtStartVersionCountsSize() {
        return txDhtStartVerCountsSize;
    }

    /**
     * @return Number of cached committed DHT transaction IDs.
     */
    public int txDhtCommittedVersionsSize() {
        return txDhtCommittedVersionsSize;
    }

    /**
     * @return Number of cached rolled back DHT transaction IDs.
     */
    public int txDhtRolledbackVersionsSize() {
        return txDhtRolledbackVersionsSize;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheMetrics.class, this);
    }
}
