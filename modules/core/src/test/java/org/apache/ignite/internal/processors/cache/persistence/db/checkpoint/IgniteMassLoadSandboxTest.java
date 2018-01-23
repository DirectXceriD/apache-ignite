/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import junit.framework.TestCase;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagesWriteThrottle;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jsr166.LongAdder8;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;

/**
 * //todo code style
 */
public class IgniteMassLoadSandboxTest extends GridCommonAbstractTest {
    /** */
    private static final String HAS_CACHE = "HAS_CACHE";
    /** Cache name. */
    public static final String CACHE_NAME = "partitioned" + new Random().nextInt(10000000);
    public static final int OBJECT_SIZE = 40000;
    public static final int CONTINUOUS_PUT_RECS_CNT = 200_000;
    public static final String PUT_THREAD = "put-thread";
    public static final String GET_THREAD = "get-thread";

    /** */
    protected boolean setWalArchAndWorkToSameValue;

    /** */
    private String cacheName;

    /** */
    private int walSegmentSize = 48 * 1024 * 1024;
    /** Custom wal mode. */
    protected WALMode customWalMode;
    private int checkpointFrequency = 15 * 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IndexedObject> ccfg = new CacheConfiguration<>(cacheName);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 1024));
        ccfg.setNodeFilter(new RemoteNodeFilter());
        ccfg.setIndexedTypes(Integer.class, IndexedObject.class);
        ccfg.setName(CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        dsCfg.setPageSize(4 * 1024);

        DataRegionConfiguration regCfg = new DataRegionConfiguration()
            .setName("dfltMemPlc")
            .setMetricsEnabled(true);

        regCfg.setMaxSize(10 * 1024L * 1024 * 1024);
        regCfg.setPersistenceEnabled(true);

        dsCfg.setDefaultDataRegionConfiguration(regCfg);

        dsCfg.setWriteThrottlingEnabled(true);


        dsCfg.setCheckpointFrequency(checkpointFrequency);

        //dsCfg.setCheckpointPageBufferSize(1024L * 1024 * 1024);

        final String workDir = U.defaultWorkDirectory();
        final File db = U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false);
        final File wal = new File(db, "wal");
        if (setWalArchAndWorkToSameValue) {
            final String walAbsPath = wal.getAbsolutePath();

            dsCfg.setWalPath(walAbsPath);

            dsCfg.setWalArchivePath(walAbsPath);
        }
        else {
            dsCfg.setWalPath(wal.getAbsolutePath());

            dsCfg.setWalArchivePath(new File(wal, "archive").getAbsolutePath());
        }

        dsCfg.setWalMode(customWalMode != null ? customWalMode : WALMode.LOG_ONLY);
        dsCfg.setWalHistorySize(1);
        dsCfg.setWalSegments(10);
        if (walSegmentSize != 0)
            dsCfg.setWalSegmentSize(walSegmentSize);

        cfg.setDataStorageConfiguration(dsCfg);

        cfg.setMarshaller(null);

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);

        if (!getTestIgniteInstanceName(0).equals(gridName))
            cfg.setUserAttributes(F.asMap(HAS_CACHE, true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "temp", false));

        cacheName = CACHE_NAME;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        // deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), "db", false));

    }

    /** Object with additional 40 000 bytes of payload */
    public static class HugeIndexedObject extends IndexedObject {
        byte[] data;

        /**
         * @param iVal Integer value.
         */
        private HugeIndexedObject(int iVal) {
            super(iVal);
            int sz = OBJECT_SIZE;
            data = new byte[sz];
            for (int i = 0; i < sz; i++)
                data[i] = (byte)('A' + (i % 10));
        }
    }

    private static String generateThreadDump() {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
        final StringBuilder dump = new StringBuilder();
        for (ThreadInfo threadInfo : threadInfos) {
            String name = threadInfo.getThreadName();
            if (name.contains("checkpoint-runner")
                || name.contains("db-checkpoint-thread")
                || name.contains("wal-file-archiver")
                || name.contains(GET_THREAD)
                || name.contains(PUT_THREAD))
                dump.append(threadInfo.toString());
            else {
                String s = threadInfo.toString();

                if (s.contains(FileWriteAheadLogManager.class.getSimpleName())
                    || s.contains(FilePageStoreManager.class.getSimpleName())) {
                    dump.append(s);
                }
            }
        }
        return dump.toString();
    }

    private static class ProgressWatchdog {
        private final LongAdder8 longAdder8 = new LongAdder8();
        private final FileWriter txtWriter;

        private ScheduledExecutorService svc = Executors.newScheduledThreadPool(1);
        private final String operation;
        private Ignite ignite;
        private volatile boolean stopping;

        private final AtomicLong prevCnt = new AtomicLong();
        private final AtomicLong prevMsElapsed = new AtomicLong();
        private final AtomicLong prevCpWrittenPages = new AtomicLong();
        private final AtomicLong prevCpSyncedPages = new AtomicLong();
        private final AtomicReference<FileWALPointer> prevWalPtrRef = new AtomicReference<>();

        ProgressWatchdog(Ignite ignite) throws IgniteCheckedException, IOException {
            this(ignite, "put");
        }

        ProgressWatchdog(Ignite ignite, String operation) throws IgniteCheckedException, IOException {
            this.ignite = ignite;
            this.operation = operation;
            txtWriter = new FileWriter(new File(getTempDirFile(), "watchdog-" + operation + ".txt"));
            line("sec",
                "cur." + operation + "/sec",
                "WAL speed, MB/s.",
                "cp. speed, MB/sec",
                "cp. sync., MB/sec",
                "WAL work seg.",
                "pageMemThrRatio",
                "throttleLevel",
                "avg." + operation + "/sec",
                "dirtyPages",
                "cpWrittenPages",
                "threshold",
                "WAL idx",
                "Arch. idx",
                "WAL Archive seg.");
        }

        private void line(Object... parms) {
            try {
                for (int i = 0; i < parms.length; i++) {
                    Object parm = parms[i];
                    txtWriter.write(parm
                        + ((i < parms.length - 1) ? "\t" : "\n"));
                }
                txtWriter.flush();
            }
            catch (IOException ignored) {
            }
        }

        public void start() {
            final long msStart = U.currentTimeMillis();
            prevMsElapsed.set(0);
            prevCnt.set(0);
            int checkPeriodMsec = 500;
            svc.scheduleAtFixedRate(
                () -> tick(msStart),
                checkPeriodMsec, checkPeriodMsec, TimeUnit.MILLISECONDS);
        }

        private void tick(long msStart) {
            long elapsedMs = U.currentTimeMillis() - msStart;
            final long totalCnt = longAdder8.longValue();
            final long averagePutPerSec = totalCnt * 1000 / elapsedMs;
            long elapsedMsFromPrevTick = elapsedMs - prevMsElapsed.getAndSet(elapsedMs);
            if (elapsedMsFromPrevTick == 0)
                return;

            final long currPutPerSec = ((totalCnt - prevCnt.getAndSet(totalCnt)) * 1000) / elapsedMsFromPrevTick;
            boolean slowProgress = currPutPerSec < averagePutPerSec / 10 && !stopping;
            final String fileNameWithDump = slowProgress ? reactNoProgress(msStart) : "";

            DataStorageConfiguration dsCfg = ignite.configuration().getDataStorageConfiguration();


            String defRegName = dsCfg.getDefaultDataRegionConfiguration().getName();
            long dirtyPages = -1;
            for (DataRegionMetrics m : ignite.dataRegionMetrics())
             if (m.getName().equals(defRegName))
                 dirtyPages = m.getDirtyPages();

            GridCacheSharedContext<Object, Object> cacheSctx = null;
            PageMemoryImpl pageMemory = null;
            try {
                cacheSctx = ((IgniteEx)ignite).context().cache().context();
                pageMemory = (PageMemoryImpl)cacheSctx.database()
                    .dataRegion(defRegName).pageMemory();

            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();
            }

            long cpBufPages = 0;

            GridCacheDatabaseSharedManager database = (GridCacheDatabaseSharedManager)(cacheSctx.database());
            AtomicInteger wrPageCntr = database.writtenPagesCounter();
            long cpWrittenPages = wrPageCntr == null ? 0 : wrPageCntr.get();

            AtomicInteger syncedPagesCounter = database.syncedPagesCounter();
            int cpSyncedPages = syncedPagesCounter == null ? 0 : syncedPagesCounter.get();

            int pageSize = pageMemory == null ? 0 : pageMemory.pageSize();
            long currBytesWritten = detectDelta(elapsedMsFromPrevTick, cpWrittenPages, prevCpWrittenPages) * pageSize;

            long currBytesSynced = detectDelta(elapsedMsFromPrevTick, cpSyncedPages, prevCpSyncedPages) * pageSize;

            String walSpeed = "";
            double threshold = 0;
            int throttleLevel = 0;
            double pageMemThrottleRatio = 0.0;
            long idx = -1;
            long lastArchIdx = -1;
            int walArchiveSegments = 0;
            long walWorkSegments = 0;
            try {

                if (pageMemory != null) {
                    cpBufPages = pageMemory.checkpointBufferPagesCount();

                    PagesWriteThrottle throttle = U.field(pageMemory, "writeThrottle");

                    threshold = U.field(throttle, "lastDirtyRatioThreshold");
                    throttleLevel = throttle.throttleLevel();
                    pageMemThrottleRatio = throttle.getPageMemThrottleRatio() ;
                }

                FileWriteAheadLogManager wal = (FileWriteAheadLogManager)cacheSctx.wal();
                FileWALPointer ptr = wal.currentWritePointer();
                idx = ptr.index();

                lastArchIdx =wal.lastAbsArchivedIdx();

                walArchiveSegments = wal.walArchiveSegments();
                int maxWorkSegments = dsCfg.getWalSegments();
                walWorkSegments = idx - lastArchIdx;

                long maxWalSegmentSize = wal.maxWalSegmentSize();

                FileWALPointer prevWalPtr = this.prevWalPtrRef.getAndSet(ptr);

                if (prevWalPtr != null) {
                    long idxDiff = ptr.index() - prevWalPtr.index();
                    long offDiff = ptr.fileOffset() - prevWalPtr.fileOffset();
                    long bytesDiff = idxDiff * maxWalSegmentSize + offDiff;

                    long bytesPerSec = (bytesDiff * 1000) / elapsedMsFromPrevTick;

                    walSpeed = getMBytesPrintable(bytesPerSec);
                } else
                    walSpeed = "0";
            }
            catch (Exception ignored) {
                //e.printStackTrace();
            }



            String thresholdStr = formatDbl(threshold);
            String cpWriteSpeed = getMBytesPrintable(currBytesWritten);
            String cpSyncSpeed = getMBytesPrintable(currBytesSynced);
            long elapsedSecs = elapsedMs / 1000;
            X.println(" >> " +
                operation +
                " done: " + totalCnt + "/" + elapsedSecs + "s, " +
                "Cur. " + operation + " " + currPutPerSec + " recs/sec " +
                "cpWriteSpeed=" + cpWriteSpeed + " " +
                "cpSyncSpeed=" + cpSyncSpeed + " " +
                "walSpeed= " + walSpeed + " " +
                "walWorkSeg.="+walWorkSegments + " " +
                "Avg. " + operation + " " + averagePutPerSec + " recs/sec, " +
                "dirtyP=" + dirtyPages + ", " +
                "cpWrittenP.=" + cpWrittenPages + ", " +
                "cpBufP.=" + cpBufPages + " " +
                "threshold=" + thresholdStr + " " +
                "walIdx=" + idx + " " +
                "archWalIdx=" + lastArchIdx + " " +
                "walArchiveSegments=" + walArchiveSegments + " " +
                fileNameWithDump);

            line(elapsedSecs,
                currPutPerSec,
                walSpeed,
                cpWriteSpeed,
                cpSyncSpeed,
                walWorkSegments,
                formatDbl(pageMemThrottleRatio),
                throttleLevel,
                averagePutPerSec,
                dirtyPages,
                cpWrittenPages,
                thresholdStr,
                idx,
                lastArchIdx,
                walArchiveSegments
                );
        }

        private String formatDbl(double threshold) {
            return String.format("%.2f", threshold).replace(",", ".");
        }

        private String getMBytesPrintable(long currBytesWritten) {
            double cpMbPs = 1.0 * currBytesWritten / (1024 * 1024);
            return formatDbl(cpMbPs);
        }

        private long detectDelta(long elapsedMsFromPrevTick,
            long absValue,
            AtomicLong cnt) {
            long cpPagesChange = absValue - cnt.getAndSet(absValue);
            if (cpPagesChange < 0)
                cpPagesChange = 0;

            return (cpPagesChange * 1000) / elapsedMsFromPrevTick;
        }

        private String reactNoProgress(long msStart) {
            try {
                String s = generateThreadDump();
                long sec = (U.currentTimeMillis() - msStart) / 1000;
                String fileName = "dumpAt" + sec + "second.txt";
                if (s.contains(IgniteCacheDatabaseSharedManager.class.getName() + ".checkpointLock"))
                    fileName = "checkpoint_" + fileName;

                fileName = operation + fileName;

                File tempDir = getTempDirFile();
                try (FileWriter writer = new FileWriter(new File(tempDir, fileName))) {
                    writer.write(s);
                }
                return fileName;
            }
            catch (IOException | IgniteCheckedException e) {
                e.printStackTrace();
            }
            return "";
        }

        public void reportProgress(int cnt) {
            longAdder8.add(cnt);
        }

        public void stop() {
            U.closeQuiet(txtWriter);

            svc.shutdown();
            try {
                svc.awaitTermination(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }

        public void stopping() {
            this.stopping = true;
        }
    }

    @NotNull private static File getTempDirFile() throws IgniteCheckedException {
        File tempDir = new File(U.defaultWorkDirectory(), "temp");

        if (!tempDir.exists())
            tempDir.mkdirs();

        return tempDir;
    }

    public void testContinuousPutMultithreaded() throws Exception {
        try {
            System.setProperty(IgniteSystemProperties.IGNITE_DIRTY_PAGES_PARALLEL, "true");
            System.setProperty(IgniteSystemProperties.IGNITE_DIRTY_PAGES_SORTED_STORAGE, "true");

            //setWalArchAndWorkToSameValue = true;

            customWalMode = WALMode.LOG_ONLY;
            final IgniteEx ignite = startGrid(1);

            ignite.active(true);

            final IgniteCache<Object, IndexedObject> cache = ignite.cache(CACHE_NAME);
            int totalRecs = CONTINUOUS_PUT_RECS_CNT;
            final int threads = Runtime.getRuntime().availableProcessors();

            final int recsPerThread = totalRecs / threads;
            final Collection<Callable<?>> tasks = new ArrayList<>();

            final ProgressWatchdog watchdog = new ProgressWatchdog(ignite);

            for (int j = 0; j < threads; j++) {
                final int finalJ = j;
                tasks.add(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        for (int i = finalJ * recsPerThread; i < ((finalJ + 1) * recsPerThread); i++) {
                            IndexedObject v = new HugeIndexedObject(i);
                            cache.put(i, v);
                            watchdog.reportProgress(1);
                        }
                        return null;
                    }
                });
            }

            watchdog.start();
            GridTestUtils.runMultiThreaded(tasks, PUT_THREAD);
            watchdog.stopping();
            stopGrid(1);
            watchdog.stop();

            // System.out.println("Please clear page cache");
            //Thread.sleep(10000);

            //runVerification(threads, recsPerThread);
        }
        finally {
            stopAllGrids();
        }
    }

    protected void runVerification(int threads, final int recsPerThread) throws Exception {
        final Ignite restartedIgnite = startGrid(1);

        restartedIgnite.active(true);

        final IgniteCache<Integer, IndexedObject> restartedCache = restartedIgnite.cache(CACHE_NAME);

        final ProgressWatchdog watchdog2 = new ProgressWatchdog(restartedIgnite, "get");

        final Collection<Callable<?>> tasksR = new ArrayList<>();
        tasksR.clear();
        for (int j = 0; j < threads; j++) {
            final int finalJ = j;
            tasksR.add(new Callable<Void>() {
                @Override public Void call() {
                    for (int i = finalJ * recsPerThread; i < ((finalJ + 1) * recsPerThread); i++) {
                        IndexedObject object = restartedCache.get(i);
                        int actVal = object.iVal;
                        TestCase.assertEquals(i, actVal);
                        watchdog2.reportProgress(1);
                    }
                    return null;
                }
            });
        }

        watchdog2.start();
        GridTestUtils.runMultiThreaded(tasksR, GET_THREAD);
        watchdog2.stop();
    }

    private void verifyByChunk(int threads, int recsPerThread,
        IgniteCache<Integer, IndexedObject> restartedCache) {
        int verifyChunk = 100;

        int totalRecsToVerify = recsPerThread * threads;
        int chunks = totalRecsToVerify / verifyChunk;

        for (int c = 0; c < chunks; c++) {

            TreeSet<Integer> keys = new TreeSet<>();

            for (int i = 0; i < verifyChunk; i++) {
                keys.add(i + c * verifyChunk);
            }

            Map<Integer, IndexedObject> values = restartedCache.getAll(keys);
            for (Map.Entry<Integer, IndexedObject> next : values.entrySet()) {
                Integer key = next.getKey();
                int actVal = values.get(next.getKey()).iVal;
                int i = key.intValue();
                TestCase.assertEquals(i, actVal);
                if (i % 1000 == 0)
                    X.println(" >> Verified: " + i);
            }

        }
    }

    private static boolean keepInDb(int id) {
        return id % 1777 == 0;
    }

    public void testPutRemoveMultithread() throws Exception {
        setWalArchAndWorkToSameValue = false;

        //if (!setWalArchAndWorkToSameValue)
        //    assertNull(getConfiguration("").getDataStorageConfiguration().getWalArchivePath());

        customWalMode = WALMode.LOG_ONLY;

        try {
            final IgniteEx ignite = startGrid(1);
            ignite.active(true);

            final IgniteCache<Object, IndexedObject> cache = ignite.cache(CACHE_NAME);
            int totalRecs = 200_000;
            final int threads = 10;

            final int recsPerThread = totalRecs / threads;
            final Collection<Callable<?>> tasks = new ArrayList<>();

            final ProgressWatchdog watchdog = new ProgressWatchdog(ignite);

            for (int j = 0; j < threads; j++) {
                final IgniteCache<Object, IndexedObject> finalCache = cache;
                final int finalJ = j;
                tasks.add(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        final List<Integer> toRmvLaterList = new ArrayList<>();
                        for (int id = finalJ * recsPerThread; id < ((finalJ + 1) * recsPerThread); id++) {
                            IndexedObject v = new HugeIndexedObject(id);
                            finalCache.put(id, v);
                            toRmvLaterList.add(id);
                            watchdog.reportProgress(1);
                            if (toRmvLaterList.size() > 100) {
                                for (Integer toRemoveId : toRmvLaterList) {
                                    if (keepInDb(toRemoveId))
                                        continue;
                                    boolean rmv = finalCache.remove(toRemoveId);
                                    assert rmv : "Expected to remove object from cache " + toRemoveId;
                                }
                                toRmvLaterList.clear();
                            }
                        }
                        return null;
                    }
                });
            }

            watchdog.start();
            GridTestUtils.runMultiThreaded(tasks, "put-thread");
            watchdog.stop();
            stopGrid(1);

            final Ignite restartedIgnite = startGrid(1);
            restartedIgnite.active(true);

            final IgniteCache<Object, IndexedObject> restartedCache = restartedIgnite.cache(CACHE_NAME);

            // Check.
            for (int i = 0; i < recsPerThread * threads; i++) {
                if (keepInDb(i)) {
                    final IndexedObject obj = restartedCache.get(i);

                    TestCase.assertNotNull(obj);
                    TestCase.assertEquals(i, obj.iVal);
                }
                if (i % 1000 == 0)
                    X.print(" V: " + i);
            }
        }
        finally {
            stopAllGrids();
        }
    }


    public void ignoredTestManyCachesAndNotManyPuts() throws Exception {
        try {
            customWalMode = WALMode.BACKGROUND;
            checkpointFrequency = 1000;
            final IgniteEx ignite = startGrid(1);

            ignite.active(true);

            final int groups = 6;
            final int cachesInGrp = 10;
            for (int g = 0; g < groups; g++) {
                for (int i = 0; i < cachesInGrp; i++) {
                    final CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>();
                    final int parts = 1000;
                    final IgniteCache<Object, Object> cache = ignite.getOrCreateCache(
                        ccfg.setName("dummyCache" + i + "." + g)
                            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                            .setGroupName("dummyGroup" + g)
                            .setAffinity(new RendezvousAffinityFunction(false, parts)));

                    cache.putAll(new TreeMap<Long, Long>() {{
                        for (int j = 0; j < parts; j++) {
                            // to fill each partition cache with at least 1 element
                            put((long)j, (long)j);
                        }
                    }});
                }
            }

            final IgniteCache<Object, IndexedObject> cache = ignite.cache(CACHE_NAME);
            final int threads = 2;

            final int recsPerThread = 200 / threads;
            final Collection<Callable<?>> tasks = new ArrayList<>();

            final ProgressWatchdog watchdog = new ProgressWatchdog(ignite);

            for (int j = 0; j < threads; j++) {
                final int finalJ = j;
                tasks.add(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        for (int i = finalJ * recsPerThread; i < ((finalJ + 1) * recsPerThread); i++) {
                            IndexedObject v = new HugeIndexedObject(i);
                            cache.put(i, v);

                            final Random random = new Random();
                            ignite.cache("dummyCache" + random.nextInt(cachesInGrp) +
                                "." + random.nextInt(groups)).put(i, i);

                            watchdog.reportProgress(1);

                            Thread.sleep(5000);
                        }
                        return null;
                    }
                });
            }

            watchdog.start();
            GridTestUtils.runMultiThreaded(tasks, PUT_THREAD);
            watchdog.stopping();
            stopGrid(1);
            watchdog.stop();

            System.out.println("Please clear page cache");
            Thread.sleep(10000);

            runVerification(threads, recsPerThread);
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TimeUnit.MINUTES.toMillis(20);
    }

    /**
     *
     */
    private static class RemoteNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return clusterNode.attribute(HAS_CACHE) != null;
        }
    }

    /**
     *
     */
    protected static class IndexedObject {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /**
         * @param iVal Integer value.
         */
        private IndexedObject(int iVal) {
            this.iVal = iVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof IndexedObject))
                return false;

            IndexedObject that = (IndexedObject)o;

            return iVal == that.iVal;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return iVal;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IndexedObject.class, this);
        }
    }
}