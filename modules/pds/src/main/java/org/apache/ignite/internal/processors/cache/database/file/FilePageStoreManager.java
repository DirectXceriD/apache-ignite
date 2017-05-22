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

package org.apache.ignite.internal.processors.cache.database.file;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistenceConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.database.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * File page store manager.
 */
public class FilePageStoreManager extends GridCacheSharedManagerAdapter implements IgnitePageStoreManager {
    /** File suffix. */
    public static final String FILE_SUFFIX = ".bin";

    /** Partition file prefix. */
    public static final String PART_FILE_PREFIX = "part-";

    /** */
    public static final String INDEX_FILE_NAME = "index" + FILE_SUFFIX;

    /** */
    public static final String PART_FILE_TEMPLATE = PART_FILE_PREFIX+ "%d" + FILE_SUFFIX;

    /** */
    public static final String CACHE_DIR_PREFIX = "cache-";

    /** */
    public static final String CACHE_CONF_FILENAME = "conf.dat";

    /** Marshaller. */
    private static final Marshaller marshaller = new JdkMarshaller();

    /** */
    private final Map<Integer, CacheStoreHolder> idxCacheStores = new ConcurrentHashMap<>();

    /** */
    private final IgniteConfiguration igniteCfg;

    /** */
    private PersistenceConfiguration pstCfg;

    /** Absolute directory for file page store */
    private File storeWorkDir;

    /** */
    private final long metaPageId = PageIdUtils.pageId(-1, PageMemory.FLAG_IDX, 0);

    /** */
    private final Set<Integer> cachesWithoutIdx = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());

    /**
     * @param ctx Kernal context.
     */
    public FilePageStoreManager(GridKernalContext ctx) {
        igniteCfg = ctx.config();

        PersistenceConfiguration pstCfg = igniteCfg.getPersistenceConfiguration();

        assert pstCfg != null : "WAL should not be created if persistence is disabled.";

        this.pstCfg = pstCfg;
    }

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        if (cctx.kernalContext().clientNode())
            return;

        String consId = U.maskForFileName(cctx.kernalContext().discovery().consistentId().toString());

        if (pstCfg.getPersistenceStorePath() != null) {
            File workDir0 = new File(pstCfg.getPersistenceStorePath());

            if (!workDir0.isAbsolute())
                workDir0 = U.resolveWorkDirectory(
                    igniteCfg.getWorkDirectory(),
                    pstCfg.getPersistenceStorePath(),
                    false
                );

            storeWorkDir = new File(workDir0, consId);
        }
        else
            storeWorkDir = new File(U.resolveWorkDirectory(
                igniteCfg.getWorkDirectory(),
                "db",
                false
            ), consId);

        U.ensureDirectory(storeWorkDir, "page store work directory", log);
    }

    /** {@inheritDoc} */
    @Override public void stop0(boolean cancel) {
        if (log.isDebugEnabled())
            log.debug("Stopping page store manager.");

        IgniteCheckedException ex = shutdown(false);

        if (ex != null)
            U.error(log, "Failed to gracefully stop page store manager", ex);
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activate page store manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        start0();
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("DeActivate page store manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        stop0(true);

        idxCacheStores.clear();
    }

    /** {@inheritDoc} */
    @Override public void beginRecover() {
        for (CacheStoreHolder holder : idxCacheStores.values()) {
            holder.idxStore.beginRecover();

            for (FilePageStore partStore : holder.partStores)
                partStore.beginRecover();
        }
    }

    /** {@inheritDoc} */
    @Override public void finishRecover() {
        for (CacheStoreHolder holder : idxCacheStores.values()) {
            holder.idxStore.finishRecover();

            for (FilePageStore partStore : holder.partStores)
                partStore.finishRecover();
        }
    }

    /** {@inheritDoc} */
    @Override public void initializeForCache(CacheConfiguration ccfg) throws IgniteCheckedException {
        int cacheId = CU.cacheId(ccfg.getName());

        if (!idxCacheStores.containsKey(cacheId)) {
            CacheStoreHolder holder = initForCache(ccfg);

            CacheStoreHolder old = idxCacheStores.put(cacheId, holder);

            assert old == null : "Non-null old store holder for cache: " + ccfg.getName();
        }
    }

    /** {@inheritDoc} */
    @Override public void shutdownForCache(GridCacheContext cacheCtx, boolean destroy) throws IgniteCheckedException {
        cachesWithoutIdx.remove(cacheCtx.cacheId());

        CacheStoreHolder old = idxCacheStores.remove(cacheCtx.cacheId());

        assert old != null : "Missing cache store holder [cache=" + cacheCtx.name() +
            ", locNodeId=" + cctx.localNodeId() + ", gridName=" + cctx.igniteInstanceName() + ']';

        IgniteCheckedException ex = shutdown(old, /*clean files if destroy*/destroy, null);

        if (ex != null)
            throw ex;
    }

    /** {@inheritDoc} */
    @Override public void onPartitionCreated(int cacheId, int partId) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onPartitionDestroyed(int cacheId, int partId, int tag) throws IgniteCheckedException {
        assert partId <= PageIdAllocator.MAX_PARTITION_ID;

        PageStore store = getStore(cacheId, partId);

        assert store instanceof FilePageStore;

        ((FilePageStore)store).truncate(tag);
    }

    /** {@inheritDoc} */
    @Override public void read(int cacheId, long pageId, ByteBuffer pageBuf) throws IgniteCheckedException {
        read(cacheId, pageId, pageBuf, false);
    }

    /**
     * Will preserve crc in buffer if keepCrc is true.
     *
     * @param cacheId Cache ID.
     * @param pageId Page ID.
     * @param pageBuf Page buffer.
     * @param keepCrc Keep CRC flag.
     * @throws IgniteCheckedException If failed.
     */
    public void read(int cacheId, long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteCheckedException {
        PageStore store = getStore(cacheId, PageIdUtils.partId(pageId));

        store.read(pageId, pageBuf, keepCrc);
    }

    /** {@inheritDoc} */
    @Override public boolean exists(int cacheId, int partId) throws IgniteCheckedException {
        PageStore store = getStore(cacheId, partId);

        return store.exists();
    }

    /** {@inheritDoc} */
    @Override public void readHeader(int cacheId, int partId, ByteBuffer buf) throws IgniteCheckedException {
        PageStore store = getStore(cacheId, partId);

        store.readHeader(buf);
    }

    /** {@inheritDoc} */
    @Override public void write(int cacheId, long pageId, ByteBuffer pageBuf,int tag) throws IgniteCheckedException {
        writeInternal(cacheId, pageId, pageBuf, tag);
    }

    /** {@inheritDoc} */
    @Override public long pageOffset(int cacheId, long pageId) throws IgniteCheckedException {
        PageStore store = getStore(cacheId, PageIdUtils.partId(pageId));

        return store.pageOffset(pageId);
    }

    /**
     * @param cacheId Cache ID to write.
     * @param pageId Page ID.
     * @param pageBuf Page buffer.
     * @return PageStore to which the page has been written.
     * @throws IgniteCheckedException If IO error occurred.
     */
    public PageStore writeInternal(int cacheId, long pageId, ByteBuffer pageBuf, int tag) throws IgniteCheckedException {
        int partId = PageIdUtils.partId(pageId);

        PageStore store = getStore(cacheId, partId);

        store.write(pageId, pageBuf, tag);

        return store;
    }

    /**
     * @param ccfg Cache configuration.
     * @return Cache store holder.
     * @throws IgniteCheckedException If failed.
     */
    private CacheStoreHolder initForCache(CacheConfiguration ccfg) throws IgniteCheckedException {
        File cacheWorkDir = new File(storeWorkDir, CACHE_DIR_PREFIX + ccfg.getName());

        boolean dirExisted = false;

        if (!cacheWorkDir.exists()) {
            boolean res = cacheWorkDir.mkdirs();

            if (!res)
                throw new IgniteCheckedException("Failed to initialize cache working directory " +
                    "(failed to create, make sure the work folder has correct permissions): " +
                    cacheWorkDir.getAbsolutePath());
        }
        else {
            if (cacheWorkDir.isFile())
                throw new IgniteCheckedException("Failed to initialize cache working directory " +
                    "(a file with the same name already exists): " + cacheWorkDir.getAbsolutePath());

            File lockF = new File(cacheWorkDir, IgniteCacheSnapshotManager.SNAPSHOT_RESTORE_STARTED_LOCK_FILENAME);

            if (lockF.exists()) {
                Path tmp = cacheWorkDir.toPath().getParent().resolve(cacheWorkDir.getName() + ".tmp");

                boolean deleted = U.delete(cacheWorkDir);

                if (Files.exists(tmp) && Files.isDirectory(tmp)) {
                    U.warn(log, "Ignite node crashed during the snapshot restore process " +
                        "(there is a snapshot restore lock file left for cache). But old version of cache was saved. " +
                        "Trying to restore it. Cache - [" + cacheWorkDir.getAbsolutePath() + ']');

                    try {
                        Files.move(tmp, cacheWorkDir.toPath());
                    }
                    catch (IOException e) {
                        throw new IgniteCheckedException(e);
                    }
                } else {
                    U.warn(log, "Ignite node crashed during the snapshot restore process " +
                        "(there is a snapshot restore lock file left for cache). Will remove both the lock file and " +
                        "incomplete cache directory [cacheDir=" + cacheWorkDir.getAbsolutePath() + ']');

                    if (!deleted)
                        throw new IgniteCheckedException("Failed to remove obsolete cache working directory " +
                            "(remove the directory manually and make sure the work folder has correct permissions): " +
                            cacheWorkDir.getAbsolutePath());

                    cacheWorkDir.mkdirs();
                }

                if (!cacheWorkDir.exists())
                    throw new IgniteCheckedException("Failed to initialize cache working directory " +
                        "(failed to create, make sure the work folder has correct permissions): " +
                        cacheWorkDir.getAbsolutePath());
            }
            else
                dirExisted = true;
        }

        File file = new File(cacheWorkDir, CACHE_CONF_FILENAME);

        if (!file.exists() || file.length() == 0) {
            try {
                file.createNewFile();

                try (OutputStream stream = new BufferedOutputStream(new FileOutputStream(file))) {
                    marshaller.marshal(ccfg, stream);
                }
            }
            catch (IOException ex) {
                throw new IgniteCheckedException("Failed to persist cache configuration: " + ccfg.getName(), ex);
            }
        }

        File idxFile = new File(cacheWorkDir, INDEX_FILE_NAME);

        if (dirExisted && !idxFile.exists())
            cachesWithoutIdx.add(CU.cacheId(ccfg.getName()));

        FilePageStore idxStore = new FilePageStore(
            PageMemory.FLAG_IDX,
            idxFile,
            cctx.kernalContext().config().getMemoryConfiguration());

        FilePageStore[] partStores = new FilePageStore[ccfg.getAffinity().partitions()];

        for (int partId = 0; partId < partStores.length; partId++) {
            FilePageStore partStore = new FilePageStore(
                PageMemory.FLAG_DATA,
                new File(cacheWorkDir, String.format(PART_FILE_TEMPLATE, partId)),
                cctx.kernalContext().config().getMemoryConfiguration()
            );

            partStores[partId] = partStore;
        }

        return new CacheStoreHolder(idxStore, partStores);
    }

    /** {@inheritDoc} */
    @Override public void sync(int cacheId, int partId) throws IgniteCheckedException {
        getStore(cacheId, partId).sync();
    }

    /** {@inheritDoc} */
    @Override public void ensure(int cacheId, int partId) throws IgniteCheckedException {
        getStore(cacheId, partId).ensure();
    }

    /** {@inheritDoc} */
    @Override public long allocatePage(int cacheId, int partId, byte flags) throws IgniteCheckedException {
        assert partId <= PageIdAllocator.MAX_PARTITION_ID || partId == PageIdAllocator.INDEX_PARTITION;

        PageStore store = getStore(cacheId, partId);

        long pageIdx = store.allocatePage();

        return PageIdUtils.pageId(partId, flags, (int)pageIdx);
    }

    /** {@inheritDoc} */
    @Override public long metaPageId(final int cacheId) {
        return metaPageId;
    }

    /** {@inheritDoc} */
    @Override public int pages(int cacheId, int partId) throws IgniteCheckedException {
        PageStore store = getStore(cacheId, partId);

        return store.pages();
    }

    /** {@inheritDoc} */
    @Override public Set<String> savedCacheNames() {
        if (cctx.kernalContext().clientNode())
            return Collections.emptySet();

        File[] files = storeWorkDir.listFiles();

        if (files == null)
            return Collections.emptySet();

        Set<String> cacheNames = new HashSet<>();

        for (File file : files) {
            if (file.isDirectory() && file.getName().startsWith(CACHE_DIR_PREFIX)) {
                File conf = new File(file, CACHE_CONF_FILENAME);
                if (conf.exists() && conf.length() > 0) {
                    String name = file.getName().substring(CACHE_DIR_PREFIX.length());

                    // TODO remove when fixed null cache names.
                    if ("null".equals(name))
                        name = null;

                    cacheNames.add(name);
                }
            }
        }

        return cacheNames;
    }

    /** {@inheritDoc} */
    @Override public CacheConfiguration readConfiguration(String cacheName) {
        File file = new File(storeWorkDir, CACHE_DIR_PREFIX + cacheName);

        assert file.exists() && file.isDirectory();

        try (InputStream stream = new BufferedInputStream(new FileInputStream(new File(file, CACHE_CONF_FILENAME)))) {
            return marshaller.unmarshal(stream, U.resolveClassLoader(igniteCfg));
        }
        catch (IOException | IgniteCheckedException e) {
            throw new IllegalStateException("Failed to read cache configuration from disk for cache: " + cacheName, e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasIndexStore(int cacheId) {
        return !cachesWithoutIdx.contains(cacheId);
    }

    /**
     * @return Store work dir.
     */
    public File workDir() {
        return storeWorkDir;
    }

    /**
     * @param cacheName Cache name.
     * @return Store dir for given cache.
     */
    public File cacheWorkDir(String cacheName) {
        return new File(storeWorkDir, "cache-" + cacheName);
    }

    /**
     * @param cleanFiles {@code True} if the stores should delete it's files upon close.
     */
    private IgniteCheckedException shutdown(boolean cleanFiles) {
        IgniteCheckedException ex = null;

        for (CacheStoreHolder holder : idxCacheStores.values())
            ex = shutdown(holder, cleanFiles, ex);

        return ex;
    }

    /**
     * @param holder Store holder.
     * @param cleanFile {@code True} if files should be cleaned.
     * @param aggr Aggregating exception.
     * @return Aggregating exception, if error occurred.
     */
    private IgniteCheckedException shutdown(CacheStoreHolder holder, boolean cleanFile,
        @Nullable IgniteCheckedException aggr) {
        aggr = shutdown(holder.idxStore, cleanFile, aggr);

        for (FilePageStore store : holder.partStores) {
            if (store != null)
                aggr = shutdown(store, cleanFile, aggr);
        }

        return aggr;
    }

    /**
     * @param store Store to shutdown.
     * @param cleanFile {@code True} if files should be cleaned.
     * @param aggr Aggregating exception.
     * @return Aggregating exception, if error occurred.
     */
    private IgniteCheckedException shutdown(FilePageStore store, boolean cleanFile, IgniteCheckedException aggr) {
        try {
            if (store != null)
                store.stop(cleanFile);
        }
        catch (IgniteCheckedException e) {
            if (aggr == null)
                aggr = new IgniteCheckedException("Failed to gracefully shutdown store");

            aggr.addSuppressed(e);
        }

        return aggr;
    }

    /**
     * @param cacheId Cache ID.
     * @param partId Partition ID.
     * @return Page store for the corresponding parameters.
     * @throws IgniteCheckedException If cache or partition with the given ID was not created.
     *
     * Note: visible for testing.
     */
    public PageStore getStore(int cacheId, int partId) throws IgniteCheckedException {
        CacheStoreHolder holder = idxCacheStores.get(cacheId);

        if (holder == null)
            throw new IgniteCheckedException("Failed to get page store for the given cache ID " +
                "(cache has not been started): " + cacheId);

        if (partId == PageIdAllocator.INDEX_PARTITION)
            return holder.idxStore;

        if (partId > PageIdAllocator.MAX_PARTITION_ID)
            throw new IgniteCheckedException("Partition ID is reserved: " + partId);

        FilePageStore store = holder.partStores[partId];

        if (store == null)
            throw new IgniteCheckedException("Failed to get page store for the given partition ID " +
                "(partition has not been created) [cacheId=" + cacheId + ", partId=" + partId + ']');

        return store;
    }

    /**
     *
     */
    private static class CacheStoreHolder {
        /** Index store. */
        private final FilePageStore idxStore;

        /** Partition stores. */
        private final FilePageStore[] partStores;

        /**
         *
         */
        public CacheStoreHolder(FilePageStore idxStore, FilePageStore[] partStores) {
            this.idxStore = idxStore;
            this.partStores = partStores;
        }
    }

}
