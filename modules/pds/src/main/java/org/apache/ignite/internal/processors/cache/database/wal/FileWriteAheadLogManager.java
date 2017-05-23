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

package org.apache.ignite.internal.processors.cache.database.wal;

import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistenceConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.database.wal.record.HeaderRecord;
import org.apache.ignite.internal.processors.cache.database.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * File WAL manager.
 */
public class FileWriteAheadLogManager extends GridCacheSharedManagerAdapter implements IgniteWriteAheadLogManager {
    /** */
    public static final String WAL_SEGMENT_FILE_EXT = ".wal";

    /** */
    private static final byte[] FILL_BUF = new byte[1024 * 1024];

    /** */
    private static final Pattern WAL_NAME_PATTERN = Pattern.compile("\\d{16}\\.v\\d+\\.wal");

    /** */
    private static final Pattern WAL_TEMP_NAME_PATTERN = Pattern.compile("\\d{16}\\.v\\d+\\.wal\\.tmp");

    /** */
    private static final FileFilter WAL_SEGMENT_FILE_FILTER = new FileFilter() {
        @Override public boolean accept(File file) {
            return !file.isDirectory() && WAL_NAME_PATTERN.matcher(file.getName()).matches();
        }
    };

    /** */
    private static final FileFilter WAL_SEGMENT_TEMP_FILE_FILTER = new FileFilter() {
        @Override public boolean accept(File file) {
            return !file.isDirectory() && WAL_TEMP_NAME_PATTERN.matcher(file.getName()).matches();
        }
    };

    /** */
    public static final String IGNITE_PDS_WAL_MODE = "IGNITE_PDS_WAL_MODE";

    /** */
    public static final String IGNITE_PDS_WAL_TLB_SIZE = "IGNITE_PDS_WAL_TLB_SIZE";

    /** */
    public static final String IGNITE_PDS_WAL_FLUSH_FREQ = "IGNITE_PDS_WAL_FLUSH_FREQUENCY";

    /** */
    public static final String IGNITE_PDS_WAL_FSYNC_DELAY = "IGNITE_PDS_WAL_FSYNC_DELAY"; // TODO may be move to config

    /** Ignite pds wal record iterator buffer size. */
    public static final String IGNITE_PDS_WAL_RECORD_ITERATOR_BUFFER_SIZE = "IGNITE_PDS_WAL_RECORD_ITERATOR_BUFFER_SIZE";

    /** */
    public static final String IGNITE_PDS_WAL_ALWAYS_WRITE_FULL_PAGES = "IGNITE_PDS_WAL_ALWAYS_WRITE_FULL_PAGES";

    /** */
    private static long fsyncDelayNanos = IgniteSystemProperties.getLong(IGNITE_PDS_WAL_FSYNC_DELAY, 1);

    /** */
    public final int tlbSize = IgniteSystemProperties.getInteger(IGNITE_PDS_WAL_TLB_SIZE, 128 * 1024);

    /** WAL flush frequency. Makes sense only for BACKGROUND log mode. */
    public static final int FLUSH_FREQ = IgniteSystemProperties.getInteger(IGNITE_PDS_WAL_FLUSH_FREQ, 2_000);

    /** */
    private final boolean alwaysWriteFullPages =
        IgniteSystemProperties.getBoolean(IGNITE_PDS_WAL_ALWAYS_WRITE_FULL_PAGES, false);

    /** */
    private long maxWalSegmentSize;

    /** */
    private final PersistenceConfiguration dbCfg;

    /** */
    private IgniteConfiguration igCfg;

    /** */
    private File walWorkDir;

    /** */
    private File walArchiveDir;

    /** */
    private volatile FileWriteHandle currentHnd;

    /** */
    private static final AtomicReferenceFieldUpdater<FileWriteAheadLogManager, FileWriteHandle> currentHndUpd =
        AtomicReferenceFieldUpdater.newUpdater(FileWriteAheadLogManager.class, FileWriteHandle.class, "currentHnd");

    /** */
    private final ThreadLocal<ByteBuffer> tlb = new ThreadLocal<ByteBuffer>() {
        @Override protected ByteBuffer initialValue() {
            ByteBuffer buf = ByteBuffer.allocateDirect(tlbSize);

            buf.order(GridUnsafe.NATIVE_BYTE_ORDER);

            return buf;
        }
    };

    /** */
    private RecordSerializer serializer;

    /** */
    private volatile FileArchiver archiver;

    /** */
    private final Mode mode;

    /** */
    private QueueFlusher flusher;

    /** */
    private ThreadLocal<WALPointer> lastWALPtr = new ThreadLocal<>();

    /**
     * @param ctx Kernal context.
     */
    public FileWriteAheadLogManager(GridKernalContext ctx) {
        igCfg = ctx.config();

        PersistenceConfiguration dbCfg = igCfg.getPersistenceConfiguration();

        assert dbCfg != null : "WAL should not be created if persistence is disabled.";

        this.dbCfg = dbCfg;
        this.igCfg = igCfg;

        maxWalSegmentSize = dbCfg.getWalSegmentSize();

        String modeStr = IgniteSystemProperties.getString(IGNITE_PDS_WAL_MODE);
        mode = modeStr == null ? Mode.DEFAULT : Mode.valueOf(modeStr.trim().toUpperCase());
    }

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        String consId = consistentId();

        if (!cctx.kernalContext().clientNode()) {
            A.notNullOrEmpty(consId, "consistentId");

            consId = U.maskForFileName(consId);

            checkWalConfiguration();
            walWorkDir = initDirectory(dbCfg.getWalStorePath(), "db/wal", consId, "write ahead log work directory");
            walArchiveDir = initDirectory(dbCfg.getWalArchivePath(), "db/wal/archive", consId,
                "write ahead log archive directory");

            serializer = new RecordV1Serializer(cctx);

            checkOrPrepareFiles();

            archiver = new FileArchiver();

            if (mode != Mode.DEFAULT) {
                if (log.isInfoEnabled())
                    log.info("Started write-ahead log manager [mode=" + mode + ']');
            }
        }
    }

    /**
     * @throws IgniteCheckedException if WAL store path is configured and archive path isn't (or vice versa)
     */
    private void checkWalConfiguration() throws IgniteCheckedException {
        if (dbCfg.getWalStorePath() == null ^ dbCfg.getWalArchivePath() == null) {
            throw new IgniteCheckedException("Properties should be either both specified or both null " +
                "[walStorePath = " + dbCfg.getWalStorePath() + ", walArchivePath = " + dbCfg.getWalArchivePath() + "]");
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0(boolean reconnect) throws IgniteCheckedException {
        super.onKernalStart0(reconnect);

        if (!cctx.kernalContext().clientNode() && cctx.kernalContext().state().active())
            archiver.start();
    }

    /**
     * @return Consistent ID.
     */
    protected String consistentId() {
        return cctx.discovery().consistentId().toString();
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        FileWriteHandle currentHnd = currentHandle();

        try {
            QueueFlusher flusher0 = flusher;

            if (flusher0 != null) {
                flusher0.shutdown();

                if (currentHnd != null)
                    currentHnd.flush((FileWALPointer)null);
            }

            if (currentHnd != null)
                currentHnd.close(false);

            if (archiver != null)
                archiver.shutdown();
        }
        catch (Exception e) {
            U.error(log, "Failed to gracefully close WAL segment: " + currentHnd.file, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activate file write ahead log [nodeId=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        start0();

        if (!cctx.kernalContext().clientNode()) {
            archiver = new FileArchiver();

            archiver.start();
        }
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate file write ahead log [nodeId=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        stop0(true);

        currentHnd = null;
    }

    /** {@inheritDoc} */
    @Override public boolean isAlwaysWriteFullPages() {
        return alwaysWriteFullPages;
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSync() {
        return mode == Mode.DEFAULT;
    }

    /** {@inheritDoc} */
    @Override public void resumeLogging(WALPointer lastPtr) throws IgniteCheckedException {
        try {
            assert currentHnd == null;
            assert lastPtr == null || lastPtr instanceof FileWALPointer;

            FileWALPointer filePtr = (FileWALPointer)lastPtr;

            currentHnd = restoreWriteHandle(filePtr);

            if (mode == Mode.BACKGROUND) {
                flusher = new QueueFlusher(cctx.igniteInstanceName());

                flusher.start();
            }
        }
        catch (StorageException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("TooBroadScope")
    @Override public WALPointer log(WALRecord record) throws IgniteCheckedException, StorageException {
        if (serializer == null || mode == Mode.NONE)
            return null;

        FileWriteHandle current = currentHandle();

        // Logging was not resumed yet.
        if (current == null)
            return null;

        // Need to calculate record size first.
        record.size(serializer.size(record));

        for (; ; current = rollOver(current)) {
            WALPointer ptr = current.addRecord(record);

            if (ptr != null) {
                lastWALPtr.set(ptr);

                return ptr;
            }

            if (isStopping())
                throw new IgniteCheckedException("Stopping.");
        }
    }

    /** {@inheritDoc} */
    @Override public void fsync(WALPointer ptr) throws IgniteCheckedException, StorageException {
        if (serializer == null || mode == Mode.NONE || mode == Mode.BACKGROUND)
            return;

        FileWriteHandle cur = currentHandle();

        // WAL manager was not started (client node).
        if (cur == null)
            return;

        FileWALPointer filePtr = (FileWALPointer)(ptr == null ? lastWALPtr.get() : ptr);

        if (mode == Mode.LOG_ONLY) {
            cur.flushOrWait(filePtr);

            return;
        }

        // No need to sync if was rolled over.
        if (filePtr != null && !cur.needFsync(filePtr))
            return;

        cur.fsync(filePtr);
    }

    /** {@inheritDoc} */
    @Override public WALIterator replay(WALPointer start)
        throws IgniteCheckedException, StorageException {
        assert start == null || start instanceof FileWALPointer : "Invalid start pointer: " + start;

        FileWriteHandle hnd = currentHandle();

        FileWALPointer end = null;

        if (hnd != null)
            end = hnd.position();

        return new RecordsIterator(cctx, walWorkDir, walArchiveDir, (FileWALPointer)start, end, dbCfg, serializer,
            archiver, log, tlbSize);
    }

    /** {@inheritDoc} */
    @Override public boolean reserve(WALPointer start) throws IgniteCheckedException {
        assert start != null && start instanceof FileWALPointer : "Invalid start pointer: " + start;

        if (mode == Mode.NONE)
            return false;

        FileArchiver archiver0 = archiver;

        if (archiver0 == null)
            throw new IgniteCheckedException("Could not reserve WAL segment: archiver == null");

        archiver0.reserve(((FileWALPointer)start).index());

        if (!hasIndex(((FileWALPointer)start).index())) {
            archiver0.release(((FileWALPointer)start).index());

            return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void release(WALPointer start) throws IgniteCheckedException {
        assert start != null && start instanceof FileWALPointer : "Invalid start pointer: " + start;

        if (mode == Mode.NONE)
            return;

        FileArchiver archiver0 = archiver;

        if (archiver0 == null)
            throw new IgniteCheckedException("Could not release WAL segment: archiver == null");

        archiver0.release(((FileWALPointer)start).index());
    }

    private boolean hasIndex(int absIdx) {
        String name = FileDescriptor.fileName(absIdx, serializer.version());

        boolean inArchive = new File(walArchiveDir, name).exists();

        if (inArchive)
            return true;

        if (absIdx <= archiver.lastArchivedIndex())
            return false;

        FileWriteHandle cur = currentHnd;

        return cur != null && cur.idx >= absIdx;
    }

    /** {@inheritDoc} */
    @Override public int truncate(WALPointer ptr) {
        if (ptr == null)
            return 0;

        assert ptr instanceof FileWALPointer : ptr;

        FileWALPointer fPtr = (FileWALPointer)ptr;

        FileDescriptor[] descs = scan(walArchiveDir.listFiles(WAL_SEGMENT_FILE_FILTER));

        int deleted = 0;

        FileArchiver archiver0 = archiver;

        for (FileDescriptor desc : descs) {
            // Do not delete reserved segment and any segment after it.
            if (archiver0 != null && archiver0.reserved(desc.idx))
                return deleted;

            // We need to leave at least one archived segment to correctly determine the archive index.
            if (desc.idx + 1 < fPtr.index()) {
                if (!desc.file.delete())
                    U.warn(log, "Failed to remove obsolete WAL segment (make sure the process has enough rights): " +
                        desc.file.getAbsolutePath());
                else
                    deleted++;
            }
        }

        return deleted;
    }

    /** {@inheritDoc} */
    @Override public boolean reserved(WALPointer ptr) {
        FileWALPointer fPtr = (FileWALPointer)ptr;

        FileArchiver archiver0 = archiver;

        return archiver0 != null && archiver0.reserved(fPtr.index());
    }

    /**
     * Creates a directory specified by the given arguments.
     *
     * @param cfg Configured directory path, may be {@code null}.
     * @param defDir Default directory path, will be used if cfg is {@code null}.
     * @param consId Local node consistent ID.
     * @param msg File description to print out on successful initialization.
     * @return Initialized directory.
     * @throws IgniteCheckedException
     */
    private File initDirectory(String cfg, String defDir, String consId, String msg) throws IgniteCheckedException {
        File dir;

        if (cfg != null) {
            File workDir0 = new File(cfg);

            //TODO check path
            dir = workDir0.isAbsolute() ?
                new File(workDir0, consId) :
                new File(U.resolveWorkDirectory(igCfg.getWorkDirectory(), cfg, false), consId);
        }
        else
            dir = new File(U.resolveWorkDirectory(igCfg.getWorkDirectory(), defDir, false), consId);

        U.ensureDirectory(dir, msg, log);

        return dir;
    }

    /**
     * @return Current log segment handle.
     */
    private FileWriteHandle currentHandle() {
        return currentHnd;
    }

    /**
     * @param cur Handle that failed to fit the given entry.
     * @return Handle that will fit the entry.
     */
    private FileWriteHandle rollOver(FileWriteHandle cur) throws StorageException, IgniteCheckedException {
        FileWriteHandle hnd = currentHandle();

        if (hnd != cur)
            return hnd;

        if (hnd.close(true)) {
            FileWriteHandle next = initNextWriteHandle(cur.idx);

            boolean swapped = currentHndUpd.compareAndSet(this, hnd, next);

            assert swapped : "Concurrent updates on rollover are not allowed";

            hnd.signalNextAvailable();
        }
        else
            hnd.awaitNext();

        return currentHandle();
    }

    /**
     * @param lastReadPtr Last read WAL file pointer.
     * @return Initialized file write handle.
     * @throws IgniteCheckedException If failed to initialize WAL write handle.
     */
    private FileWriteHandle restoreWriteHandle(FileWALPointer lastReadPtr) throws IgniteCheckedException {
        int absIdx = lastReadPtr == null ? 0 : lastReadPtr.index();

        archiver.currentWalIndex(absIdx);

        int segNo = absIdx % dbCfg.getWalSegments();

        File curFile = new File(walWorkDir, FileDescriptor.fileName(segNo, serializer.version()));

        int offset = lastReadPtr == null ? 0 : lastReadPtr.fileOffset();
        int len = lastReadPtr == null ? 0 : lastReadPtr.length();

        log.info("Resuming logging in WAL segment [file=" + curFile.getAbsolutePath() +
            ", offset=" + offset + ']');

        try {
            RandomAccessFile file = new RandomAccessFile(curFile, "rw");

            try {
                FileWriteHandle hnd = new FileWriteHandle(
                    file,
                    absIdx,
                    cctx.igniteInstanceName(),
                    offset + len,
                    maxWalSegmentSize,
                    serializer);

                if (lastReadPtr == null) {
                    HeaderRecord header = new HeaderRecord(serializer.version());

                    header.size(serializer.size(header));

                    hnd.addRecord(header);
                }

                return hnd;
            }
            catch (IgniteCheckedException | IOException e) {
                file.close();

                throw e;
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to restore WAL write handle: " + curFile.getAbsolutePath(), e);
        }
    }

    /**
     * Fills the file header for a new segment.
     *
     * @return Initialized file handle.
     * @throws StorageException If IO exception occurred.
     * @throws IgniteCheckedException If failed.
     */
    private FileWriteHandle initNextWriteHandle(int curIdx) throws StorageException, IgniteCheckedException {
        try {
            File nextFile = pollNextFile(curIdx);

            if (log.isDebugEnabled())
                log.debug("Switching to a new WAL segment: " + nextFile.getAbsolutePath());

            RandomAccessFile file = new RandomAccessFile(nextFile, "rw");

            FileWriteHandle hnd = new FileWriteHandle(
                file,
                curIdx + 1,
                cctx.igniteInstanceName(),
                0,
                maxWalSegmentSize,
                serializer);

            HeaderRecord header = new HeaderRecord(serializer.version());

            header.size(serializer.size(header));

            hnd.addRecord(header);

            return hnd;
        }
        catch (IOException e) {
            throw new StorageException(e);
        }
    }

    /**
     *
     */
    private void checkOrPrepareFiles() throws IgniteCheckedException {
        // Clean temp files.
        {
            File[] tmpFiles = walWorkDir.listFiles(WAL_SEGMENT_TEMP_FILE_FILTER);

            if (!F.isEmpty(tmpFiles)) {
                for (File tmp : tmpFiles) {
                    boolean deleted = tmp.delete();

                    if (!deleted)
                        throw new IgniteCheckedException("Failed to delete previously created temp file " +
                            "(make sure Ignite process has enough rights): " + tmp.getAbsolutePath());
                }
            }
        }

        File[] allFiles = walWorkDir.listFiles(WAL_SEGMENT_FILE_FILTER);

        if (allFiles.length != 0 && allFiles.length > dbCfg.getWalSegments())
            throw new IgniteCheckedException("Failed to initialize wal (work directory contains " +
                "incorrect number of segments) [cur=" + allFiles.length + ", expected=" + dbCfg.getWalSegments() + ']');

        // Allocate the first segment synchronously. All other segments will be allocated by archiver in background.
        if (allFiles.length == 0) {
            File first = new File(walWorkDir, FileDescriptor.fileName(0, serializer.version()));

            createFile(first);
        }
        else
            checkFiles(0, false, null);
    }

    /**
     * Clears the file with zeros.
     *
     * @param file File to format.
     */
    private void formatFile(File file) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Formatting file [exists=" + file.exists() + ", file=" + file.getAbsolutePath() + ']');

        try (RandomAccessFile rnd = new RandomAccessFile(file, "rw")) {
            int left = dbCfg.getWalSegmentSize();

            if (mode == Mode.DEFAULT) {
                while (left > 0) {
                    int toWrite = Math.min(FILL_BUF.length, left);

                    rnd.write(FILL_BUF, 0, toWrite);

                    left -= toWrite;
                }

                rnd.getChannel().force(false);
            }
            else {
                rnd.setLength(0);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to format WAL segment file: " + file.getAbsolutePath(), e);
        }
    }

    /**
     * Creates a file atomically with temp file.
     *
     * @param file File to create.
     * @throws IgniteCheckedException If failed.
     */
    private void createFile(File file) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Creating new file [exists=" + file.exists() + ", file=" + file.getAbsolutePath() + ']');

        File tmp = new File(file.getParent(), file.getName() + ".tmp");

        formatFile(tmp);

        try {
            Files.move(tmp.toPath(), file.toPath());
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to move temp file to a regular WAL segment file: " +
                file.getAbsolutePath(), e);
        }

        if (log.isDebugEnabled())
            log.debug("Created WAL segment [file=" + file.getAbsolutePath() + ", size=" + file.length() + ']');
    }

    /**
     * @param curIdx Current absolute WAL segment index.
     * @return File.
     * @throws IgniteCheckedException If failed.
     */
    private File pollNextFile(int curIdx) throws IgniteCheckedException {
        // Signal to archiver that we are done with the segment and it can be archived.
        int absNextIdx = archiver.nextAbsoluteSegmentIndex(curIdx);

        int segmentIdx = absNextIdx % dbCfg.getWalSegments();

        return new File(walWorkDir, FileDescriptor.fileName(segmentIdx, serializer.version()));
    }

    /**
     * @param ver Serializer version.
     * @return Entry serializer.
     */
    private static RecordSerializer forVersion(GridCacheSharedContext cctx, int ver) throws IgniteCheckedException {
        if (ver <= 0)
            throw new IgniteCheckedException("Failed to create a serializer (corrupted WAL file).");

        switch (ver) {
            case 1:
                return new RecordV1Serializer(cctx);

            default:
                throw new IgniteCheckedException("Failed to create a serializer with the given version " +
                    "(forward compatibility is not supported): " + ver);
        }
    }

    /**
     * @return Sorted WAL files descriptors.
     */
    private static FileDescriptor[] scan(File[] allFiles) {
        if (allFiles == null)
            return new FileDescriptor[0];

        FileDescriptor[] descs = new FileDescriptor[allFiles.length];

        for (int i = 0; i < allFiles.length; i++) {
            File f = allFiles[i];

            descs[i] = new FileDescriptor(f);
        }

        Arrays.sort(descs);

        return descs;
    }

    /**
     * File archiver operates on absolute segment indexes. For any given absolute segment index N we can calculate
     * the work WAL segment: S(N) = N % dbCfg.walSegments.
     * When a work segment is finished, it is given to the archiver. If the absolute index of last archived segment
     * is denoted by A and the absolute index of next segment we want to write is denoted by W, then we can allow
     * write to S(W) if W - A <= walSegments.
     */
    private class FileArchiver extends Thread {
        /** */
        private IgniteCheckedException cleanException;

        /** Absolute current segment index. */
        private int curAbsWalIdx = -1;

        /** */
        private int lastAbsArchivedIdx = -1;

        /** */
        private volatile boolean stopped;

        /** */
        private NavigableMap<Integer, Integer> reserved = new TreeMap<>();

        /** */
        private Map<Integer, Integer> locked = new HashMap<>();

        /**
         *
         */
        private FileArchiver() {
            super("wal-file-archiver%" + cctx.igniteInstanceName());

            lastAbsArchivedIdx = lastArchivedIndex();
        }

        /**
         * @throws IgniteInterruptedCheckedException If failed to wait for thread shutdown.
         */
        private void shutdown() throws IgniteInterruptedCheckedException {
            synchronized (this) {
                stopped = true;

                notifyAll();
            }

            U.join(this);
        }

        /**
         * @param curAbsWalIdx Current absolute WAL segment index.
         */
        private void currentWalIndex(int curAbsWalIdx) {
            synchronized (this) {
                this.curAbsWalIdx = curAbsWalIdx;

                notifyAll();
            }
        }

        /**
         * @param absIdx Index for reservation.
         */
        private synchronized void reserve(int absIdx) {
            Integer cur = reserved.get(absIdx);

            if (cur == null)
                reserved.put(absIdx, 1);
            else
                reserved.put(absIdx, cur + 1);
        }

        /**
         * @param absIdx Index for reservation.
         * @return {@code True} if index is reserved.
         */
        private synchronized boolean reserved(int absIdx) {
            return locked.containsKey(absIdx) || reserved.floorKey(absIdx) != null;
        }

        /**
         * @param absIdx Reserved index.
         */
        private synchronized void release(int absIdx) {
            Integer cur = reserved.get(absIdx);

            assert cur != null && cur >= 1 : cur;

            if (cur == 1)
                reserved.remove(absIdx);
            else
                reserved.put(absIdx, cur - 1);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                allocateRemainingFiles();
            }
            catch (IgniteCheckedException e) {
                synchronized (this) {
                    // Stop the thread and report to starter.
                    cleanException = e;

                    notifyAll();

                    return;
                }
            }

            try {
                synchronized (this) {
                    while (curAbsWalIdx == -1 && !stopped)
                        wait();

                    if (curAbsWalIdx != 0 && lastAbsArchivedIdx == -1)
                        lastAbsArchivedIdx = curAbsWalIdx - 1;
                }

                while (!Thread.currentThread().isInterrupted() && !stopped) {
                    int toArchive;

                    synchronized (this) {
                        assert lastAbsArchivedIdx <= curAbsWalIdx : "lastArchived=" + lastAbsArchivedIdx +
                            ", current=" + curAbsWalIdx;

                        while (lastAbsArchivedIdx >= curAbsWalIdx - 1 && !stopped)
                            wait();

                        toArchive = lastAbsArchivedIdx + 1;
                    }

                    if (stopped)
                        break;

                    try {
                        File workFile = archiveSegment(toArchive);

                        synchronized (this) {
                            while (locked.containsKey(toArchive) && !stopped)
                                wait();

                            // Firstly, format working file
                            if (!stopped)
                                formatFile(workFile);

                            // Then increase counter to allow rollover on clean working file
                            lastAbsArchivedIdx = toArchive;

                            notifyAll();
                        }
                    }
                    catch (IgniteCheckedException e) {
                        synchronized (this) {
                            cleanException = e;

                            notifyAll();
                        }
                    }
                }
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }

        /**
         * Gets the absolute index of the next WAL segment available to write.
         *
         * @param curIdx Current index that we want to increment.
         * @return Next index (curIdx+1) when it is ready to be written.
         * @throws IgniteCheckedException If failed.
         */
        private int nextAbsoluteSegmentIndex(int curIdx) throws IgniteCheckedException {
            try {
                synchronized (this) {
                    if (cleanException != null)
                        throw cleanException;

                    assert curIdx == curAbsWalIdx;

                    curAbsWalIdx++;

                    // Notify archiver thread.
                    notifyAll();

                    while (curAbsWalIdx - lastAbsArchivedIdx > dbCfg.getWalSegments() && cleanException == null)
                        wait();

                    return curAbsWalIdx;
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteInterruptedCheckedException(e);
            }
        }

        /**
         * @param absIdx Segment absolute index.
         * @return {@code True} if can read, {@code false} if work segment
         */
        @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
        private boolean checkCanReadArchiveOrReserveWorkSegment(int absIdx) {
            synchronized (this) {
                if (lastAbsArchivedIdx >= absIdx)
                    return true;

                Integer cur = locked.get(absIdx);

                cur = cur == null ? 1 : cur + 1;

                locked.put(absIdx, cur);

                if (log.isDebugEnabled())
                    log.debug("Reserved work segment [absIdx=" + absIdx + ", pins=" + cur + ']');

                return false;
            }
        }

        /**
         * @param absIdx Segment absolute index.
         */
        @SuppressWarnings("NonPrivateFieldAccessedInSynchronizedContext")
        private void releaseWorkSegment(int absIdx) {
            synchronized (this) {
                Integer cur = locked.get(absIdx);

                assert cur != null && cur > 0;

                if (cur == 1) {
                    locked.remove(absIdx);

                    if (log.isDebugEnabled())
                        log.debug("Fully released work segment (ready to archive) [absIdx=" + absIdx + ']');
                }
                else {
                    locked.put(absIdx, cur - 1);

                    if (log.isDebugEnabled())
                        log.debug("Partially released work segment [absIdx=" + absIdx + ", pins=" + (cur - 1) + ']');
                }

                notifyAll();
            }
        }

        /**
         * @param absIdx Absolute index to archive.
         */
        private File archiveSegment(int absIdx) throws IgniteCheckedException {
            int segIdx = absIdx % dbCfg.getWalSegments();

            File origFile = new File(walWorkDir, FileDescriptor.fileName(segIdx, serializer.version()));

            String name = FileDescriptor.fileName(absIdx, serializer.version());

            File dstTmpFile = new File(walArchiveDir, name + ".tmp");

            File dstFile = new File(walArchiveDir, name);

            if (log.isDebugEnabled())
                log.debug("Starting to copy WAL segment [absIdx=" + absIdx + ", segIdx=" + segIdx +
                    ", origFile=" + origFile.getAbsolutePath() + ", dstFile=" + dstFile.getAbsolutePath() + ']');

            try {
                Files.deleteIfExists(dstTmpFile.toPath());

                Files.copy(origFile.toPath(), dstTmpFile.toPath());

                Files.move(dstTmpFile.toPath(), dstFile.toPath());

                if (mode == Mode.DEFAULT) {
                    try (RandomAccessFile f0 = new RandomAccessFile(dstFile, "rw")) {
                        f0.getChannel().force(false);
                    }
                }
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to archive WAL segment [" +
                    "srcFile=" + origFile.getAbsolutePath() +
                    ", dstFile=" + dstTmpFile.getAbsolutePath() + ']', e);
            }

            if (log.isDebugEnabled())
                log.debug("Copied file [src=" + origFile.getAbsolutePath() +
                    ", dst=" + dstFile.getAbsolutePath() + ']');

            return origFile;
        }

        /**
         * Lists files in archive directory and returns the index of last archived file.
         *
         * @return The absolute index of last archived file.
         */
        private int lastArchivedIndex() {
            int lastIdx = -1;

            for (File file : walArchiveDir.listFiles(WAL_SEGMENT_FILE_FILTER)) {
                try {
                    int idx = Integer.parseInt(file.getName().substring(0, 16));

                    lastIdx = Math.max(lastIdx, idx);
                }
                catch (NumberFormatException | IndexOutOfBoundsException ignore) {

                }
            }

            return lastIdx;
        }

        /**
         *
         */
        private boolean checkStop() {
            return stopped;
        }

        /**
         *
         */
        private void allocateRemainingFiles() throws IgniteCheckedException {
            checkFiles(1, true, new IgnitePredicate<Integer>() {
                @Override public boolean apply(Integer integer) {
                    return !checkStop();
                }
            });
        }
    }

    /**
     * Validate files depending on {@link PersistenceConfiguration#getWalSegments()}  and create if need.
     * Check end when exit condition return false or all files are passed.
     *
     * @param startWith Start with.
     * @param create Flag create file.
     * @param p Predicate Exit condition.
     * @throws IgniteCheckedException if validation or create file fail.
     */
    private void checkFiles(int startWith, boolean create, IgnitePredicate<Integer> p) throws IgniteCheckedException {
        for (int i = startWith; i < dbCfg.getWalSegments() && (p == null || (p != null && p.apply(i))); i++) {
            File checkFile = new File(walWorkDir, FileDescriptor.fileName(i, serializer.version()));

            if (checkFile.exists()) {
                if (checkFile.isDirectory())
                    throw new IgniteCheckedException("Failed to initialize WAL log segment (a directory with " +
                        "the same name already exists): " + checkFile.getAbsolutePath());
                else if (checkFile.length() != dbCfg.getWalSegmentSize() && mode == Mode.DEFAULT)
                    throw new IgniteCheckedException("Failed to initialize WAL log segment " +
                        "(WAL segment size change is not supported):" + checkFile.getAbsolutePath());
            }
            else if (create)
                createFile(checkFile);
        }
    }

    /**
     * WAL file descriptor.
     */
    private static class FileDescriptor implements Comparable<FileDescriptor> {
        /** */
        protected final File file;

        /** */
        protected final int idx;

        /** */
        protected final int ver;

        /**
         * @param file File.
         */
        private FileDescriptor(File file) {
            this(file, null);
        }

        /**
         * @param file File.
         * @param idx index
         */
        private FileDescriptor(File file, Integer idx) {
            this.file = file;

            String fileName = file.getName();

            assert fileName.endsWith(WAL_SEGMENT_FILE_EXT);

            int v = fileName.lastIndexOf(".v");

            assert v > 0;

            int begin = v + 2;
            int end = fileName.length() - WAL_SEGMENT_FILE_EXT.length();

            if (idx == null)
                this.idx = Integer.parseInt(fileName.substring(0, v));
            else
                this.idx = idx;

            ver = Integer.parseInt(fileName.substring(begin, end));
        }

        /**
         * @param segment Segment index.
         * @param ver Serializer version.
         * @return Segment file name.
         */
        private static String fileName(long segment, int ver) {
            SB b = new SB();

            String segmentStr = Long.toString(segment);

            for (int i = segmentStr.length(); i < 16; i++)
                b.a('0');

            b.a(segmentStr).a(".v").a(ver).a(WAL_SEGMENT_FILE_EXT);

            return b.toString();
        }

        /**
         * @param segment Segment number as integer.
         * @return Segment number as aligned string.
         */
        private static String segmentNumber(long segment) {
            SB b = new SB();

            String segmentStr = Long.toString(segment);

            for (int i = segmentStr.length(); i < 16; i++)
                b.a('0');

            b.a(segmentStr);

            return b.toString();
        }

        /** {@inheritDoc} */
        @Override public int compareTo(FileDescriptor o) {
            return Long.compare(idx, o.idx);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof FileDescriptor))
                return false;

            FileDescriptor that = (FileDescriptor)o;

            return idx == that.idx;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return idx;
        }
    }

    /**
     *
     */
    private abstract static class FileHandle {
        /** */
        protected RandomAccessFile file;

        /** */
        protected FileChannel ch;

        /** */
        protected final int idx;

        /** */
        protected String gridName;

        /**
         * @param file File.
         * @param idx Index.
         */
        private FileHandle(RandomAccessFile file, int idx, String gridName) {
            this.file = file;
            this.idx = idx;
            this.gridName = gridName;

            ch = file.getChannel();

            assert ch != null;
        }
    }

    private static class ReadFileHandle extends FileHandle {
        /** Entry serializer. */
        private RecordSerializer ser;

        /** */
        private FileInput in;

        /** */
        private boolean workDir;

        /**
         * @param file File to read.
         * @param idx File index.
         * @param ser Entry serializer.
         * @param in File input.
         */
        private ReadFileHandle(
            RandomAccessFile file,
            int idx,
            String gridName,
            RecordSerializer ser,
            FileInput in
        ) {
            super(file, idx, gridName);

            this.ser = ser;
            this.in = in;
        }

        /**
         * @throws IgniteCheckedException If failed to close the WAL segment file.
         */
        public void close() throws IgniteCheckedException {
            try {
                file.close();
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }
        }
    }

    /**
     * File handle for one log segment.
     */
    @SuppressWarnings("SignalWithoutCorrespondingAwait")
    private class FileWriteHandle extends FileHandle {
        /** */
        private final RecordSerializer serializer;

        /** */
        private final long maxSegmentSize;

        /** */
        private final AtomicReference<WALRecord> head = new AtomicReference<>();

        /** */
        private volatile long written;

        /** */
        private volatile long lastFsyncPos;

        /** Environment failure. */
        private volatile Throwable envFailed;

        /** */
        private final AtomicBoolean stop = new AtomicBoolean(false);

        /** */
        private final Lock lock = new ReentrantLock();

        /** */
        private final Condition writeComplete = lock.newCondition();

        /** */
        private final Condition fsync = lock.newCondition();

        /** */
        private final Condition nextSegment = lock.newCondition();

        /**
         * @param file Mapped file to use.
         * @param idx Index for easy access.
         * @param pos Position.
         * @param maxSegmentSize Max segment size.
         * @param serializer Serializer.
         * @throws IOException If failed.
         */
        private FileWriteHandle(
            RandomAccessFile file,
            int idx,
            String gridName,
            long pos,
            long maxSegmentSize,
            RecordSerializer serializer
        ) throws IOException {
            super(file, idx, gridName);

            assert serializer != null;

            ch.position(pos);

            this.maxSegmentSize = maxSegmentSize;
            this.serializer = serializer;

            head.set(new FakeRecord(pos));
            written = pos;
            lastFsyncPos = pos;
        }

        /**
         * @param rec Record.
         * @return Pointer.
         * @throws StorageException If failed.
         * @throws IgniteCheckedException If failed.
         */
        private WALPointer addRecord(WALRecord rec) throws StorageException, IgniteCheckedException {
            assert rec.size() > 0 || rec.getClass() == FakeRecord.class;

            boolean flushed = false;

            for (; ; ) {
                WALRecord h = head.get();

                long nextPos = nextPosition(h);

                // It is important that we read `stop` after `head` in this loop for correct close,
                // because otherwise we will have a race on the last flush in close.
                if (nextPos + rec.size() >= maxSegmentSize || stop.get()) {
                    // Can not write to this segment, need to switch to the next one.
                    return null;
                }

                int newChainSize = h.chainSize() + rec.size();

                if (newChainSize > tlbSize && !flushed) {
                    boolean res = h.previous() == null || flush(h);

                    if (rec.size() > tlbSize)
                        flushed = res;

                    continue;
                }

                rec.chainSize(newChainSize);
                rec.previous(h);
                rec.position(nextPos);

                if (head.compareAndSet(h, rec))
                    return new FileWALPointer(idx, (int)rec.position(), rec.size());
            }
        }

        /**
         * @param rec Record.
         * @return Position for the next record.
         */
        private long nextPosition(WALRecord rec) {
            return rec.position() + rec.size();
        }

        /**
         * Flush or wait for concurrent flush completion.
         *
         * @param ptr Pointer.
         * @throws IgniteCheckedException If failed.
         */
        private void flushOrWait(FileWALPointer ptr) throws IgniteCheckedException {
            long expWritten;

            if (ptr != null) {
                // If requested obsolete file index, it must be already flushed by close.
                if (ptr.index() != idx)
                    return;

                expWritten = ptr.fileOffset();
            }
            else // We read head position before the flush because otherwise we can get wrong position.
                expWritten = head.get().position();

            if (flush(ptr))
                return;

            // Spin-wait for a while before acquiring the lock.
            for (int i = 0; i < 64; i++) {
                if (written >= expWritten)
                    return;
            }

            // If we did not flush ourselves then await for concurrent flush to complete.
            lock.lock();

            try {
                while (written < expWritten && envFailed == null)
                    U.await(writeComplete);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param ptr Pointer.
         * @return {@code true} If the flush really happened.
         * @throws IgniteCheckedException If failed.
         * @throws StorageException If failed.
         */
        private boolean flush(FileWALPointer ptr) throws IgniteCheckedException, StorageException {
            if (ptr == null) { // Unconditional flush.
                for (; ; ) {
                    WALRecord expHead = head.get();

                    if (expHead.previous() == null) {
                        assert expHead instanceof FakeRecord;

                        return false;
                    }

                    if (flush(expHead))
                        return true;
                }
            }

            assert ptr.index() == idx;

            for (; ; ) {
                WALRecord h = head.get();

                // If current chain begin position is greater than requested, then someone else flushed our changes.
                if (chainBeginPosition(h) > ptr.fileOffset())
                    return false;

                if (flush(h))
                    return true; // We are lucky.
            }
        }

        /**
         * @param h Head of the chain.
         * @return Chain begin position.
         */
        private long chainBeginPosition(WALRecord h) {
            return h.position() + h.size() - h.chainSize();
        }

        /**
         * @param expHead Expected head of chain.
         * @throws IgniteCheckedException If failed.
         * @throws StorageException If failed.
         */
        private boolean flush(WALRecord expHead) throws StorageException, IgniteCheckedException {
            if (expHead.previous() == null) {
                assert expHead instanceof FakeRecord;

                return false;
            }

            // Fail-fast before CAS.
            checkEnvironment();

            if (!head.compareAndSet(expHead, new FakeRecord(nextPosition(expHead))))
                return false;

            // At this point we grabbed the piece of WAL chain.
            // Any failure in this code must invalidate the environment.
            try {
                // We can safely allow other threads to start building next chains while we are doing flush here.
                ByteBuffer buf;

                boolean tmpBuf = false;

                if (expHead.chainSize() > tlbSize) {
                    buf = GridUnsafe.allocateBuffer(expHead.chainSize());

                    tmpBuf = true; // We need to manually release this temporary direct buffer.
                }
                else
                    buf = tlb.get();

                try {
                    long pos = fillBuffer(buf, expHead);

                    writeBuffer(pos, buf);
                }
                finally {
                    if (tmpBuf)
                        GridUnsafe.freeBuffer(buf);
                }

                return true;
            }
            catch (Throwable e) {
                invalidateEnvironment(e);

                throw e;
            }
        }

        /**
         * @param buf Buffer.
         * @param head Head of the chain to write to the buffer.
         * @return Position in file for this buffer.
         * @throws IgniteCheckedException If failed.
         */
        private long fillBuffer(ByteBuffer buf, WALRecord head) throws IgniteCheckedException {
            final int limit = head.chainSize();

            assert limit <= buf.capacity();

            buf.rewind();
            buf.limit(limit);

            do {
                buf.position(head.chainSize() - head.size());
                buf.limit(head.chainSize()); // Just to make sure that serializer works in bounds.

                try {
                    serializer.writeRecord(head, buf);
                }
                catch (RuntimeException e) {
                    throw new IllegalStateException("Failed to write record: " + head, e);
                }

                assert !buf.hasRemaining() : "Reported record size is greater than actual: " + head;

                head = head.previous();
            }
            while (head.previous() != null);

            assert head instanceof FakeRecord : head.getClass();

            buf.rewind();
            buf.limit(limit);

            return head.position();
        }

        /**
         * Non-blocking check if this pointer needs to be sync'ed.
         *
         * @param ptr WAL pointer to check.
         * @return {@code False} if this pointer has been already sync'ed.
         */
        private boolean needFsync(FileWALPointer ptr) {
            // If index has changed, it means that the log was rolled over and already sync'ed.
            // If requested position is smaller than last sync'ed, it also means all is good.
            // If position is equal, then our record is the last not synced.
            return idx == ptr.index() && lastFsyncPos <= ptr.fileOffset();
        }

        /**
         * @return Pointer to the end of the last written record (probably not fsync-ed).
         */
        private FileWALPointer position() {
            lock.lock();

            try {
                return new FileWALPointer(idx, (int)written, 0);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param ptr Pointer to sync.
         * @throws StorageException If failed.
         */
        private void fsync(FileWALPointer ptr) throws StorageException, IgniteCheckedException {
            lock.lock();

            try {
                if (ptr != null) {
                    if (!needFsync(ptr))
                        return;

                    if (fsyncDelayNanos > 0 && !stop.get()) {
                        // Delay fsync to collect as many updates as possible: trade latency for throughput.
                        U.await(fsync, fsyncDelayNanos, TimeUnit.NANOSECONDS);

                        if (!needFsync(ptr))
                            return;
                    }
                }

                flushOrWait(ptr);

                if (lastFsyncPos != written) {
                    assert lastFsyncPos < written; // Fsync position must be behind.

                    try {
                        ch.force(false);
                    }
                    catch (IOException e) {
                        throw new StorageException(e);
                    }

                    lastFsyncPos = written;

                    if (fsyncDelayNanos > 0)
                        fsync.signalAll();
                }
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @return {@code true} If this thread actually closed the segment.
         * @throws IgniteCheckedException If failed.
         * @throws StorageException If failed.
         */
        private boolean close(boolean rollOver) throws IgniteCheckedException, StorageException {
            if (stop.compareAndSet(false, true)) {
                // Here we can be sure that no other records will be added and this fsync will be the last.
                if (mode == Mode.DEFAULT)
                    fsync(null);
                else
                    flushOrWait(null);

                try {
                    if (rollOver && written < (maxSegmentSize - 1)) {
                        ByteBuffer allocate = ByteBuffer.allocate(1);
                        allocate.put((byte) WALRecord.RecordType.SWITCH_SEGMENT_RECORD.ordinal());

                        ch.write(allocate, written);

                        if (mode == Mode.DEFAULT)
                            ch.force(false);
                    }

                    ch.close();
                }
                catch (IOException e) {
                    throw new IgniteCheckedException(e);
                }

                if (log.isDebugEnabled())
                    log.debug("Closed WAL write handle [idx=" + idx + "]");

                return true;
            }

            return false;
        }

        /**
         *
         */
        private void signalNextAvailable() {
            lock.lock();

            try {
                assert head.get() instanceof FakeRecord: "head";
                assert written == lastFsyncPos || mode != Mode.DEFAULT :
                    "fsync [written=" + written + ", lastFsync=" + lastFsyncPos + ']';

                ch = null;

                nextSegment.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void awaitNext() throws IgniteCheckedException {
            lock.lock();

            try {
                while (ch != null)
                    U.await(nextSegment);
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * @param pos Position in file.
         * @param buf Buffer.
         * @throws StorageException If failed.
         * @throws IgniteCheckedException If failed.
         */
        @SuppressWarnings("TooBroadScope")
        private void writeBuffer(long pos, ByteBuffer buf) throws StorageException, IgniteCheckedException {
            boolean interrupted = false;

            lock.lock();

            try {
                assert ch != null : "Writing to a closed segment.";

                checkEnvironment();

                long lastLogged = U.currentTimeMillis();

                long logBackoff = 2_000;

                // If we were too fast, need to wait previous writes to complete.
                while (written != pos) {
                    assert written < pos : "written = " + written + ", pos = " + pos; // No one can write further than we are now.

                    long now = U.currentTimeMillis();

                    if (now - lastLogged >= logBackoff) {
                        if (logBackoff < 60 * 60_000)
                            logBackoff *= 2;

                        U.warn(log, "Still waiting for a concurrent write to complete [written=" + written +
                            ", pos=" + pos + ", lastFsyncPos=" + lastFsyncPos + ", stop=" + stop.get() +
                            ", actualPos=" + safePosition() + ']');

                        lastLogged = now;
                    }

                    try {
                        writeComplete.await(2, TimeUnit.SECONDS);
                    }
                    catch (InterruptedException ignore) {
                        interrupted = true;
                    }

                    checkEnvironment();
                }

                // Do the write.
                int size = buf.remaining();

                assert size > 0 : size;

                try {
                    assert written == ch.position();

                    do {
                        ch.write(buf);
                    }
                    while (buf.hasRemaining());

                    written += size;

                    assert written == ch.position();
                }
                catch (IOException e) {
                    invalidateEnvironmentLocked(e);

                    throw new StorageException(e);
                }
            }
            finally {
                writeComplete.signalAll();

                lock.unlock();

                if (interrupted)
                    Thread.currentThread().interrupt();
            }
        }

        /**
         * @param e Exception to set as a cause for all further operations.
         */
        private void invalidateEnvironment(Throwable e) {
            lock.lock();

            try {
                invalidateEnvironmentLocked(e);
            }
            finally {
                writeComplete.signalAll();

                lock.unlock();
            }
        }

        /**
         * @param e Exception to set as a cause for all further operations.
         */
        private void invalidateEnvironmentLocked(Throwable e) {
            if (envFailed == null) {
                envFailed = e;

                U.error(log, "IO error encountered while running WAL flush. All further operations will be failed and " +
                    "local node will be stopped.", e);

                new Thread() {
                    @Override public void run() {
                        G.stop(gridName, true);
                    }
                }.start();
            }
        }

        /**
         * @throws StorageException If environment is no longer valid and we missed a WAL write.
         */
        private void checkEnvironment() throws StorageException {
            if (envFailed != null)
                throw new StorageException("Failed to flush WAL buffer (environment was invalidated by a " +
                    "previous error)", envFailed);
        }

        /**
         * @return Safely reads current position of the file channel as String. Will return "null" if channel is null.
         */
        private String safePosition() {
            FileChannel ch = this.ch;

            if (ch == null)
                return "null";

            try {
                return String.valueOf(ch.position());
            }
            catch (IOException e) {
                return "{Failed to read channel position: " + e.getMessage() + "}";
            }
        }
    }

    /**
     * Fake record.
     */
    private static final class FakeRecord extends WALRecord {
        /**
         * @param pos Position.
         */
        FakeRecord(long pos) {
            position(pos);
        }

        /** {@inheritDoc} */
        @Override public RecordType type() {
            return null;
        }
    }

    /**
     * Iterator over WAL-log.
     */
    public static class RecordsIterator extends GridCloseableIteratorAdapter<IgniteBiTuple<WALPointer, WALRecord>>
        implements WALIterator {
        /** */
        private static final long serialVersionUID = 0L;

        /** Buffer size. */
        private final int buffSize = IgniteSystemProperties.getInteger(
            IGNITE_PDS_WAL_RECORD_ITERATOR_BUFFER_SIZE, 64 * 1024 * 1024);

        /** */
        private final File walWorkDir;

        /** */
        private final File walArchiveDir;

        /** */
        private final FileArchiver archiver;

        /** */
        private final PersistenceConfiguration dbCfg;

        /** */
        private final RecordSerializer serializer;

        /** */
        private final GridCacheSharedContext cctx;

        /** */
        private FileWALPointer start;

        /** */
        private FileWALPointer end;

        /** */
        private IgniteBiTuple<WALPointer, WALRecord> curRec;

        /** */
        private int curIdx = -1;

        /** */
        private ReadFileHandle curHandle;

        /** */
        private ByteBuffer buf;

        /** */
        private IgniteLogger log;

        /**
         * @param cctx Shared context.
         * @param walWorkDir WAL work dir.
         * @param walArchiveDir WAL archive dir.
         * @param start Optional start pointer.
         * @param end Optional end pointer.
         * @param dbCfg Database configuration.
         * @param serializer Serializer.
         * @param archiver Archiver.
         * @throws IgniteCheckedException If failed to initialize WAL segment.
         */
        public RecordsIterator(
            GridCacheSharedContext cctx,
            File walWorkDir,
            File walArchiveDir,
            FileWALPointer start,
            FileWALPointer end,
            PersistenceConfiguration dbCfg,
            RecordSerializer serializer,
            FileArchiver archiver,
            IgniteLogger log,
            int tlbSize
        ) throws IgniteCheckedException {
            this.cctx = cctx;
            this.walWorkDir = walWorkDir;
            this.walArchiveDir = walArchiveDir;
            this.dbCfg = dbCfg;
            this.serializer = serializer;
            this.archiver = archiver;
            this.start = start;
            this.end = end;
            this.log = log;

            int buffSize0 = Math.min(16 * tlbSize, buffSize);

            // Do not allocate direct buffer for iterator.
            buf = ByteBuffer.allocate(buffSize0);
            buf.order(ByteOrder.nativeOrder());

            init();

            advance();
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<WALPointer, WALRecord> onNext() throws IgniteCheckedException {
            IgniteBiTuple<WALPointer, WALRecord> ret = curRec;

            advance();

            return ret;
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            return curRec != null;
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
            curRec = null;

            if (curHandle != null) {
                curHandle.close();

                if (curHandle.workDir)
                    releaseWorkSegment(curIdx);

                curHandle = null;
            }

            curIdx = Integer.MAX_VALUE;
        }

        /**
         * @throws IgniteCheckedException If failed to initialize first file handle.
         */
        private void init() throws IgniteCheckedException {
            FileDescriptor[] descs = scan(walArchiveDir.listFiles(WAL_SEGMENT_FILE_FILTER));

            if (start != null) {
                if (!F.isEmpty(descs)) {
                    if (descs[0].idx > start.index())
                        throw new IgniteCheckedException("WAL history is too short " +
                            "[descs=" + Arrays.asList(descs) + ", start=" + start + ']');

                    for (FileDescriptor desc : descs) {
                        if (desc.idx == start.index()) {
                            curIdx = start.index();

                            break;
                        }
                    }

                    if (curIdx == -1) {
                        int lastArchived = descs[descs.length - 1].idx;

                        if (lastArchived > start.index())
                            throw new IgniteCheckedException("WAL history is corrupted (segment is missing): " + start);

                        // This pointer may be in work files because archiver did not
                        // copy the file yet, check that it is not too far forward.
                        curIdx = start.index();
                    }
                }
                else {
                    // This means that whole checkpoint history fits in one segment in WAL work directory.
                    // Will start from this index right away.
                    curIdx = start.index();
                }
            }
            else
                curIdx = !F.isEmpty(descs) ? descs[0].idx : 0;

            curIdx--;

            if (log.isDebugEnabled())
                log.debug("Initialized WAL cursor [start=" + start + ", end=" + end + ", curIdx=" + curIdx + ']');
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void advance() throws IgniteCheckedException {
            while (true) {
                advanceRecord();

                if (curRec != null)
                    return;
                else {
                    advanceSegment();

                    if (curHandle == null)
                        return;
                }
            }
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void advanceRecord() throws IgniteCheckedException {
            try {
                ReadFileHandle hnd = curHandle;

                if (hnd != null) {
                    RecordSerializer ser = hnd.ser;

                    int pos = (int)hnd.in.position();

                    WALRecord rec = ser.readRecord(hnd.in);

                    WALPointer ptr = new FileWALPointer(hnd.idx, pos, rec.size());

                    curRec = new IgniteBiTuple<>(ptr, rec);
                }
            }
            catch (IOException | IgniteCheckedException e) {
                // TODO: verify that wrapped IntegrityException is acceptable in this case.
                curRec = null;
            }
        }

        /**
         * @throws IgniteCheckedException If failed.
         */
        private void advanceSegment() throws IgniteCheckedException {
            ReadFileHandle cur0 = curHandle;

            if (cur0 != null) {
                cur0.close();

                if (cur0.workDir)
                    releaseWorkSegment(cur0.idx);

                curHandle = null;
            }

            // We are past the end marker.
            if (end != null && curIdx + 1 > end.index())
                return;

            curIdx++;

            FileDescriptor fd;

            boolean readArchive = canReadArchiveOrReserveWork(curIdx);

            if (readArchive) {
                fd = new FileDescriptor(new File(walArchiveDir,
                    FileDescriptor.fileName(curIdx, serializer.version())));
            }
            else {
                int workIdx = curIdx % dbCfg.getWalSegments();

                fd = new FileDescriptor(
                    new File(walWorkDir, FileDescriptor.fileName(workIdx, serializer.version())),
                    curIdx);
            }

            if (log.isDebugEnabled())
                log.debug("Reading next file [absIdx=" + curIdx + ", file=" + fd.file.getAbsolutePath() + ']');

            assert fd != null;

            try {
                curHandle = initReadHandle(fd, start != null && curIdx == start.index() ? start : null);
            }
            catch (FileNotFoundException e) {
                if (readArchive)
                    throw new IgniteCheckedException("Missing WAL segment in the archive", e);
                else
                    curHandle = null;
            }

            if (curHandle != null)
                curHandle.workDir = !readArchive;
            else
                releaseWorkSegment(curIdx);

            curRec = null;
        }

        /**
         * @param desc File descriptor.
         * @param start Optional start pointer.
         * @return Initialized file handle.
         * @throws FileNotFoundException If segment file is missing.
         * @throws IgniteCheckedException If initialized failed due to another unexpected error.
         */
        private ReadFileHandle initReadHandle(FileDescriptor desc, FileWALPointer start)
            throws IgniteCheckedException, FileNotFoundException {
            try {
                RandomAccessFile rf = new RandomAccessFile(desc.file, "r");

                try {
                    RecordSerializer ser = forVersion(cctx, desc.ver);
                    FileInput in = new FileInput(rf.getChannel(), buf);

                    WALRecord rec = ser.readRecord(in);

                    if (rec == null)
                        return null;

                    if (rec.type() != WALRecord.RecordType.HEADER_RECORD)
                        throw new IOException("Missing file header record: " + desc.file.getAbsoluteFile());

                    int ver = ((HeaderRecord)rec).version();

                    if (ver != ser.version())
                        throw new IOException("Unexpected file format version: " + ver + ", " +
                            desc.file.getAbsoluteFile());

                    if (start != null && desc.idx == start.index())
                        in.seek(start.fileOffset());

                    return new ReadFileHandle(rf, desc.idx, cctx.igniteInstanceName(), ser, in);
                }
                catch (SegmentEofException | EOFException ignore) {
                    try {
                        rf.close();
                    }
                    catch (IOException ce) {
                        throw new IgniteCheckedException(ce);
                    }

                    return null;
                }
                catch (IOException | IgniteCheckedException e) {
                    try {
                        rf.close();
                    }
                    catch (IOException ce) {
                        e.addSuppressed(ce);
                    }

                    throw e;
                }
            }
            catch (FileNotFoundException e) {
                throw e;
            }
            catch (IOException e) {
                throw new IgniteCheckedException(
                    "Failed to initialize WAL segment: " + desc.file.getAbsolutePath(), e);
            }
        }

        /**
         * @param absIdx Absolute index to check.
         * @return {@code True} if we can safely read the archive, {@code false} if the segment has not been
         *      archived yet. In this case the corresponding work segment is reserved (will not be deleted until
         *      release).
         */
        private boolean canReadArchiveOrReserveWork(int absIdx) {
            return archiver != null && archiver.checkCanReadArchiveOrReserveWorkSegment(absIdx);
        }

        /**
         * @param absIdx Absolute index to release.
         */
        private void releaseWorkSegment(int absIdx) {
            if (archiver != null)
                archiver.releaseWorkSegment(absIdx);
        }
    }

    private class QueueFlusher extends Thread {
        /** */
        private volatile boolean stopped;

        /**
         * @param gridName Grid name.
         */
        private QueueFlusher(String gridName) {
            super("wal-queue-flusher-#" + gridName);
        }

        /** {@inheritDoc} */
        @Override public void run() {
            while (!stopped) {
                long wakeup = U.currentTimeMillis() + FLUSH_FREQ;

                LockSupport.parkUntil(wakeup);

                FileWriteHandle hnd = currentHandle();

                try {
                    hnd.flush(hnd.head.get());
                }
                catch (IgniteCheckedException e) {
                    U.warn(log, "Failed to flush WAL record queue", e);
                }
            }
        }

        private void shutdown() {
            stopped = true;

            LockSupport.unpark(this);

            try {
                join();
            }
            catch (InterruptedException ignore) {
                // Got interrupted while waiting for flusher to shutdown.
            }
        }
    }

    /**
     * WAL Mode.
     */
    private enum Mode {
        NONE, LOG_ONLY, BACKGROUND, DEFAULT
    }
}
