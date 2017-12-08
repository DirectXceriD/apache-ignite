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

package org.apache.ignite.internal.processors.cache.persistence.file;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jsr166.ConcurrentHashMap8;
import sun.nio.ch.DirectBuffer;

/**
 * Limited capabilities Direct IO, which enables file write and read using aligned buffers and O_DIRECT mode.
 *
 * Works only for Linux
 */
public class AlignedBuffersDirectFileIO implements FileIO {
    /** File system & linux block size. Minimal amount of data can be written using DirectIO. */
    private final int fsBlockSize;

    /** Durable memory Page size. Can have greater value than {@link #fsBlockSize}. */
    private final int pageSize;

    /** File. */
    private final File file;

    /** Logger. */
    private final IgniteLogger log;

    /** Thread local with buffers with capacity = one page {@link #pageSize} and aligned using {@link #fsBlockSize}. */
    private ThreadLocal<ByteBuffer> tlbOnePageAligned;

    /** Managed aligned buffers. Used to check if buffer is applicable for direct IO our data should be copied. */
    private ConcurrentHashMap8<Long, Thread> managedAlignedBuffers;

    /** File descriptor. */
    private int fd = -1;

    /**
     * Creates Direct File IO.
     *
     * @param fsBlockSize FS/OS block size.
     * @param pageSize Durable memory Page size.
     * @param file File to open.
     * @param modes Open options (flags).
     * @param tlbOnePageAligned Thread local with buffers with capacity = one page {@code pageSize} and aligned using
     * {@code fsBlockSize}.
     * @param managedAlignedBuffers Managed aligned buffers map, used to check if buffer is known.
     * @param log Logger.
     * @throws IOException if file open failed.
     */
    AlignedBuffersDirectFileIO(
        int fsBlockSize,
        int pageSize,
        File file,
        OpenOption[] modes,
        ThreadLocal<ByteBuffer> tlbOnePageAligned,
        ConcurrentHashMap8<Long, Thread> managedAlignedBuffers,
        IgniteLogger log)
        throws IOException {
        this.log = log;
        this.fsBlockSize = fsBlockSize;
        this.pageSize = pageSize;
        this.file = file;
        this.tlbOnePageAligned = tlbOnePageAligned;
        this.managedAlignedBuffers = managedAlignedBuffers;

        String pathname = file.getAbsolutePath();

        int fd = IgniteNativeIoLib.open(pathname, setupOpenFlags(modes, log), 00644);

        if (fd < 0)
            throw new IOException("Error opening file [" + pathname + "], got error [" + getLastError() + "]");

        this.fd = fd;
    }

    /**
     * Convert Java open options to native flags.
     *
     * @param modes java options.
     * @param log logger.
     * @return native flags with {@link IgniteNativeIoLib#O_DIRECT} enabled.
     */
    private static int setupOpenFlags(OpenOption[] modes, IgniteLogger log) {
        int flags = IgniteNativeIoLib.O_DIRECT;
        List<OpenOption> openOptionList = Arrays.asList(modes);

        for (OpenOption mode : openOptionList) {
            if (mode == StandardOpenOption.READ) {
                flags |= openOptionList.contains(StandardOpenOption.WRITE)
                    ? IgniteNativeIoLib.O_RDWR
                    : IgniteNativeIoLib.O_RDONLY;
            }
            else if (mode == StandardOpenOption.WRITE) {
                flags |= openOptionList.contains(StandardOpenOption.READ)
                    ? IgniteNativeIoLib.O_RDWR
                    : IgniteNativeIoLib.O_WRONLY;
            }
            else if (mode == StandardOpenOption.CREATE)
                flags |= IgniteNativeIoLib.O_CREAT;
            else if (mode == StandardOpenOption.TRUNCATE_EXISTING)
                flags |= IgniteNativeIoLib.O_TRUNC;
            else if (mode == StandardOpenOption.SYNC)
                flags |= IgniteNativeIoLib.O_SYNC;
            else
                log.error("Unsupported open option [" + mode + "]");
        }

        return flags;
    }

    /** {@inheritDoc} */
    @Override public long position() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void position(long newPosition) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf) throws IOException {
        int size = checkSizeIsPadded(destBuf.remaining());

        if (isKnownAligned(destBuf)) {
            int loaded = readAligned(destBuf);

            if (loaded < 0)
                return loaded;

            return moveBufPosition(destBuf, loaded);
        }

        boolean useTlb = size == pageSize;
        ByteBuffer alignedBuf = useTlb ? tlbOnePageAligned.get() : AlignedBuffers.allocate(fsBlockSize, size);

        try {
            int loaded = readAligned(alignedBuf);

            if (loaded < 0)
                return loaded;

            AlignedBuffers.copyMemory(alignedBuf, destBuf);

            return moveBufPosition(destBuf, loaded);
        }
        finally {
            if (!useTlb)
                AlignedBuffers.free(alignedBuf);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(ByteBuffer destBuf, long position) throws IOException {
        int size = checkSizeIsPadded(destBuf.remaining());

        if (isKnownAligned(destBuf)) {
            int loaded = readAligned(destBuf, position);

            if (loaded < 0)
                return loaded;

            return moveBufPosition(destBuf, loaded);
        }

        boolean useTlb = size == pageSize;
        ByteBuffer alignedBuf = useTlb ? tlbOnePageAligned.get() : AlignedBuffers.allocate(fsBlockSize, size);

        try {
            int loaded = readAligned(alignedBuf, position);

            if (loaded < 0)
                return loaded;

            AlignedBuffers.copyMemory(alignedBuf, destBuf);

            return moveBufPosition(destBuf, loaded);
        }
        finally {
            if (!useTlb)
                AlignedBuffers.free(alignedBuf);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf) throws IOException {
        int size = checkSizeIsPadded(srcBuf.remaining());

        if (isKnownAligned(srcBuf)) {
            int written = writeAligned(srcBuf);

            return moveBufPosition(srcBuf, written);
        }

        boolean useTlb = size == pageSize;
        ByteBuffer alignedBuf = useTlb ? tlbOnePageAligned.get() : AlignedBuffers.allocate(fsBlockSize, size);

        try {
            AlignedBuffers.copyMemory(srcBuf, alignedBuf);

            int written = writeAligned(alignedBuf);

            return moveBufPosition(srcBuf, written);
        }
        finally {
            if (!useTlb)
                AlignedBuffers.free(alignedBuf);
        }
    }

    /** {@inheritDoc} */
    @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
        int size = checkSizeIsPadded(srcBuf.remaining());

        if (isKnownAligned(srcBuf)) {
            int written = writeAligned(srcBuf, position);

            return moveBufPosition(srcBuf, written);
        }

        boolean useTlb = size == pageSize;
        ByteBuffer alignedBuf = useTlb ? tlbOnePageAligned.get() : AlignedBuffers.allocate(fsBlockSize, size);

        try {
            AlignedBuffers.copyMemory(srcBuf, alignedBuf);

            int written = writeAligned(alignedBuf, position);

            return moveBufPosition(srcBuf, written);
        }
        finally {
            if (!useTlb)
                AlignedBuffers.free(alignedBuf);
        }
    }

    /**
     * @param srcBuf buffer to change position.
     * @param bytes number of bytes to move.
     * @return position delta.
     */
    private int moveBufPosition(ByteBuffer srcBuf, int bytes) {
        srcBuf.position(srcBuf.position() + bytes);

        return bytes;
    }

    /**
     * Checks if we can run fast path, well known buffer is already aligned.
     *
     * @param srcBuf buffer to check if it is known buffer.
     * @return {@code true} if this buffer was allocated with alignment, may be used directly.
     */
    private boolean isKnownAligned(ByteBuffer srcBuf) {
        return srcBuf instanceof DirectBuffer
            && managedAlignedBuffers != null
            && managedAlignedBuffers.containsKey(GridUnsafe.bufferAddress(srcBuf));
    }

    /**
     * Check if size is appropriate for aligned/direct IO.
     *
     * @param size buffer size to write, should be divisible by {@link #fsBlockSize}.
     * @return size from parameter.
     * @throws IOException if provided size can't be written using direct IO.
     */
    private int checkSizeIsPadded(int size) throws IOException {
        if (size % fsBlockSize != 0) {
            throw new IOException(
                String.format("Unable to apply DirectIO for read/write buffer [%d] bytes on file system " +
                    "block size [%d]. Consider setting %s.setPageSize(%d) or disable Direct IO.",
                    size, fsBlockSize, DataStorageConfiguration.class.getSimpleName(), fsBlockSize));
        }

        return size;
    }

    /**
     * Checks if file is opened and returns descriptor.
     *
     * @return file descriptor.
     * @throws IOException if file not opened.
     */
    private int fdCheckOpened() throws IOException {
        if (fd < 0)
            throw new IOException(String.format("Error %s not opened", file));

        return fd;
    }

    /**
     * Read bytes from file using Native IO and aligned buffers.
     *
     * @param destBuf Destination aligned byte buffer.
     * @param filePos Starting position of file.
     * @return number of bytes read from file, or <tt>-1</tt> if tried to read past EOF for file.
     * @throws IOException if reading failed.
     */
    private int readAligned(ByteBuffer destBuf, long filePos) throws IOException {
        int rd = IgniteNativeIoLib.pread(fdCheckOpened(), bufferPtr(destBuf), nl(destBuf.remaining()),
            nl(filePos)).intValue();

        if (rd == 0)
            return -1;

        if (rd < 0)
            throw new IOException(String.format("Error during reading file [%s] : %s", file, getLastError()));

        return rd;
    }


    /**
     * Read bytes from file using Native IO and aligned buffers.
     *
     * @param destBuf Destination aligned byte buffer.
     * @return number of bytes read from file, or <tt>-1</tt> if tried to read past EOF for file.
     * @throws IOException if reading failed.
     */
    private int readAligned(ByteBuffer destBuf) throws IOException {
        int rd = IgniteNativeIoLib.read(fdCheckOpened(), bufferPtr(destBuf), nl(destBuf.remaining())).intValue();

        if (rd == 0)
            return -1; //Tried to read past EOF for file

        if (rd < 0)
            throw new IOException(String.format("Error during reading file [%s] : %s", file, getLastError()));

        return rd;
    }

    /**
     * Writes the aligned native buffer starting at {@code buf} to the file at offset
     * {@code filePos}. The file offset is not changed.
     *
     * @param buf pointer to buffer.
     * @param filePos position in file to write data.
     * @return the number of bytes written.
     */
    private int writeAligned(ByteBuffer buf, long filePos) throws IOException {
        int wr = IgniteNativeIoLib.pwrite(fdCheckOpened(), bufferPtr(buf), nl(buf.remaining()), nl(filePos)).intValue();

        if (wr < 0) {
            throw new IOException(String.format("Error during writing file [%s] to position [%d]: %s",
                file, filePos, getLastError()));
        }

        return wr;
    }

    /**
     * Writes the aligned native buffer starting at {@code buf} to the file at offset
     * {@code filePos}. The file offset is changed.
     *
     * @param buf pointer to buffer.
     * @return the number of bytes written.
     */
    private int writeAligned(ByteBuffer buf) throws IOException {
        int wr = IgniteNativeIoLib.write(fdCheckOpened(), bufferPtr(buf), nl(buf.remaining())).intValue();

        if (wr < 0)
            throw new IOException(String.format("Error during writing file [%s] : %s", file, getLastError()));

        return wr;
    }

    /**
     * @param l value to box to native long.
     * @return native long.
     */
    @NotNull private NativeLong nl(long l) {
        return new NativeLong(l);
    }

    /**
     * Retrieve last error set by the OS as string. This corresponds to and <code>errno</code> on
     * *nix platforms.
     * @return displayable string with OS error info.
     */
    public static String getLastError() {
        return IgniteNativeIoLib.strerror(Native.getLastError());
    }

    /**
     * Gets address in memory for direct aligned buffer. Produces warnings if data or offset seems to be not aligned.
     *
     * @param buf Direct aligned buffer.
     * @return Buffer memory address.
     */
    @NotNull private Pointer bufferPtr(ByteBuffer buf) {
        long alignedPointer = GridUnsafe.bufferAddress(buf);

        if ((alignedPointer) % fsBlockSize != 0) {
            U.warn(log, String.format("IO Buffer Pointer [%d] seems to be not aligned for FS block size [%d]. " +
                "Direct IO may fail.", alignedPointer, fsBlockSize));
        }

        if ((alignedPointer + buf.position()) % fsBlockSize != 0) {
            U.warn(log, String.format("IO Buffer Pointer [%d] and/or offset [%d] seems to be not aligned " +
                "for FS block size [%d]. Direct IO may fail.", alignedPointer, buf.position(), fsBlockSize));
        }

        return new Pointer(alignedPointer);
    }

    /** {@inheritDoc} */
    @Override public void write(byte[] buf, int off, int len) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void force() throws IOException {
        if (IgniteNativeIoLib.fsync(fdCheckOpened()) < 0)
            throw new IOException(String.format("Error fsync()'ing %s, got %s", file, getLastError()));
    }

    /** {@inheritDoc} */
    @Override public long size() throws IOException {
        return file.length();
    }

    /** {@inheritDoc} */
    @Override public void clear() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        if (IgniteNativeIoLib.close(fdCheckOpened()) < 0)
            throw new IOException(String.format("Error closing %s, got %s", file, getLastError()));

        fd = -1;
    }
}
