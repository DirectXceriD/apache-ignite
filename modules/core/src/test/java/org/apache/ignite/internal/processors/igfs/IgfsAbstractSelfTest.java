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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsMetrics;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsParentNotDirectoryException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.secondary.local.LocalIgfsSecondaryFileSystem;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;

import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Test fo regular igfs operations.
 */
@SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "ConstantConditions"})
public abstract class IgfsAbstractSelfTest extends IgfsAbstractBaseSelfTest {
    /**
     * Constructor.
     *
     * @param mode IGFS mode.
     */
    protected IgfsAbstractSelfTest(IgfsMode mode) {
        super(mode);
    }

    /**
     * Constructor.
     *
     * @param mode IGFS mode.
     * @param memoryMode Memory mode.
     */
    protected IgfsAbstractSelfTest(IgfsMode mode, CacheMemoryMode memoryMode) {
        super(mode, memoryMode);
    }

    /**
     * Test existence check when the path exists both locally and remotely.
     *
     * @throws Exception If failed.
     */
    public void testExists() throws Exception {
        create(igfs, paths(DIR), null);

        checkExist(igfs, igfsSecondary, DIR);
    }

    /**
     * Test existence check when the path doesn't exist remotely.
     *
     * @throws Exception If failed.
     */
    public void testExistsPathDoesNotExist() throws Exception {
        assert !igfs.exists(DIR);
    }

    /**
     * Test list files routine.
     *
     * @throws Exception If failed.
     */
    public void testListFiles() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));

        Collection<IgfsFile> paths = igfs.listFiles(SUBDIR);

        assert paths != null;
        assert paths.size() == 2;

        Iterator<IgfsFile> iter = paths.iterator();

        IgfsFile path1 = iter.next();
        IgfsFile path2 = iter.next();

        assert (SUBSUBDIR.equals(path1.path()) && FILE.equals(path2.path())) ||
            (FILE.equals(path1.path()) && SUBSUBDIR.equals(path2.path()));
    }

    /**
     * Test list files routine when the path doesn't exist remotely.
     *
     * @throws Exception If failed.
     */
    public void testListFilesPathDoesNotExist() throws Exception {
        Collection<IgfsFile> paths = null;

        try {
            paths = igfs.listFiles(SUBDIR);
        }
        catch (IgniteException ignore) {
            // No-op.
        }

        assert paths == null || paths.isEmpty();
    }

    /**
     * Test info routine when the path exists both locally and remotely.
     *
     * @throws Exception If failed.
     */
    public void testInfo() throws Exception {
        create(igfs, paths(DIR), null);

        IgfsFile info = igfs.info(DIR);

        assert info != null;

        assertEquals(DIR, info.path());
    }

    /**
     * Test info routine when the path doesn't exist remotely.
     *
     * @throws Exception If failed.
     */
    public void testInfoPathDoesNotExist() throws Exception {
        IgfsFile info = null;

        try {
            info = igfs.info(DIR);
        }
        catch (IgniteException ignore) {
            // No-op.
        }

        assert info == null;
    }

    /**
     * Test rename in case both local and remote file systems have the same folder structure and the path being renamed
     * is a file.
     *
     * @throws Exception If failed.
     */
    public void testRenameFile() throws Exception {
        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        igfs.rename(FILE, FILE2);

        checkExist(igfs, igfsSecondary, FILE2);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file rename when parent folder is the root.
     *
     * @throws Exception If failed.
     */
    public void testRenameFileParentRoot() throws Exception {
        IgfsPath file1 = new IgfsPath("/file1");
        IgfsPath file2 = new IgfsPath("/file2");

        create(igfs, null, paths(file1));

        igfs.rename(file1, file2);

        checkExist(igfs, igfsSecondary, file2);
        checkNotExist(igfs, igfsSecondary, file1);
    }

    /**
     * Test rename in case both local and remote file systems have the same folder structure and the path being renamed
     * is a directory.
     *
     * @throws Exception If failed.
     */
    public void testRenameDirectory() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        igfs.rename(SUBDIR, SUBDIR2);

        checkExist(igfs, igfsSecondary, SUBDIR2);
        checkNotExist(igfs, igfsSecondary, SUBDIR);
    }

    /**
     * Test directory rename when parent folder is the root.
     *
     * @throws Exception If failed.
     */
    public void testRenameDirectoryParentRoot() throws Exception {
        IgfsPath dir1 = new IgfsPath("/dir1");
        IgfsPath dir2 = new IgfsPath("/dir2");

        create(igfs, paths(dir1), null);

        igfs.rename(dir1, dir2);

        checkExist(igfs, igfsSecondary, dir2);
        checkNotExist(igfs, igfsSecondary, dir1);
    }

    /**
     * Test move in case both local and remote file systems have the same folder structure and the path being renamed is
     * a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveFile() throws Exception {
        create(igfs, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));

        igfs.rename(FILE, SUBDIR_NEW);

        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move when destination is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileDestinationRoot() throws Exception {
        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        igfs.rename(FILE, IgfsPath.ROOT);

        checkExist(igfs, igfsSecondary, new IgfsPath("/" + FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move when source parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileSourceParentRoot() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        create(igfs, paths(DIR_NEW, SUBDIR_NEW), paths(file));

        igfs.rename(file, SUBDIR_NEW);

        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, file);
    }

    /**
     * Test move and rename in case both local and remote file systems have the same folder structure and the path being
     * renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFile() throws Exception {
        create(igfs, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));

        igfs.rename(FILE, FILE_NEW);

        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move and rename when destination is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileDestinationRoot() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        igfs.rename(FILE, file);

        checkExist(igfs, igfsSecondary, file);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move and rename when source parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileSourceParentRoot() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE_NEW.name());

        create(igfs, paths(DIR_NEW, SUBDIR_NEW), paths(file));

        igfs.rename(file, FILE_NEW);

        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, file);
    }

    /**
     * Test move in case both local and remote file systems have the same folder structure and the path being renamed is
     * a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectory() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move when destination is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectoryDestinationRoot() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), null);

        igfs.rename(SUBSUBDIR, IgfsPath.ROOT);

        checkExist(igfs, igfsSecondary, new IgfsPath("/" + SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move when source parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectorySourceParentRoot() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR.name());

        create(igfs, paths(DIR_NEW, SUBDIR_NEW, dir), null);

        igfs.rename(dir, SUBDIR_NEW);

        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, dir);
    }

    /**
     * Test move and rename  in case both local and remote file systems have the same folder structure and the path
     * being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectory() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move and rename when destination is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectoryDestinationRoot() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR.name());

        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), null);

        igfs.rename(SUBSUBDIR, dir);

        checkExist(igfs, igfsSecondary, dir);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move and rename when source parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectorySourceParentRoot() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR_NEW.name());

        create(igfs, paths(DIR_NEW, SUBDIR_NEW, dir), null);

        igfs.rename(dir, SUBSUBDIR_NEW);

        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, dir);
    }

    /**
     * Ensure that rename doesn't occur in case source doesn't exist remotely.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameSourceDoesNotExist() throws Exception {
        create(igfs, paths(DIR, DIR_NEW), null);

        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                igfs.rename(SUBDIR, SUBDIR_NEW);

                return null;
            }
        }, IgfsException.class, null);

        checkNotExist(igfs, igfsSecondary, SUBDIR, SUBDIR_NEW);
    }

    /**
     * Test mkdirs in case both local and remote file systems have the same folder structure.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testMkdirs() throws Exception {
        if (!propertiesSupported())
            return;

        Map<String, String> props = properties(null, null, "0555"); // mkdirs command doesn't propagate user info.

        igfs.mkdirs(new IgfsPath("/x"), null);
        checkExist(igfs, igfsSecondary, new IgfsPath("/x"));

        igfs.mkdirs(new IgfsPath("/k/l"), null);
        checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));

        igfs.mkdirs(new IgfsPath("/x/y"), null);
        checkExist(igfs, igfsSecondary, new IgfsPath("/x/y"));

        igfs.mkdirs(new IgfsPath("/a/b/c/d"), null);
        checkExist(igfs, igfsSecondary, new IgfsPath("/a/b/c/d"));

        igfs.mkdirs(new IgfsPath("/a/b/c/d/e"), null);
        checkExist(igfs, igfsSecondary, new IgfsPath("/a/b/c/d/e"));

        create(igfs, null, new IgfsPath[] { new IgfsPath("/d/f") }); // "f" is a file.
        checkExist(igfs, igfsSecondary, new IgfsPath("/d/f"));
        assertTrue(igfs.info(new IgfsPath("/d/f")).isFile());

        try {
            igfs.mkdirs(new IgfsPath("/d/f"), null);

            fail("IgfsParentNotDirectoryException expected.");
        }
        catch (IgfsParentNotDirectoryException ignore) {
            // No-op.
        }
        catch (IgfsException ignore) {
            // Currently Ok for Hadoop fs:
            if (!getClass().getSimpleName().startsWith("Hadoop"))
                throw ignore;
        }

        try {
            igfs.mkdirs(new IgfsPath("/d/f/something/else"), null);

            fail("IgfsParentNotDirectoryException expected.");
        }
        catch (IgfsParentNotDirectoryException ignore) {
            // No-op.
        }
        catch (IgfsException ignore) {
            // Currently Ok for Hadoop fs:
            if (!getClass().getSimpleName().startsWith("Hadoop"))
                throw ignore;
        }

        create(igfs, paths(DIR, SUBDIR), null);

        igfs.mkdirs(SUBSUBDIR, props);

        // Ensure that directory was created and properties are propagated.
        checkExist(igfs, igfsSecondary, SUBSUBDIR);

        if (permissionsSupported()) {
            if (dual)
                // Check only permissions because user and group will always be present in Hadoop Fs.
                assertEquals(props.get(IgfsUtils.PROP_PERMISSION), igfsSecondary.permissions(SUBSUBDIR.toString()));

            // We check only permission because IGFS client adds username and group name explicitly.
            assertEquals(props.get(IgfsUtils.PROP_PERMISSION),
                igfs.info(SUBSUBDIR).properties().get(IgfsUtils.PROP_PERMISSION));
        }
    }

    /**
     * Test mkdirs in case parent is the root directory.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testMkdirsParentRoot() throws Exception {
        Map<String, String> props = properties(null, null, "0555"); // mkdirs command doesn't propagate user info.

        igfs.mkdirs(DIR, props);

        checkExist(igfs, igfsSecondary, DIR);

        if (permissionsSupported()) {
            if (dual)
                // check permission only since Hadoop Fs will always have user and group:
                assertEquals(props.get(IgfsUtils.PROP_PERMISSION), igfsSecondary.permissions(DIR.toString()));

            // We check only permission because IGFS client adds username and group name explicitly.
            assertEquals(props.get(IgfsUtils.PROP_PERMISSION),
                igfs.info(DIR).properties().get(IgfsUtils.PROP_PERMISSION));
        }
    }

    /**
     * Test delete in case both local and remote file systems have the same folder structure.
     *
     * @throws Exception If failed.
     */
    public void testDelete() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));

        igfs.delete(SUBDIR, true);

        checkNotExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Test delete when the path parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testDeleteParentRoot() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));

        igfs.delete(DIR, true);

        checkNotExist(igfs, igfsSecondary, DIR, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Ensure that delete will not be successful in non-empty directory when recursive flag is set to {@code false}.
     *
     * @throws Exception If failed.
     */
    public void testDeleteDirectoryNotEmpty() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));
        checkExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR, FILE);

        try {
            boolean ok = igfs.delete(SUBDIR, false);

            assertFalse(ok);
        }
        catch (IgfsDirectoryNotEmptyException ignore) {
            // No-op, expected.
        }

        checkExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Test update in case both local and remote file systems have the same folder structure.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testUpdate() throws Exception {
        if(!propertiesSupported())
            return;

        Map<String, String> props = properties("owner", "group", "0555");

        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        igfs.update(FILE, props);

        if (dual)
            assertEquals(props, igfsSecondary.properties(FILE.toString()));

        assertEquals(props, igfs.info(FILE).properties());
    }

    /**
     * Test update when parent is the root.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testUpdateParentRoot() throws Exception {
        if(!propertiesSupported())
            return;

        Map<String, String> props = properties("owner", "group", "0555");

        create(igfs, paths(DIR), null);

        igfs.update(DIR, props);

        if (dual)
            assertEquals(props, igfsSecondary.properties(DIR.toString()));

        assertEquals(props, igfs.info(DIR).properties());
    }

    /**
     * Check that exception is thrown in case the path being updated doesn't exist remotely.
     *
     * @throws Exception If failed.
     */
    public void testUpdatePathDoesNotExist() throws Exception {
        final Map<String, String> props = properties("owner", "group", "0555");

        assert igfs.update(SUBDIR, props) == null;

        checkNotExist(igfs, igfsSecondary, SUBDIR);
    }

    /**
     * Ensure that formatting is not propagated to the secondary file system.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testFormat() throws Exception {
        if (mode == PROXY)
            return;

        final GridCacheAdapter<IgfsBlockKey, byte[]> dataCache = getDataCache(igfs);

        assert dataCache != null;

        int size0 = dataCache.size(new CachePeekMode[] {CachePeekMode.ALL});
        assert size0 == 0 : "Initial data cache size = " + size0;

        if (dual)
            create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE, FILE_NEW));

        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        try (IgfsOutputStream os = igfs.create(FILE, true)) {
            os.write(new byte[10 * 1024 * 1024]);
        }

        awaitFileClose(igfs, FILE);

        if (dual)
            checkExist(igfsSecondary, DIR, SUBDIR, FILE, DIR_NEW, SUBDIR_NEW, FILE_NEW);

        checkExist(igfs, DIR, SUBDIR, FILE);

        assertEquals(10 * 1024 * 1024, igfs.info(FILE).length());

        assert dataCache.size(new CachePeekMode[] {CachePeekMode.ALL}) > 0;

        igfs.format();

        // Ensure format is not propagated to the secondary file system.
        if (dual) {
            checkExist(igfsSecondary, DIR, SUBDIR, FILE, DIR_NEW, SUBDIR_NEW, FILE_NEW);

            igfsSecondary.format();
        }

        // Ensure entries deletion in the primary file system.
        checkNotExist(igfs, DIR, SUBDIR, FILE);

        if (!GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    return dataCache.size(new CachePeekMode[] {CachePeekMode.ALL}) == 0;
                } catch (IgniteCheckedException ice) {
                    throw new IgniteException(ice);
                }
            }
        }, 10_000)) {
            Iterable<? extends GridCacheEntryEx> entries = dataCache.allEntries();

            for (GridCacheEntryEx e: entries) {
                X.println("deleted = " + e.deleted());
                X.println("detached = " + e.detached());
                X.println("info = " + e.info());
                X.println("k = " + e.key() + ", v = " + e.valueBytes());
            }

            assert false;
        }
    }

    /**
     * Tests that root directory properties persist afetr the #format() operation.
     *
     * @throws Exception If failed.
     */
    public void testRootPropertiesPersistAfterFormat() throws Exception {
        if(!propertiesSupported())
            return;

        if (dual && !(igfsSecondaryFileSystem instanceof IgfsSecondaryFileSystemImpl)) {
            // In case of Hadoop dual mode only user name, group name, and permission properties are updated,
            // an arbitrary named property is just ignored:
            checkRootPropertyUpdate("foo", "moo", null);
            checkRootPropertyUpdate(IgfsUtils.PROP_PERMISSION, "0777", "0777");
        }
        else {
            checkRootPropertyUpdate("foo", "moo", "moo");
            checkRootPropertyUpdate(IgfsUtils.PROP_PERMISSION, "0777", "0777");
        }
    }

    /**
     * Check root property update.
     *
     * @throws Exception If failed.
     */
    private void checkRootPropertyUpdate(String prop, String setVal, String expGetVal) throws Exception {
        igfs.update(IgfsPath.ROOT, Collections.singletonMap(prop, setVal));

        igfs.format();

        IgfsFile file = igfs.info(IgfsPath.ROOT);

        assert file != null;

        Map<String,String> props = file.properties();

        assertEquals(expGetVal, props.get(prop));
    }

    /**
     * Test regular file open.
     *
     * @throws Exception If failed.
     */
    public void testOpen() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        createFile(igfs, FILE, true, chunk);

        checkFileContent(igfs, FILE, chunk);

        // Read again when the whole file is in memory.
        checkFileContent(igfs, FILE, chunk);
    }

    /**
     * Test file open in case it doesn't exist both locally and remotely.
     *
     * @throws Exception If failed.
     */
    public void testOpenDoesNotExist() throws Exception {
        igfsSecondary.delete(FILE.toString(), false);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsInputStream is = null;

                try {
                    is = igfs.open(FILE);
                }
                finally {
                    U.closeQuiet(is);
                }

                return null;
            }
        }, IgfsPathNotFoundException.class, "File not found: " + FILE);
    }

    /**
     * Test setTimes operation.
     *
     * @throws Exception If failed.
     */
    public void testSetTimes() throws Exception {
        createFile(igfs, FILE, true, chunk);

        checkExist(igfs, igfsSecondary, DIR);
        checkExist(igfs, igfsSecondary, SUBDIR);
        checkExist(igfs, igfsSecondary, FILE);

        checkSetTimes(SUBDIR);
        checkSetTimes(FILE);

        try {
            igfs.setTimes(FILE2, Long.MAX_VALUE, Long.MAX_VALUE);

            fail("Exception is not thrown for missing file.");
        }
        catch (Exception ignore) {
            // No-op.
        }
    }

    /**
     * Check setTimes logic for path.
     *
     * @param path Path.
     * @throws Exception If failed.
     */
    private void checkSetTimes(IgfsPath path) throws Exception {
        if (timesSupported()) {

            IgfsFile info = igfs.info(path);
            T2<Long, Long> secondaryTimes = dual ? igfsSecondary.times(path.toString()) : null;

            assert info != null;

            // Change nothing.
            igfs.setTimes(path, -1, -1);

            IgfsFile newInfo = igfs.info(path);

            assert newInfo != null;

            assertEquals(info.accessTime(), newInfo.accessTime());
            assertEquals(info.modificationTime(), newInfo.modificationTime());

            if (dual) {
                T2<Long, Long> newSecondaryTimes = igfsSecondary.times(path.toString());

                assertEquals(secondaryTimes.get1(), newSecondaryTimes.get1());
                assertEquals(secondaryTimes.get2(), newSecondaryTimes.get2());
            }

            // Change only access time.
            igfs.setTimes(path, info.accessTime() + 1, -1);

            newInfo = igfs.info(path);

            assert newInfo != null;

            assertEquals(info.accessTime() + 1, newInfo.accessTime());
            assertEquals(info.modificationTime(), newInfo.modificationTime());

            if (dual) {
                T2<Long, Long> newSecondaryTimes = igfsSecondary.times(path.toString());

                assertEquals(newInfo.accessTime(), (long) newSecondaryTimes.get1());
                assertEquals(secondaryTimes.get2(), newSecondaryTimes.get2());
            }

            // Change only modification time.
            igfs.setTimes(path, -1, info.modificationTime() + 1);

            newInfo = igfs.info(path);

            assert newInfo != null;

            assertEquals(info.accessTime() + 1, newInfo.accessTime());
            assertEquals(info.modificationTime() + 1, newInfo.modificationTime());

            if (dual) {
                T2<Long, Long> newSecondaryTimes = igfsSecondary.times(path.toString());

                assertEquals(newInfo.accessTime(), (long) newSecondaryTimes.get1());
                assertEquals(newInfo.modificationTime(), (long) newSecondaryTimes.get2());
            }

            // Change both.
            igfs.setTimes(path, info.accessTime() + 2, info.modificationTime() + 2);

            newInfo = igfs.info(path);

            assert newInfo != null;

            assertEquals(info.accessTime() + 2, newInfo.accessTime());
            assertEquals(info.modificationTime() + 2, newInfo.modificationTime());

            if (dual) {
                T2<Long, Long> newSecondaryTimes = igfsSecondary.times(path.toString());

                assertEquals(newInfo.accessTime(), (long) newSecondaryTimes.get1());
                assertEquals(newInfo.modificationTime(), (long) newSecondaryTimes.get2());
            }
        }
    }

    /**
     * Test regular create.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ConstantConditions", "EmptyTryBlock", "UnusedDeclaration"})
    public void testCreate() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        createFile(igfs, FILE, true, chunk);

        checkFile(igfs, igfsSecondary, FILE, chunk);

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/r"), false)) {
            checkExist(igfs, igfsSecondary, new IgfsPath("/r"));
            assert igfs.info(new IgfsPath("/r")).isFile();
        }

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/k/l"), false)) {
            checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
            assert igfs.info(new IgfsPath("/k/l")).isFile();
        }

        try {
            try (IgfsOutputStream os = igfs.create(new IgfsPath("/k/l"), false)) {}

            fail("Exception expected");
        } catch (IgniteException e) {
            // okay
        }

        checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
        assert igfs.info(new IgfsPath("/k/l")).isFile();

        try {
            try (IgfsOutputStream os = igfs.create(new IgfsPath("/k/l/m"), true)) {}

            fail("Exception expected");
        } catch (IgniteException e) {
            // okay
        }
        checkNotExist(igfs, igfsSecondary, new IgfsPath("/k/l/m"));
        checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
        assert igfs.info(new IgfsPath("/k/l")).isFile();

        try {
            try (IgfsOutputStream os = igfs.create(new IgfsPath("/k/l/m/n/o/p"), true)) {}

            fail("Exception expected");
        } catch (IgniteException e) {
            // okay
        }
        checkNotExist(igfs, igfsSecondary, new IgfsPath("/k/l/m"));
        checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
        assert igfs.info(new IgfsPath("/k/l")).isFile();

        igfs.mkdirs(new IgfsPath("/x/y"), null);
        try {
            try (IgfsOutputStream os = igfs.create(new IgfsPath("/x/y"), true)) {}

            fail("Exception expected");
        } catch (IgniteException e) {
            // okay
        }

        checkExist(igfs, igfsSecondary, new IgfsPath("/x/y"));
        assert igfs.info(new IgfsPath("/x/y")).isDirectory();

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/x/y/f"), false)) {
            assert igfs.info(new IgfsPath("/x/y/f")).isFile();
        }

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/x/y/z/f"), false)) {
            assert igfs.info(new IgfsPath("/x/y/z/f")).isFile();
        }

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/x/y/z/t/f"), false)) {
            assert igfs.info(new IgfsPath("/x/y/z/t/f")).isFile();
        }

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/x/y/z/t/t2/t3/t4/t5/f"), false)) {
            assert igfs.info(new IgfsPath("/x/y/z/t/t2/t3/t4/t5/f")).isFile();
        }
    }

    /**
     * Test create when parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testCreateParentRoot() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        createFile(igfs, file, true, chunk);

        checkFile(igfs, igfsSecondary, file, chunk);
    }

    /**
     * Test subsequent "create" commands on the same file without closing the output streams.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoClose() throws Exception {
        if (mode != PRIMARY)
            return;

        create(igfs, paths(DIR, SUBDIR), null);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsOutputStream os1 = null;
                IgfsOutputStream os2 = null;

                try {
                    os1 = igfs.create(FILE, true);
                    os2 = igfs.create(FILE, true);
                } finally {
                    U.closeQuiet(os1);
                    U.closeQuiet(os2);
                }

                return null;
            }
        }, IgfsException.class, null);
    }

    /**
     * Test rename on the file when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testCreateRenameNoClose() throws Exception {
        if (dual)
            return;

        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = null;

        try {
            os = igfs.create(FILE, true);

            igfs.rename(FILE, FILE2);

            os.close();
        }
        finally {
            U.closeQuiet(os);
        }
    }

    /**
     * Test rename on the file parent when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testCreateRenameParentNoClose() throws Exception {
        if (dual)
            return;

        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = null;

        try {
            os = igfs.create(FILE, true);

            igfs.rename(SUBDIR, SUBDIR2);

            os.close();
        }
        finally {
            U.closeQuiet(os);
        }
    }

    /**
     * Test delete on the file when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testCreateDeleteNoClose() throws Exception {
        if (mode != PRIMARY)
            return;

        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = null;

        IgniteUuid id = null;

        try {
            os = igfs.create(FILE, false);

            id = igfs.context().meta().fileId(FILE);

            assert id != null;

            boolean del = igfs.delete(FILE, false);

            assertTrue(del);
            assertFalse(igfs.exists(FILE));
            // The id still exists in meta cache since
            // it is locked for writing and just moved to TRASH.
            // Delete worker cannot delete it for that reason:
            assertTrue(igfs.context().meta().exists(id));

            os.write(chunk);

            os.close();
        }
        finally {
            U.closeQuiet(os);
        }

        final IgniteUuid id0 = id;

        // Delete worker should delete the file once its output stream is finally closed:
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    return !igfs.context().meta().exists(id0);
                }
                catch (IgniteCheckedException ice) {
                    throw new IgniteException(ice);
                }
            }
        }, 5_000L);
    }

    /**
     * Test delete on the file parent when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testCreateDeleteParentNoClose() throws Exception {
        if (mode != PRIMARY)
            return;

        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = null;

        IgniteUuid id = null;

        try {
            os = igfs.create(FILE, false);

            id = igfs.context().meta().fileId(FILE);

            assert id != null;

            boolean del = igfs.delete(SUBDIR, true);

            assertTrue(del);
            assertFalse(igfs.exists(FILE));
            // The id still exists in meta cache since
            // it is locked for writing and just moved to TRASH.
            // Delete worker cannot delete it for that reason:
            assertTrue(igfs.context().meta().exists(id));

            os.write(chunk);

            os.close();
        }
        finally {
            U.closeQuiet(os);
        }

        final IgniteUuid id0 = id;

        // Delete worker should delete the file once its output stream is finally closed:
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    return !igfs.context().meta().exists(id0);
                }
                catch (IgniteCheckedException ice) {
                    throw new IgniteException(ice);
                }
            }
        }, 5_000L);
    }

    /**
     * Test update on the file when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testCreateUpdateNoClose() throws Exception {
        if (dual)
            return;

        if(!propertiesSupported())
            return;

        Map<String, String> props = properties("owner", "group", "0555");

        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = null;

        try {
            os = igfs.create(FILE, true);

            igfs.update(FILE, props);

            os.close();
        }
        finally {
            U.closeQuiet(os);
        }
    }

    /**
     * Checks simple write.
     *
     * @throws Exception On error.
     */
    public void testSimpleWrite() throws Exception {
        IgfsPath path = new IgfsPath("/file1");

        IgfsOutputStream os = igfs.create(path, 128, true/*overwrite*/, null, 0, 256, null);

        os.write(chunk);

        os.close();

        assert igfs.exists(path);
        checkFileContent(igfs, path, chunk);

        os = igfs.create(path, 128, true/*overwrite*/, null, 0, 256, null);

        assert igfs.exists(path);

        os.write(chunk);

        assert igfs.exists(path);

        os.write(chunk);

        assert igfs.exists(path);

        os.close();

        assert igfs.exists(path);
        checkFileContent(igfs, path, chunk, chunk);
    }

    /**
     * Ensure consistency of data during file creation.
     *
     * @throws Exception If failed.
     */
    public void testCreateConsistency() throws Exception {
        final AtomicInteger ctr = new AtomicInteger();
        final AtomicReference<Exception> err = new AtomicReference<>();

        final int threadCnt = 10;

        multithreaded(new Runnable() {
            @Override public void run() {
                int idx = ctr.incrementAndGet();

                final IgfsPath path = new IgfsPath("/file" + idx);

                try {
                    for (int i = 0; i < REPEAT_CNT; i++) {
                        IgfsOutputStream os = igfs.create(path, 128, true/*overwrite*/, null, 0, 256, null);

                        os.write(chunk);

                        os.close();

                        assert igfs.exists(path);
                    }

                    awaitFileClose(igfs, path);

                    checkFileContent(igfs, path, chunk);
                }
                catch (IOException | IgniteCheckedException e) {
                    err.compareAndSet(null, e); // Log the very first error.
                }
            }
        }, threadCnt);

        if (err.get() != null)
            throw err.get();
    }

    /**
     * Ensure create consistency when multiple threads writes to the same file.
     *
     * @throws Exception If failed.
     */
    public void testCreateConsistencyMultithreaded() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicInteger createCtr = new AtomicInteger(); // How many times the file was re-created.
        final AtomicReference<Exception> err = new AtomicReference<>();

        igfs.create(FILE, false).close();

        int threadCnt = 50;

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @SuppressWarnings("ThrowFromFinallyBlock")
            @Override public void run() {
                while (!stop.get() && err.get() == null) {
                    IgfsOutputStream os = null;

                    try {
                        os = igfs.create(FILE, true);

                        os.write(chunk);

                        os.close();

                        createCtr.incrementAndGet();
                    }
                    catch (IgniteException e) {
                        // No-op.
                    }
                    catch (IOException e) {
                        err.compareAndSet(null, e);

                        Throwable[] chain = X.getThrowables(e);

                        Throwable cause = chain[chain.length - 1];

                        System.out.println("Failed due to IOException exception. Cause:");
                        cause.printStackTrace(System.out);
                    }
                    finally {
                        if (os != null)
                            try {
                                os.close();
                            }
                            catch (IOException ioe) {
                                throw new IgniteException(ioe);
                            }
                    }
                }
            }
        }, threadCnt);

        long startTime = U.currentTimeMillis();

        while (err.get() == null
                && createCtr.get() < 500
                && U.currentTimeMillis() - startTime < 60 * 1000)
            U.sleep(100);

        stop.set(true);

        fut.get();

        awaitFileClose(igfs.asSecondary(), FILE);

        if (err.get() != null) {
            X.println("Test failed: rethrowing first error: " + err.get());

            throw err.get();
        }

        checkFileContent(igfs, FILE, chunk);
    }

    /**
     * Test regular append.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TryFinallyCanBeTryWithResources", "EmptyTryBlock"})
    public void testAppend() throws Exception {
        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            assert igfs.exists(SUBDIR);

            createFile(igfs, FILE, true, BLOCK_SIZE, chunk);

            checkFile(igfs, igfsSecondary, FILE, chunk);

            appendFile(igfs, FILE, chunk);

            checkFile(igfs, igfsSecondary, FILE, chunk, chunk);

            // Test create via append:
            IgfsPath path2 = FILE2;

            IgfsOutputStream os = null;

            try {
                os = igfs.append(path2, true/*create*/);

                writeFileChunks(os, chunk);
            } finally {
                U.closeQuiet(os);

                awaitFileClose(igfs, path2);
            }

            try {
                os = igfs.append(path2, false/*create*/);

                writeFileChunks(os, chunk);
            } finally {
                U.closeQuiet(os);

                awaitFileClose(igfs, path2);
            }

            checkFile(igfs, igfsSecondary, path2, chunk, chunk);

            // Negative append (create == false):
            try {
                try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/should-not-be-created"), false)) {
                    // No-op.
                }

                fail("Exception expected");
            } catch (IgniteException e) {
                // okay
            }
            checkNotExist(igfs, igfsSecondary, new IgfsPath("/d1"));

            // Positive mkdirs via append:
            try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/k/l"), true)) {
                checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
                assert igfs.info(new IgfsPath("/k/l")).isFile();
            }

            // Negative append (file is immediate parent):
            try {
                try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/k/l/m"), true)) {
                    // No-op.
                }

                fail("Exception expected");
            } catch (IgniteException e) {
                // okay
            }
            checkNotExist(igfs, igfsSecondary, new IgfsPath("/k/l/m"));
            checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
            assert igfs.info(new IgfsPath("/k/l")).isFile();

            // Negative append (file is in the parent chain):
            try {
                try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/k/l/m/n/o/p"), true)) {
                    // No-op.
                }

                fail("Exception expected");
            } catch (IgniteException e) {
                // okay
            }
            checkNotExist(igfs, igfsSecondary, new IgfsPath("/k/l/m"));
            checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
            assert igfs.info(new IgfsPath("/k/l")).isFile();

            // Negative append (target is a directory):
            igfs.mkdirs(new IgfsPath("/x/y"), null);
            checkExist(igfs, igfsSecondary, new IgfsPath("/x/y"));
            assert igfs.info(new IgfsPath("/x/y")).isDirectory();
            try {
                try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/x/y"), true)) {
                    // No-op.
                }

                fail("Exception expected");
            } catch (IgniteException e) {
                // okay
            }

            // Positive append with create
            try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/x/y/f"), true)) {
                assert igfs.info(new IgfsPath("/x/y/f")).isFile();
            }

            // Positive append with create & 1 mkdirs:
            try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/x/y/z/f"), true)) {
                assert igfs.info(new IgfsPath("/x/y/z/f")).isFile();
            }

            // Positive append with create & 2 mkdirs:
            try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/x/y/z/t/f"), true)) {
                assert igfs.info(new IgfsPath("/x/y/z/t/f")).isFile();
            }

            // Positive mkdirs create & many mkdirs:
            try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/x/y/z/t/t2/t3/t4/t5/f"), true)) {
                assert igfs.info(new IgfsPath("/x/y/z/t/t2/t3/t4/t5/f")).isFile();
            }

            // Negative mkdirs via append (create == false):
            try {
                try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/d1/d2/d3/f"), false)) {
                    // No-op.
                }

                fail("Exception expected");
            } catch (IgniteException e) {
                // okay
            }
            checkNotExist(igfs, igfsSecondary, new IgfsPath("/d1"));
        }
    }

    /**
     * Test create when parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testAppendParentRoot() throws Exception {
        if (appendSupported()) {
            IgfsPath file = new IgfsPath("/" + FILE.name());

            createFile(igfs, file, true, BLOCK_SIZE, chunk);

            appendFile(igfs, file, chunk);

            checkFile(igfs, igfsSecondary, file, chunk, chunk);
        }
    }

    /**
     * Test subsequent "append" commands on the same file without closing the output streams.
     *
     * @throws Exception If failed.
     */
    public void testAppendNoClose() throws Exception {
        if (mode != PRIMARY)
            return;

        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs, FILE, false);

            GridTestUtils.assertThrowsInherited(log(), new Callable<Object>() {
                @Override public Object call() throws Exception {
                    IgfsOutputStream os1 = null;
                    IgfsOutputStream os2 = null;

                    try {
                        os1 = igfs.append(FILE, false);
                        os2 = igfs.append(FILE, false);
                    } finally {
                        U.closeQuiet(os1);
                        U.closeQuiet(os2);
                    }

                    return null;
                }
            }, IgniteException.class, null);
        }
    }

    /**
     * Test rename on the file when it was opened for write(append) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testAppendRenameNoClose() throws Exception {
        if (dual)
            return;

        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs, FILE, false);

            IgfsOutputStream os = null;

            try {
                os = igfs.append(FILE, false);

                igfs.rename(FILE, FILE2);

                os.close();
            } finally {
                U.closeQuiet(os);
            }
        }
    }

    /**
     * Test rename on the file parent when it was opened for write(append) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testAppendRenameParentNoClose() throws Exception {
        if (dual)
            return;

        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs, FILE, false);

            IgfsOutputStream os = null;

            try {
                os = igfs.append(FILE, false);

                igfs.rename(SUBDIR, SUBDIR2);

                os.close();
            } finally {
                U.closeQuiet(os);
            }
        }
    }

    /**
     * Test delete on the file when it was opened for write(append) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testAppendDeleteNoClose() throws Exception {
        if (mode != PRIMARY)
            return;

        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs, FILE, false);

            IgfsOutputStream os = null;
            IgniteUuid id = null;

            try {
                id = igfs.context().meta().fileId(FILE);

                os = igfs.append(FILE, false);

                boolean del = igfs.delete(FILE, false);

                assertTrue(del);
                assertFalse(igfs.exists(FILE));
                assertTrue(igfs.context().meta().exists(id)); // id still exists in meta cache since
                // it is locked for writing and just moved to TRASH.
                // Delete worker cannot delete it for that reason.

                os.write(chunk);

                os.close();
            } finally {
                U.closeQuiet(os);
            }

            assert id != null;

            final IgniteUuid id0 = id;

            // Delete worker should delete the file once its output stream is finally closed:
            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    try {
                        return !igfs.context().meta().exists(id0);
                    } catch (IgniteCheckedException ice) {
                        throw new IgniteException(ice);
                    }
                }
            }, 5_000L);
        }
    }

    /**
     * Test delete on the file parent when it was opened for write(append) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testAppendDeleteParentNoClose() throws Exception {
        if (mode != PRIMARY)
            return;

        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs, FILE, false);

            IgfsOutputStream os = null;
            IgniteUuid id = null;

            try {
                id = igfs.context().meta().fileId(FILE);

                os = igfs.append(FILE, false);

                boolean del = igfs.delete(SUBDIR, true); // Since GG-4911 we allow deletes in this case.

                assertTrue(del);
                assertFalse(igfs.exists(FILE));
                assertTrue(igfs.context().meta().exists(id)); // id still exists in meta cache since
                // it is locked for writing and just moved to TRASH.
                // Delete worker cannot delete it for that reason.

                os.write(chunk);

                os.close();
            } finally {
                U.closeQuiet(os);
            }

            assert id != null;

            final IgniteUuid id0 = id;

            // Delete worker should delete the file once its output stream is finally closed:
            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    try {
                        return !igfs.context().meta().exists(id0);
                    }
                    catch (IgniteCheckedException ice) {
                        throw new IgniteException(ice);
                    }
                }
            }, 5_000L);
        }
    }

    /**
     * Test update on the file when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testAppendUpdateNoClose() throws Exception {
        if (dual)
            return;

        if (appendSupported()) {
            Map<String, String> props = properties("owner", "group", "0555");

            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs, FILE, false);

            IgfsOutputStream os = null;

            try {
                os = igfs.append(FILE, false);

                if (permissionsSupported())
                    igfs.update(FILE, props);

                os.close();
            } finally {
                U.closeQuiet(os);
            }
        }
    }

    /**
     * Ensure consistency of data during appending to a file.
     *
     * @throws Exception If failed.
     */
    public void testAppendConsistency() throws Exception {
        if (appendSupported()) {
            final AtomicInteger ctr = new AtomicInteger();
            final AtomicReference<Exception> err = new AtomicReference<>();

            int threadCnt = 10;

            for (int i = 0; i < threadCnt; i++)
                createFile(igfs, new IgfsPath("/file" + i), false);

            multithreaded(new Runnable() {
                @Override
                public void run() {
                    int idx = ctr.getAndIncrement();

                    IgfsPath path = new IgfsPath("/file" + idx);

                    try {
                        byte[][] chunks = new byte[REPEAT_CNT][];

                        for (int i = 0; i < REPEAT_CNT; i++) {
                            chunks[i] = chunk;

                            IgfsOutputStream os = igfs.append(path, false);

                            os.write(chunk);

                            os.close();

                            assert igfs.exists(path);
                        }

                        awaitFileClose(igfs, path);

                        checkFileContent(igfs, path, chunks);
                    } catch (IOException | IgniteCheckedException e) {
                        err.compareAndSet(null, e); // Log the very first error.
                    }
                }
            }, threadCnt);

            if (err.get() != null)
                throw err.get();
        }
    }

    /**
     * Ensure append consistency when multiple threads writes to the same file.
     *
     * @throws Exception If failed.
     */
    public void testAppendConsistencyMultithreaded() throws Exception {
        if (appendSupported()) {
            final AtomicBoolean stop = new AtomicBoolean();

            final AtomicInteger chunksCtr = new AtomicInteger(); // How many chunks were written.
            final AtomicReference<Exception> err = new AtomicReference<>();

            igfs.create(FILE, false).close();

            int threadCnt = 50;

            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @SuppressWarnings("ThrowFromFinallyBlock")
                @Override
                public void run() {
                    while (!stop.get() && err.get() == null) {
                        IgfsOutputStream os = null;

                        try {
                            os = igfs.append(FILE, false);

                            os.write(chunk);

                            os.close();

                            chunksCtr.incrementAndGet();
                        } catch (IgniteException ignore) {
                            // No-op.
                        } catch (IOException e) {
                            err.compareAndSet(null, e);
                        } finally {
                            if (os != null)
                                try {
                                    os.close();
                                } catch (IOException ioe) {
                                    throw new IgniteException(ioe);
                                }
                        }
                    }
                }
            }, threadCnt);

            long startTime = U.currentTimeMillis();

            while (err.get() == null
                && chunksCtr.get() < 50 && U.currentTimeMillis() - startTime < 60 * 1000)
                U.sleep(100);

            stop.set(true);

            fut.get();

            awaitFileClose(igfs, FILE);

            if (err.get() != null) {
                X.println("Test failed: rethrowing first error: " + err.get());

                throw err.get();
            }

            byte[][] data = new byte[chunksCtr.get()][];

            Arrays.fill(data, chunk);

            checkFileContent(igfs, FILE, data);
        }
    }

    /**
     * Ensure that IGFS is able to stop in case not closed output stream exist.
     *
     * @throws Exception If failed.
     */
    public void testStop() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = igfs.create(FILE, true);

        os.write(chunk);

        igfs.stop(true);

        // Reset test state.
        afterTestsStopped();
        beforeTestsStarted();
    }

    /**
     * Ensure that in case we create the folder A and delete its parent at the same time, resulting file system
     * structure is consistent.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentMkdirsDelete() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.mkdirs(SUBSUBDIR);
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }

                    return true;
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        return igfs.delete(DIR, true);
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            assert res1.get(); // MKDIRS must succeed anyway.

            if (res2.get())
                checkNotExist(igfs, igfsSecondary, DIR, SUBDIR, SUBSUBDIR);
            else
                checkExist(igfs, igfsSecondary, DIR, SUBDIR, SUBSUBDIR);

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure that in case we rename the folder A and delete it at the same time, only one of these requests succeed.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentRenameDeleteSource() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfs, paths(DIR, SUBDIR, DIR_NEW), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        return igfs.delete(SUBDIR, true);
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            res1.get();
            res2.get();

            if (res1.get()) {
                assert !res2.get(); // Rename succeeded, so delete must fail.

                checkExist(igfs, igfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR);
            }
            else {
                assert res2.get(); // Rename failed because delete succeeded.

                checkExist(igfs, DIR); // DIR_NEW should not be synchronized with he primary IGFS.

                if (dual)
                    checkExist(igfsSecondary, DIR, DIR_NEW);

                checkNotExist(igfs, igfsSecondary, SUBDIR, SUBDIR_NEW);
            }

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure that in case we rename the folder A to B and delete B at the same time, FS consistency is not
     * compromised.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentRenameDeleteDestination() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfs, paths(DIR, SUBDIR, DIR_NEW), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        return igfs.delete(SUBDIR_NEW, true);
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            assert res1.get();

            if (res2.get()) {
                // Delete after rename.
                checkExist(igfs, igfsSecondary, DIR, DIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR, SUBDIR_NEW);
            }
            else {
                // Delete before rename.
                checkExist(igfs, igfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR);
            }

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure file system consistency in case two concurrent rename requests are executed: A -> B and B -> A.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentRenames() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfs, paths(DIR, SUBDIR, DIR_NEW), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR_NEW, SUBDIR);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            res1.get();
            res2.get();

            assert res1.get(); // First rename must be successful anyway.

            if (res2.get()) {
                checkExist(igfs, igfsSecondary, DIR, SUBDIR, DIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR_NEW);
            }
            else {
                checkExist(igfs, igfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR);
            }

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure that in case we delete the folder A and delete its parent at the same time, resulting file system
     * structure is consistent.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentDeletes() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        return igfs.delete(SUBDIR, true);
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        return igfs.delete(SUBSUBDIR, true);
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            assert res1.get(); // Delete on the parent must succeed anyway.
            res2.get();

            checkExist(igfs, igfsSecondary, DIR);
            checkNotExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR);

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure that deadlocks do not occur during concurrent rename operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksRename() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2,  RENAME_CNT, 0, 0, 0, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent delete operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksDelete() throws Exception {
         checkDeadlocksRepeat(5, 2, 2, 2,  0, DELETE_CNT, 0, 0, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent update operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksUpdate() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2, 0, 0, UPDATE_CNT, 0, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent directory creation operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksMkdirs() throws Exception {
         checkDeadlocksRepeat(5, 2, 2, 2,  0, 0, 0, MKDIRS_CNT, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent delete & rename operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksDeleteRename() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2,  RENAME_CNT, DELETE_CNT, 0, 0, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent delete & rename operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksDeleteMkdirsRename() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2,  RENAME_CNT, DELETE_CNT, 0, MKDIRS_CNT, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent delete & rename operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksDeleteMkdirs() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2,  0, DELETE_CNT, 0, MKDIRS_CNT, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent file creation operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksCreate() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2, 0, 0, 0, 0, CREATE_CNT);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent operations of various types.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocks() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2,  RENAME_CNT, DELETE_CNT, UPDATE_CNT, MKDIRS_CNT, CREATE_CNT);
    }

    /**
     * Invokes {@link #checkDeadlocks(int, int, int, int, int, int, int, int, int)} for
     *  {@link #REPEAT_CNT} times.
     *
     * @param lvlCnt Total levels in folder hierarchy.
     * @param childrenDirPerLvl How many children directories to create per level.
     * @param childrenFilePerLvl How many children file to create per level.
     * @param primaryLvlCnt How many levels will exist in the primary file system before check start.
     * @param renCnt How many renames to perform.
     * @param delCnt How many deletes to perform.
     * @param updateCnt How many updates to perform.
     * @param mkdirsCnt How many directory creations to perform.
     * @param createCnt How many file creations to perform.
     * @throws Exception If failed.
     */
    private void checkDeadlocksRepeat(final int lvlCnt, final int childrenDirPerLvl, final int childrenFilePerLvl,
        int primaryLvlCnt, int renCnt, int delCnt,
        int updateCnt, int mkdirsCnt, int createCnt) throws Exception {
        if (relaxedConsistency())
            return;

        for (int i = 0; i < REPEAT_CNT; i++) {
            try {
                checkDeadlocks(lvlCnt, childrenDirPerLvl, childrenFilePerLvl, primaryLvlCnt, renCnt, delCnt,
                    updateCnt, mkdirsCnt, createCnt);

                if (i % 10 == 0)
                    X.println(" - " + i);
            }
            finally {
                clear(igfs, igfsSecondary);
            }
        }
    }

    /**
     * Check deadlocks by creating complex directories structure and then executing chaotic operations on it. A lot of
     * exception are expected here. We are not interested in them. Instead, we want to ensure that no deadlocks occur
     * during execution.
     *
     * @param lvlCnt Total levels in folder hierarchy.
     * @param childrenDirPerLvl How many children directories to create per level.
     * @param childrenFilePerLvl How many children file to create per level.
     * @param primaryLvlCnt How many levels will exist in the primary file system before check start.
     * @param renCnt How many renames to perform.
     * @param delCnt How many deletes to perform.
     * @param updateCnt How many updates to perform.
     * @param mkdirsCnt How many directory creations to perform.
     * @param createCnt How many file creations to perform.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private void checkDeadlocks(final int lvlCnt, final int childrenDirPerLvl, final int childrenFilePerLvl,
        int primaryLvlCnt, int renCnt, int delCnt, int updateCnt, int mkdirsCnt, int createCnt) throws Exception {
        assert childrenDirPerLvl > 0;

        // First define file system structure.
        final Map<Integer, List<IgfsPath>> dirPaths = new HashMap<>();
        final Map<Integer, List<IgfsPath>> filePaths = new HashMap<>();

        Queue<IgniteBiTuple<Integer, IgfsPath>> queue = new ArrayDeque<>();

        queue.add(F.t(0, IgfsPath.ROOT)); // Add root directory.

        while (!queue.isEmpty()) {
            IgniteBiTuple<Integer, IgfsPath> entry = queue.poll();

            int lvl = entry.getKey();

            if (lvl < lvlCnt) {
                int newLvl = lvl + 1;

                for (int i = 0; i < childrenDirPerLvl; i++) {
                    IgfsPath path = new IgfsPath(entry.getValue(), "dir-" + newLvl + "-" + i);

                    queue.add(F.t(newLvl, path));

                    if (!dirPaths.containsKey(newLvl))
                        dirPaths.put(newLvl, new ArrayList<IgfsPath>());

                    dirPaths.get(newLvl).add(path);
                }

                for (int i = 0; i < childrenFilePerLvl; i++) {
                    IgfsPath path = new IgfsPath(entry.getValue(), "file-" + newLvl + "-" + i);

                    if (!filePaths.containsKey(newLvl))
                        filePaths.put(newLvl, new ArrayList<IgfsPath>());

                    filePaths.get(newLvl).add(path);
                }
            }
        }

        // Now as we have all paths defined, plan operations on them.
        final Random rand = new Random(SEED);

        final int totalOpCnt = renCnt + delCnt + updateCnt + mkdirsCnt + createCnt;

        if (totalOpCnt == 0)
            throw new RuntimeException("Operations count is zero.");

        final CyclicBarrier barrier = new CyclicBarrier(totalOpCnt);

        Collection<Thread> threads = new ArrayList<>(totalOpCnt);

        // Renames.
        for (int i = 0; i < renCnt; i++) {
            Runnable r = new Runnable() {
                @Override public void run() {
                    try {
                        int fromLvl = rand.nextInt(lvlCnt) + 1;
                        int toLvl = rand.nextInt(lvlCnt) + 1;

                        List<IgfsPath> fromPaths;
                        List<IgfsPath> toPaths;

                        if (rand.nextInt(childrenDirPerLvl + childrenFilePerLvl) < childrenDirPerLvl) {
                            // Rename directories.
                            fromPaths = dirPaths.get(fromLvl);
                            toPaths = dirPaths.get(toLvl);
                        }
                        else {
                            // Rename files.
                            fromPaths = filePaths.get(fromLvl);
                            toPaths = filePaths.get(toLvl);
                        }

                        IgfsPath fromPath = fromPaths.get(rand.nextInt(fromPaths.size()));
                        IgfsPath toPath = toPaths.get(rand.nextInt(toPaths.size()));

                        U.awaitQuiet(barrier);

                        igfs.rename(fromPath, toPath);
                    }
                    catch (IgniteException ignore) {
                        // No-op.
                    }
                }
            };

            threads.add(new Thread(r));
        }

        // Deletes.
        for (int i = 0; i < delCnt; i++) {
            Runnable r = new Runnable() {
                @Override public void run() {
                    try {
                        int lvl = rand.nextInt(lvlCnt) + 1;

                        IgfsPath path = rand.nextInt(childrenDirPerLvl + childrenFilePerLvl) < childrenDirPerLvl ?
                            dirPaths.get(lvl).get(rand.nextInt(dirPaths.get(lvl).size())) :
                            filePaths.get(lvl).get(rand.nextInt(filePaths.get(lvl).size()));

                        U.awaitQuiet(barrier);

                        igfs.delete(path, true);
                    }
                    catch (IgniteException ignore) {
                        // No-op.
                    }
                }
            };

            threads.add(new Thread(r));
        }

        // Updates.
        for (int i = 0; i < updateCnt; i++) {
            Runnable r = new Runnable() {
                @Override public void run() {
                    try {
                        int lvl = rand.nextInt(lvlCnt) + 1;

                        IgfsPath path = rand.nextInt(childrenDirPerLvl + childrenFilePerLvl) < childrenDirPerLvl ?
                            dirPaths.get(lvl).get(rand.nextInt(dirPaths.get(lvl).size())) :
                            filePaths.get(lvl).get(rand.nextInt(filePaths.get(lvl).size()));

                        U.awaitQuiet(barrier);

                        igfs.update(path, properties("owner", "group", null));
                    }
                    catch (IgniteException ignore) {
                        // No-op.
                    }
                }
            };

            threads.add(new Thread(r));
        }

        // Directory creations.
        final AtomicInteger dirCtr = new AtomicInteger();

        for (int i = 0; i < mkdirsCnt; i++) {
            Runnable r = new Runnable() {
                @Override public void run() {
                    try {
                        int lvl = rand.nextInt(lvlCnt) + 1;

                        IgfsPath parentPath = dirPaths.get(lvl).get(rand.nextInt(dirPaths.get(lvl).size()));

                        IgfsPath path = new IgfsPath(parentPath, "newDir-" + dirCtr.incrementAndGet());

                        U.awaitQuiet(barrier);

                        igfs.mkdirs(path);

                    }
                    catch (IgniteException ignore) {
                        // No-op.
                    }
                }
            };

            threads.add(new Thread(r));
        }

        // File creations.
        final AtomicInteger fileCtr = new AtomicInteger();

        for (int i = 0; i < createCnt; i++) {
            Runnable r = new Runnable() {
                @Override public void run() {
                    try {
                        int lvl = rand.nextInt(lvlCnt) + 1;

                        IgfsPath parentPath = dirPaths.get(lvl).get(rand.nextInt(dirPaths.get(lvl).size()));

                        IgfsPath path = new IgfsPath(parentPath, "newFile-" + fileCtr.incrementAndGet());

                        U.awaitQuiet(barrier);

                        try (IgfsOutputStream os = igfs.create(path, true)) {
                             os.write(chunk);
                        }
                    }
                    catch (IOException | IgniteException ignore) {
                        // No-op.
                    }
                }
            };

            threads.add(new Thread(r));
        }

        // Create file/directory structure.
        for (int i = 0; i < lvlCnt; i++) {
            int lvl = i + 1;

            boolean targetToPrimary = !dual || lvl <= primaryLvlCnt;

            IgfsPath[] dirs = dirPaths.get(lvl).toArray(new IgfsPath[dirPaths.get(lvl).size()]);
            IgfsPath[] files = filePaths.get(lvl).toArray(new IgfsPath[filePaths.get(lvl).size()]);

            if (targetToPrimary)
                create(igfs, dirs, files);
            else
                create(igfsSecondary, dirs, files);
        }

        // Start all threads and wait for them to finish.
        for (Thread thread : threads)
            thread.start();

        U.joinThreads(threads, null);
    }

    /**
     * If it is PROXY mode.
     *
     * @return {@code true} if it is PROXY mode.
     */
    protected final boolean isProxy() {
        return mode == PROXY;
    }

    /**
     * Checks #localSpaceSize() and #secondarySpaceSize() Metrics methods.
     *
     * @throws Exception If failed.
     */
    public void testMetricsSpaceSize() throws Exception {
        final IgniteFileSystem fs = igfs;

        // Needed to suppress influence of previous tests:
        fs.resetMetrics();

        checkMetricsZero(fs);

        try (IgfsOutputStream os = fs.create(new IgfsPath("/file1"), 128, true/*overwrite*/, null, 0, 256, null)) {
            os.write(chunk);
        }

        IgfsMetrics m = fs.metrics();

        assertTrue(m.localSpaceSize() >= 0);

        if (dual || isProxy())
            assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return fs.metrics().secondarySpaceSize() > 0;
                }
            }, 1000L));
        else
            assertEquals(0, m.secondarySpaceSize()); // *****

        try (IgfsOutputStream os = fs.create(new IgfsPath("/file2"), 128, true/*overwrite*/, null, 0, 256, null)) {
            os.write(chunk);
        }

        fs.delete(new IgfsPath("/file1"), false);

        m = fs.metrics();

        assertEquals(chunk.length * 2, m.bytesWritten());
        assertEquals(1, m.filesCount());

        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        assertTrue(m.localSpaceSize() >= 0);

        if (dual || isProxy())
            assertTrue(m.secondarySpaceSize() > 0);
        else
            assertEquals(0, m.secondarySpaceSize());

        fs.delete(new IgfsPath("/foo1"), true);
        fs.delete(new IgfsPath("/file2"), true);

        m = fs.metrics();

        assertEquals(0, m.directoriesCount());
        assertEquals(0, m.filesCount());

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return fs.metrics().localSpaceSize() == 0;
            }
        }, 1000L));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return fs.metrics().secondarySpaceSize() == 0;
            }
        }, 1000L));

        fs.resetMetrics();

        checkMetricsZero(fs);
    }

    /**
     * Checks zero metrics state (e.g. after reset).
     *
     * @param fs IgniteFileSystem to test the metrics.
     */
    private void checkMetricsZero(final IgniteFileSystem fs) throws Exception {
        IgfsMetrics m = fs.metrics();

        assertEquals(0, m.blocksReadRemote());
        assertEquals(0, m.blocksReadTotal());

        assertEquals(0, m.blocksWrittenRemote());
        assertEquals(0, m.blocksWrittenTotal());

        assertEquals(0, m.bytesRead());
        assertEquals(0, m.bytesReadTime());

        assertEquals(0, m.bytesWriteTime());
        assertEquals(0, m.bytesWritten());

        assertEquals(0, m.directoriesCount());
        assertEquals(0, m.filesCount());

        assertEquals(0, m.filesOpenedForRead());
        assertEquals(0, m.filesOpenedForWrite());

        assertTrue(m.maxSpaceSize() >= 0);

        // Sizes are updated asynchronously, so we need to wait
        // this to change:
        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return fs.metrics().localSpaceSize() == 0;
            }
        }, 2000L));

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return fs.metrics().secondarySpaceSize() == 0;
            }
        }, 2000L));
    }

    /**
     * Checks block metrics.
     *
     * @throws Exception
     */
    public final void testMetricsBlock() throws Exception {
        final boolean proxy = isProxy();

        final IgfsImpl secIgfsEx = (dual || proxy) ? (IgfsImpl)igfsSecondary.igfs() : null;

        assert !dual || (igfsSecondaryFileSystem != null);
        assert !proxy || (igfsSecondaryFileSystem != null);
        assert !(dual && proxy);

        final IgfsPath file00 = new IgfsPath("/file00");

        igfs.create(file00, false).close();

        final int blockSize = igfs.info(file00).blockSize();

        igfs.await(file00);

        IgfsEntryInfo info = null;

        if (proxy) {
            if (secIgfsEx != null)
                info = secIgfsEx.meta.infoForPath(file00);
        }
        else
            info = ((IgfsImpl)igfs).meta.infoForPath(file00);

        if (info != null) {
            final int blockSize2 = info.blockSize();

            assert blockSize2 == blockSize : "IgfsFile blk size = " + blockSize + ", Meta size = " + blockSize2;
        }
        // **** this fails: see https://issues.apache.org/jira/browse/IGNITE-3877
        // ###############################################################

        final MetricExpectations e = new MetricExpectations(dual, proxy, blockSize);

        // TODO: not disable prefetch, but consider it in assertions.
        igfs.configuration().setPrefetchBlocks(0);

        Assert.assertTrue("https://issues.apache.org/jira/browse/IGNITE-3664", !(igfsSecondaryFileSystem instanceof LocalIgfsSecondaryFileSystem));

        igfs.format();

        igfs.resetMetrics();

        IgfsPath fileRemote = null;

        IgfsPath file1 = new IgfsPath("/file1");
        IgfsPath file2 = new IgfsPath("/file2");

        assert dual == IgfsUtils.isDualMode(igfs.mode(file1));
        assert dual == IgfsUtils.isDualMode(igfs.mode(file2));

        int rmtBlockSize = -1;

        if (secIgfsEx != null) {
            secIgfsEx.format();

            secIgfsEx.resetMetrics();

            fileRemote = new IgfsPath("/fileRemote");

            // Create remote file and write some data to it.
            try (IgfsOutputStream out = secIgfsEx.create(fileRemote, 256, true, null, 1, 256, null)) {
                rmtBlockSize = secIgfsEx.info(fileRemote).blockSize();

                out.write(new byte[rmtBlockSize * 5]);
            }

            assert rmtBlockSize == blockSize;
        }

        // Start metrics measuring.
        final IgfsMetrics initMetrics = igfs.metrics();
        //
        //        assert e.blockSize == blockSize;

        assert blockSize > 0 : "Unexpected block size: " + blockSize;

        checkBlockMetrics(initMetrics, igfs.metrics(), 0, 0, 0, 0, 0, 0);

        e.primBlocksWritten = 7;

        // Write two blocks to the file.
        try (IgfsOutputStream os = igfs.append(file1, true)) {
            os.write(new byte[blockSize * (int)e.primBlocksWritten]);
        }

        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        // Write one more file (one block).
        try (IgfsOutputStream os = igfs.create(file2, 256, true, null, 1, 256, null)) {
            os.write(new byte[blockSize]);
        }

        e.primBlocksWritten++;

        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        e.primBlocksRead = 2;

        // Read data from the first file.
        try (IgfsInputStream is = igfs.open(file1)) {
            is.readFully(0, new byte[blockSize * (int)e.primBlocksRead]);
        }

        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        // Read data from the second file with hits.
        try (IgfsInputStream is = igfs.open(file2)) {
            ((IgfsInputStreamImpl)is).readChunks(0, blockSize);
        }

        e.primBlocksRead++;

        //        checkBlockMetrics(initMetrics, igfs.metrics(),
        //            (int)m.blocksRead, m.totalBlocksRead(), m.rmtBlocksRead(),
        //            blocksWritten, (dual || proxy) ? blocksWritten : 0, blockSize * blocksWritten);
        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        // Clear the first file.
        igfs.create(file1, true).close();

        //        checkBlockMetrics(initMetrics, igfs.metrics(),
        //            blocksRead, proxy ? blocksRead : 0, blockSize * blocksRead,
        //            blocksWritten, (dual || proxy) ? blocksWritten : 0, blockSize * blocksWritten);
        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        // Delete the second file.
        igfs.delete(file2, false);

        //        checkBlockMetrics(initMetrics, igfs.metrics(),
        //            blocksRead, proxy ? blocksRead : 0, blockSize * blocksRead,
        //            blocksWritten, (dual || proxy) ? blocksWritten : 0, blockSize * blocksWritten);
        checkMetricExpectations(initMetrics, igfs.metrics(), e);

        IgfsMetrics metrics;

        //int rmtBlocksRead = 0;

        if (fileRemote != null) {
            // Read remote file.
            try (IgfsInputStream is = igfs.open(fileRemote)) {
                ((IgfsInputStreamImpl)is).readChunks(0, rmtBlockSize);
            }

            e.rmtBlocksRead++;

            //            checkBlockMetrics(initMetrics, igfs.metrics(),
            //                blocksRead + rmtBlocksRead, proxy ? (blocksRead + rmtBlocksRead) : 0, blockSize * blocksRead + rmtBlockSize * rmtBlocksRead,
            //                blocksWritten, (dual || proxy) ? blocksWritten : 0, blockSize * blocksWritten);
            checkMetricExpectations(initMetrics, igfs.metrics(), e);

            // Lets wait for blocks will be placed to cache
            //U.sleep(300); // ?
            igfs.await(fileRemote);

            // Read remote file again.
            try (IgfsInputStream is = igfs.open(fileRemote)) {
                ((IgfsInputStreamImpl)is).readChunks(0, rmtBlockSize);
            }

            if (dual)
                e.primBlocksRead++;
            else
                e.rmtBlocksRead++;

            checkMetricExpectations(initMetrics, igfs.metrics(), e);

            // Write some data to the file working in DUAL mode.
            try (IgfsOutputStream os = igfs.append(fileRemote, false)) {
                os.write(new byte[rmtBlockSize]);
            }

            e.primBlocksWritten++;

            checkMetricExpectations(initMetrics, igfs.metrics(), e);

            igfs.delete(fileRemote, false);

            U.sleep(300);

            assert igfs.metrics().secondarySpaceSize() == 0;

            // Write partial block to the first file.
            try (IgfsOutputStream os = igfs.append(file1, false)) {
                os.write(new byte[blockSize / 2]);
            }

            e.primBlocksWritten += 0.5;

            checkMetricExpectations(initMetrics, igfs.metrics(), e);

            // Now read partial block.
            // Read remote file again.
            try (IgfsInputStream is = igfs.open(file1)) {
                is.seek(blockSize * (int)e.primBlocksWritten);

                ((IgfsInputStreamImpl)is).readChunks(0, blockSize / 2);
            }

            e.primBlocksRead += 0.5;

            checkMetricExpectations(initMetrics, igfs.metrics(), e);
        }

        igfs.resetMetrics();

        metrics = igfs.metrics();

        assert metrics.blocksReadTotal() == 0;
        assert metrics.blocksReadRemote() == 0;
        assert metrics.blocksWrittenTotal() == 0;
        assert metrics.blocksWrittenRemote() == 0;
        assert metrics.bytesRead() == 0;
        assert metrics.bytesReadTime() == 0;
        assert metrics.bytesWritten() == 0;
        assert metrics.bytesWriteTime() == 0;

        assert metrics.bytesReadTime() >= 0;
        assert metrics.bytesWriteTime() >= 0;
    }

    /**
     * Checks basic metrics.
     *
     * @throws Exception
     */
    public void testMetricsBasic() throws Exception {
        Assert.assertNotNull(igfs);

        igfs.format();

        igfs.resetMetrics();

        IgfsMetrics m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(0, m.directoriesCount());
        Assert.assertEquals(0, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        igfs.mkdirs(new IgfsPath("/primary/dir1"));

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(2, m.directoriesCount());
        Assert.assertEquals(0, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        igfs.mkdirs(new IgfsPath("/primary/dir1/dir2/dir3"));
        igfs.mkdirs(new IgfsPath("/primary/dir4"));

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(0, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        IgfsOutputStream out1 = igfs.create(new IgfsPath("/primary/dir1/file1"), false);
        IgfsOutputStream out2 = igfs.create(new IgfsPath("/primary/dir1/file2"), false);
        IgfsOutputStream out3 = igfs.create(new IgfsPath("/primary/dir1/dir2/file"), false);

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(3, m.filesOpenedForWrite());

        out1.write(new byte[10]);
        out2.write(new byte[20]);
        out3.write(new byte[30]);

        out1.close();

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(2, m.filesOpenedForWrite());

        out2.close();
        out3.close();

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        IgfsOutputStream out = igfs.append(new IgfsPath("/primary/dir1/file1"), false);

        out.write(new byte[20]);

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(1, m.filesOpenedForWrite());

        out.write(new byte[20]);

        out.close();

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        IgfsInputStream in1 = igfs.open(new IgfsPath("/primary/dir1/file1"));
        IgfsInputStream in2 = igfs.open(new IgfsPath("/primary/dir1/file2"));

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(2, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        in1.close();
        in2.close();

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(5, m.directoriesCount());
        Assert.assertEquals(3, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        igfs.delete(new IgfsPath("/primary/dir1/file1"), false);
        igfs.delete(new IgfsPath("/primary/dir1/dir2"), true);

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(3, m.directoriesCount());
        Assert.assertEquals(1, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());

        igfs.format();

        // NB: format does not clear secondary file system.
        if (igfsSecondary != null)
            igfsSecondary.format();

        m = igfs.metrics();

        Assert.assertNotNull(m);
        Assert.assertEquals(0, m.directoriesCount());
        Assert.assertEquals(0, m.filesCount());
        Assert.assertEquals(0, m.filesOpenedForRead());
        Assert.assertEquals(0, m.filesOpenedForWrite());
    }

    /**
     * Checks metrics after the file system closed twice.
     *
     * @throws Exception
     */
    public void testMetricsMultipleFsClose() throws Exception {
        igfs.format();

        igfs.resetMetrics();

        IgfsOutputStream out = igfs.create(new IgfsPath("/primary/file"), false);

        out.close();
        out.close();

        IgfsInputStream in = igfs.open(new IgfsPath("/primary/file"));

        in.close();
        in.close();

        IgfsMetrics m = igfs.metrics();

        Assert.assertEquals(0, m.filesOpenedForWrite());
        Assert.assertEquals(0, m.filesOpenedForRead());
    }

//    /**
//     * Test for multiple closings. (Static delegate).
//     *
//     * @throws Exception If failed.
//     */
//    public static void testMultipleClose0(IgniteFileSystem fs) throws Exception {
//    }

    /**
     *
     * @param initMetrics
     * @param metrics
     * @param e
     * @throws Exception
     */
    private static void checkMetricExpectations(IgfsMetrics initMetrics, IgfsMetrics metrics, MetricExpectations e)
        throws Exception {
        checkBlockMetrics(initMetrics, metrics,
            e.totalBlocksRead(), e.rmtBlocksRead(), e.bytesRead(),
            e.totalBlocksWritten(), e.rmtBlocksWritten(), e.bytesWritten());
    }

    private static class MetricExpectations {
        private final boolean proxy;
        private final boolean dual;

        private final int blockSize;
        private final int rmtBlockSize;

        /** How many blocks read from the primary file system after primary write. */
        double primBlocksRead;

        /** How many blocks read read from primary if the
         * file existed in secondary but did not exist in primary (fetched up). */
        double rmtBlocksRead;

        /** How many blocks written to the primary file system. */
        double primBlocksWritten;

        MetricExpectations(boolean dual, boolean proxy, int blockSize) {
            this.dual = dual;
            this.proxy = proxy;

            this.blockSize = blockSize;
            this.rmtBlockSize = blockSize; // NB: currently same value
        }

        double totalBlocksRead0() {
            if (proxy || dual)
                return primBlocksRead + rmtBlocksRead;

            return primBlocksRead;
        }

        int totalBlocksRead() {
            return (int)Math.ceil(totalBlocksRead0());
        }

        double rmtBlocksRead0() {
            return rmtBlocksRead;
        }

        int rmtBlocksRead() {
            if (proxy)
                return totalBlocksRead();

            return (int)Math.ceil(rmtBlocksRead0());
        }

        long bytesRead() {
            return (long)(blockSize * primBlocksRead + rmtBlockSize * rmtBlocksRead);
        }

        int totalBlocksWritten() {
            System.out.println("primBlocksWritten = " + primBlocksWritten);

            return (int)primBlocksWritten;
        }

        int rmtBlocksWritten() {
            return (dual || proxy) ? (int)primBlocksWritten : 0;
        }

        long bytesWritten() {
            return (long)(blockSize * primBlocksWritten);
        }
    }

    /**
     * Ensure overall block-related metrics correctness.
     *
     * @param initMetrics Initial metrics.
     * @param metrics Metrics to check.
     * @param blocksRead Blocks read remote.
     * @param blocksReadRemote Blocks read remote.
     * @param bytesRead Bytes read.
     * @param blocksWrite Blocks write.
     * @param blocksWriteRemote Blocks write remote.
     * @param bytesWrite Bytes write.
     * @throws Exception If failed.
     */
    private static void checkBlockMetrics(IgfsMetrics initMetrics, IgfsMetrics metrics, long blocksRead,
        long blocksReadRemote, long bytesRead, long blocksWrite, long blocksWriteRemote, long bytesWrite)
        throws Exception {
        assert metrics != null;

        Assert.assertEquals(blocksRead, metrics.blocksReadTotal() - initMetrics.blocksReadTotal());
        Assert.assertEquals(blocksReadRemote, metrics.blocksReadRemote() - initMetrics.blocksReadRemote());
        Assert.assertEquals(bytesRead, metrics.bytesRead() - initMetrics.bytesRead());

        Assert.assertEquals(blocksWrite, metrics.blocksWrittenTotal() - initMetrics.blocksWrittenTotal());
        Assert.assertEquals(blocksWriteRemote, metrics.blocksWrittenRemote() - initMetrics.blocksWrittenRemote());
        Assert.assertEquals(bytesWrite, metrics.bytesWritten() - initMetrics.bytesWritten());
    }

}