/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import junit.framework.AssertionFailedError;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.misc.VisorWalTask;
import org.apache.ignite.internal.visor.misc.VisorWalTaskArg;
import org.apache.ignite.internal.visor.misc.VisorWalTaskOperation;
import org.apache.ignite.internal.visor.misc.VisorWalTaskResult;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE;

/**
 * Test correctness of WAL Visor Task and corretness of delete.
 */
public class IgnitePdsUnusedWalSegmentsTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        System.setProperty(IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE, "2");

        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration<Integer, IgnitePdsUnusedWalSegmentsTest.IndexedObject> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        ccfg.setRebalanceMode(CacheRebalanceMode.SYNC);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 32));

        cfg.setCacheConfiguration(ccfg);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg.setPageSize(4 * 1024);

        cfg.setDataStorageConfiguration(dbCfg);

        dbCfg.setWalSegmentSize(4 * 1024 * 1024)
                .setWalHistorySize(Integer.MAX_VALUE)
                .setWalSegments(10)
                .setWalMode(WALMode.LOG_ONLY)
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                        .setMaxSize(100 * 1024 * 1024)
                        .setPersistenceEnabled(true));

        BinaryConfiguration binCfg = new BinaryConfiguration();

        binCfg.setCompactFooter(false);

        cfg.setBinaryConfiguration(binCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Tests correctness of {@link VisorWalTaskOperation}
     *
     * @throws Exception if failed.
     */
    public void testCorrectnessOfDeletionTaskSegments() throws Exception {
        VisorWalTaskResult printRes = null;
        VisorWalTaskResult delRes = null;
        try {
            IgniteEx ignite1 = startGrid(0);

            IgniteEx ignite2 = (IgniteEx) startGridsMultiThreaded(1,1);

            if (checkTopology())
                checkTopology(2);

            ignite1.cluster().active(true);

            IgniteCache<Object, Object> cache = ignite1.cache(CACHE_NAME);

            Random rnd = new Random();

            Map<Integer, IndexedObject> map = new HashMap<>();

            for (int i = 0; i < 40_000; i++) {
                if (i % 1000 == 0)
                    X.println(" >> " + i);

                int k = rnd.nextInt(300_000);

                IndexedObject v = new IndexedObject(rnd.nextInt(10_000));

                cache.put(k, v);

                map.put(k, v);
            }

            GridCacheDatabaseSharedManager dbMgr1 = (GridCacheDatabaseSharedManager)ignite1.context().cache().context()
                      .database();

            GridCacheDatabaseSharedManager dbMgr2 = (GridCacheDatabaseSharedManager) ignite2.context().cache().context()
                    .database();

            dbMgr1.waitForCheckpoint("test");

            dbMgr2.waitForCheckpoint("test");

            for (int i = 0; i < 1_000; i++) {
                int k = rnd.nextInt(300_000);

                IndexedObject v = new IndexedObject(rnd.nextInt(10_000));

                cache.put(k, v);

                map.put(k, v);
            }

            dbMgr1.waitForCheckpoint("test");

            dbMgr2.waitForCheckpoint("test");

            printRes = ignite1.compute().execute(VisorWalTask.class,
                    new VisorTaskArgument<>(ignite1.cluster().node().id(),
                            new VisorWalTaskArg(VisorWalTaskOperation.PRINT_UNUSED_WAL_SEGMENTS), false));

            assertEquals("Check that print task finished with no exceptions", printRes.results().size(),2);

            List<File> walArchives = new ArrayList<>();

            for(Collection<String> pathsPerNode: printRes.results().values()){
                assertTrue("Check that all nodes has unused wal segments", pathsPerNode.size() > 0);

                for(String path: pathsPerNode)
                    walArchives.add(Paths.get(path).toFile());
            }

            delRes = ignite1.compute().execute(VisorWalTask.class,
                    new VisorTaskArgument<>(ignite1.cluster().node().id(),
                            new VisorWalTaskArg(VisorWalTaskOperation.DELETE_UNUSED_WAL_SEGMENTS), false));

            assertEquals("Check that delete task finished with no exceptions", delRes.results().size(),2);

            List<File> walDeletedArchives = new ArrayList<>();

            for(Collection<String> pathsPerNode: delRes.results().values()){
                assertTrue("Check that unused wal segments deleted from all nodes",
                        pathsPerNode.size() > 0);

                for(String path: pathsPerNode)
                    walDeletedArchives.add(Paths.get(path).toFile());
            }

            stopGrid(0);

            stopGrid(1);

            IgniteEx ignite = (IgniteEx) startGridsMultiThreaded(2);

            cache = ignite.cache(CACHE_NAME);

            for (Integer k : map.keySet())
                assertEquals(map.get(k), cache.get(k));

            for(File f: walDeletedArchives)
                assertTrue("Checking existing of deleted wal archive: " + f.getAbsolutePath(), !f.exists());

            for(File f: walArchives)
                assertTrue( "Checking existing of wal archive from print task after delete: " + f.getAbsolutePath(),
                        !f.exists());
        }
        catch (AssertionFailedError e){
            System.out.println("Print task result " + printRes);

            System.out.println("Delete task result " + delRes);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class IndexedObject {
        /** */
        @QuerySqlField(index = true)
        private int iVal;

        /** */
        private byte[] payload = new byte[1024];

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

    /**
     *
     */
    private enum EnumVal {
        /** */
        VAL1,

        /** */
        VAL2,

        /** */
        VAL3
    }
}
