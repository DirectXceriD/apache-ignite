/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.baseline;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class IgniteAbsentEvictionNodeOutOfBaselineTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setWalSegmentSize(512 * 1024)
            .setWalSegments(4)
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(256 * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY));

        return cfg;
    }

    public void test() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        startGrid(1);

        startGrid(2);

        ignite0.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite0.getOrCreateCache("test");

        for(int i = 0; i< 100; i++) {
            cache.put(i, i);
        }

        stopGrid(2);

        resetBaselineTopology();

        awaitPartitionMapExchange();

        for(int i = 0; i< 1000; i++) {
            cache.put(i, i);
        }

        startGrid(2);

        awaitPartitionMapExchange();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

//        cleanPersistenceDir();
    }
}
