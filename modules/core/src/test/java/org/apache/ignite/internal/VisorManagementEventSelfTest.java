package org.apache.ignite.internal;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_VISOR_TASK_STARTED;;

/**
 *
 */
public class VisorManagementEventSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Enable visor management events.
        cfg.setIncludeEventTypes(
            EVT_VISOR_TASK_STARTED
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration<Integer, Integer>()
                .setName("TEST")
                .setIndexedTypes(Integer.class, Integer.class)
                .setStatisticsEnabled(true)
        );

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        List<String> addrs = Arrays.asList("127.0.0.1:47500..47502");

        ipFinder.setAddresses(addrs);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTest();
    }

    /**
     * Current test case start valid one node visor task that has GridVisorManagementTask annotation.
     * No exceptions are expected.
     *
     * @throws Exception If failed.
     */
    public void testManagementOneNodeVisorTask() throws Exception {
        IgniteEx ignite = startGrid(0);

        doTestVisorTask(TestManagementVisorOneNodeTask.class, new VisorTaskArgument(), ignite);
    }

    /**
     * Current test case start valid multi node visor task that has GridVisorManagementTask annotation.
     * No exceptions are expected.
     *
     * @throws Exception If failed.
     */
    public void testManagementMultiNodeVisorTask() throws Exception {
        IgniteEx ignite = startGrid(0);

        doTestVisorTask(TestManagementVisorMultiNodeTask.class, new VisorTaskArgument(), ignite);
    }

    /**
     * Current test case start one node visor task that has not GridVisorManagementTask annotation.
     * No exceptions are expected.
     *
     * @throws Exception If failed.
     */
    public void testNotManagementOneNodeVisorTask() throws Exception {
        IgniteEx ignite = startGrid(0);

        doTestNotManagementVisorTask(TestNotManagementVisorOneNodeTask.class, new VisorTaskArgument(), ignite);
    }

    /**
     * Current test case start multi node visor task that has not GridVisorManagementTask annotation.
     * No exceptions are expected.
     *
     * @throws Exception If failed.
     */
    public void testNotManagementMultiNodeVisorTask() throws Exception {
        IgniteEx ignite = startGrid(0);

        doTestNotManagementVisorTask(TestNotManagementVisorMultiNodeTask.class, new VisorTaskArgument(), ignite);
    }

    /**
     * @param cls class of the task.
     * @param arg argument.
     * @param ignite instance of Ignite.
     *
     * @throws Exception If failed.
     */
    private <T, R>  void doTestVisorTask(
        Class<? extends ComputeTask<VisorTaskArgument<T>, R>> cls, T arg, IgniteEx ignite) throws Exception
    {
        final AtomicReference<TaskEvent> evt = new AtomicReference<>();

        final CountDownLatch evtLatch = new CountDownLatch(1);

        ignite.events().localListen(new IgnitePredicate<TaskEvent>() {
            @Override public boolean apply(TaskEvent e) {
                evt.set(e);

                evtLatch.countDown();

                return false;
            }
        }, EventType.EVT_VISOR_TASK_STARTED);

        for (ClusterNode node : ignite.cluster().forServers().nodes())
            ignite.compute().executeAsync(cls, new VisorTaskArgument<>(node.id(), arg, true));

        assertTrue(evtLatch.await(10000, TimeUnit.MILLISECONDS));

        assertNotNull(evt.get());
    }

    /**
     * @param cls class of the task.
     * @param arg argument.
     * @param ignite instance of Ignite.
     *
     * @throws Exception If failed.
     */
    private <T, R>  void doTestNotManagementVisorTask(
        Class<? extends ComputeTask<VisorTaskArgument<T>, R>> cls, T arg, IgniteEx ignite) throws Exception
    {
        final AtomicReference<TaskEvent> evt = new AtomicReference<>();

        final CountDownLatch evtLatch = new CountDownLatch(1);

        ignite.events().localListen(new IgnitePredicate<TaskEvent>() {
            @Override public boolean apply(TaskEvent e) {
                evt.set(e);

                evtLatch.countDown();

                return false;
            }
        }, EventType.EVT_VISOR_TASK_STARTED);

        for (ClusterNode node : ignite.cluster().forServers().nodes())
            ignite.compute().executeAsync(cls, new VisorTaskArgument<>(node.id(), arg, true));

        assertFalse(evtLatch.await(10000, TimeUnit.MILLISECONDS));
    }
}
