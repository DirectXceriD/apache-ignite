package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.DiscoverySpiTestListener;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 */
@RunWith(JUnit4.class)
public class TcpDiscoveryReconnectUnstableTopologyTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setAutoActivationEnabled(false);
        cfg.setIncludeEventTypes(EventType.EVTS_DISCOVERY);

        BlockTcpDiscoverySpi spi = new BlockTcpDiscoverySpi();
        spi.setJoinTimeout(1000000000L);

        Field rndAddrsField = U.findField(BlockTcpDiscoverySpi.class, "skipAddrsRandomization");

        assertNotNull(rndAddrsField);

        rndAddrsField.set(spi, true);

        cfg.setDiscoverySpi(spi.setIpFinder(ipFinder));

        cfg.setClientMode(igniteInstanceName.startsWith("client"));

        cfg.setFailureDetectionTimeout(1000000000L);
        cfg.setClientFailureDetectionTimeout(1000000000L);
        cfg.setMetricsUpdateFrequency(1000000000L / 2);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testReconnectUnstableTopology() throws Exception {
        try {
            List<IgniteEx> nodes = new ArrayList<>();

            nodes.add(startGrid(0));

            nodes.add(startGrid(1));

            nodes.add(startGrid("client"));

            nodes.add(startGrid(2));

            nodes.add(startGrid(3));

            for (int i = 0; i < nodes.size(); i++) {
                IgniteEx ex = nodes.get(i);

                assertEquals(i + 1, ex.localNode().order());
            }

            DiscoverySpiTestListener lsnr = new DiscoverySpiTestListener();

            spi(grid("client")).setInternalListener(lsnr);

            lsnr.startBlockReconnect();

            CountDownLatch restartLatch = new CountDownLatch(1);

            IgniteInternalFuture<?> fut = multithreadedAsync(() -> {
                stopGrid(1);
                stopGrid(2);
                stopGrid(3);
                try {
                    startGrid(1);
                    startGrid(2);
                    startGrid(3);
                }
                catch (Exception e) {
                    fail();
                }

                restartLatch.countDown();
            }, 1, "restarter");

            U.awaitQuiet(restartLatch);

            lsnr.stopBlockRestart();

            fut.get();

            startGrid(4);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ig Ignite.
     */
    private TcpDiscoverySpi spi(Ignite ig) {
        return (TcpDiscoverySpi)ig.configuration().getDiscoverySpi();
    }

    /**
     * Discovery SPI with blocking support.
     */
    protected class BlockTcpDiscoverySpi extends TcpDiscoverySpi {
        /** Closure. */
        private volatile IgniteBiClosure<ClusterNode, DiscoveryCustomMessage, Void> clo;

        /**
         * @param clo Closure.
         */
        public void setClosure(IgniteBiClosure<ClusterNode, DiscoveryCustomMessage, Void> clo) {
            this.clo = clo;
        }

        /**
         * @param addr Address.
         * @param msg Message.
         */
        private synchronized void apply(ClusterNode addr, TcpDiscoveryAbstractMessage msg) {
            if (!(msg instanceof TcpDiscoveryCustomEventMessage))
                return;

            TcpDiscoveryCustomEventMessage cm = (TcpDiscoveryCustomEventMessage)msg;

            DiscoveryCustomMessage delegate;

            try {
                DiscoverySpiCustomMessage custMsg = cm.message(marshaller(), U.resolveClassLoader(ignite().configuration()));

                assertNotNull(custMsg);

                delegate = ((CustomMessageWrapper)custMsg).delegate();

            }
            catch (Throwable throwable) {
                throw new RuntimeException(throwable);
            }

            if (clo != null)
                clo.apply(addr, delegate);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(
            Socket sock,
            TcpDiscoveryAbstractMessage msg,
            byte[] data,
            long timeout
        ) throws IOException {
            if (spiCtx != null)
                apply(spiCtx.localNode(), msg);

            super.writeToSocket(sock, msg, data, timeout);
        }

        /** {@inheritDoc} */
        @Override protected void writeToSocket(Socket sock,
            OutputStream out,
            TcpDiscoveryAbstractMessage msg,
            long timeout) throws IOException, IgniteCheckedException {
            if (spiCtx != null)
                apply(spiCtx.localNode(), msg);

            super.writeToSocket(sock, out, msg, timeout);
        }
    }
}
