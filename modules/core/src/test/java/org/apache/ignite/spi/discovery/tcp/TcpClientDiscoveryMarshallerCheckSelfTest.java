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

package org.apache.ignite.spi.discovery.tcp;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.marshaller.jdk.JdkMarshaller;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}.
 */
public class TcpClientDiscoveryMarshallerCheckSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean testFooter;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(igniteInstanceName);

        if (testFooter) {
            cfg.setMarshaller(new BinaryMarshaller());

            TcpDiscoverySpi spi = new TcpDiscoverySpi();

            spi.setJoinTimeout(-1); // IGNITE-605, and further tests limitation bypass

            cfg.setDiscoverySpi(spi);

            if (igniteInstanceName.endsWith("0")) {
                cfg.setClientMode(true);
                BinaryConfiguration bc = new BinaryConfiguration();
                bc.setCompactFooter(false);
                cfg.setBinaryConfiguration(bc);
            }
        }
        else {
            if (igniteInstanceName.endsWith("0"))
                cfg.setMarshaller(new JdkMarshaller());
            else {
                cfg.setClientMode(true);
                cfg.setMarshaller(new BinaryMarshaller());
            }

            TcpDiscoverySpi spi = new TcpDiscoverySpi().setIpFinder(ipFinder);

            cfg.setDiscoverySpi(spi);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMarshallerInConsistency() throws Exception {
        startGrid(0);

        try {
            startGrid(1);

            fail("Expected SPI exception was not thrown.");
        }
        catch (IgniteCheckedException e) {
            Throwable ex = e.getCause().getCause();

            assertTrue(ex instanceof IgniteSpiException);
            assertTrue(ex.getMessage().contains("Local node's marshaller differs from remote node's marshaller"));
        }
    }

    /**
     * Starts client-server grid with different binary configurations.
     *
     * @throws Exception If failed.
     */
    private void clientServerInconsistentConfigFail(boolean multiNodes) throws Exception {
        testFooter = true;

        IgniteEx ig0 = startGrid(1);
        IgniteCache ic = ig0.getOrCreateCache("cahe_name");
        if (multiNodes)
            startGrid(2);

        try {
            IgniteEx ig = startGrid(0);

            for (String c : ig.cacheNames())
                System.out.println(c);

            fail("Expected SPI exception was not thrown, multiNodes=" + multiNodes);
        } catch (IgniteCheckedException expect) {
            Throwable ex = expect.getCause().getCause();

            assertTrue(ex instanceof IgniteSpiException);
            assertTrue("Catched exception: " + ex.getMessage(), ex.getMessage().contains("Local node's binary " +
                    "configuration is not equal to remote node's binary configuration"));
        } finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testInconsistentFooterConfigSingle() throws Exception {
        clientServerInconsistentConfigFail(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInconsistentFooterConfigMulti() throws Exception {
        for (int i = 0; i < 10; ++i)
            clientServerInconsistentConfigFail(true);
    }
}
