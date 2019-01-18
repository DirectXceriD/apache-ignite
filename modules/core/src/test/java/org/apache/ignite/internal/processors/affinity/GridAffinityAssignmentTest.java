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

package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.util.BitSetIntSet;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.spi.discovery.DiscoveryMetricsProvider;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.internal.util.reflection.Whitebox;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertSame;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.fail;

/**
 * Tests for {@link GridAffinityAssignment}.
 */
@RunWith(JUnit4.class)
public class GridAffinityAssignmentTest {
    /**  */
    protected DiscoveryMetricsProvider metrics = new DiscoveryMetricsProvider() {
        @Override public ClusterMetrics metrics() {
            return null;
        }

        @Override public Map<Integer, CacheMetrics> cacheMetrics() {
            return null;
        }
    };

    /** */
    protected IgniteProductVersion ver = new IgniteProductVersion();

    /**
     * Test GridAffinityAssignment logic when backup threshold is not reached.
     */
    @Test
    public void testPrimaryBackupPartitions() {
        ClusterNode clusterNode1 = node(metrics, ver, "1");
        ClusterNode clusterNode2 = node(metrics, ver, "2");
        ClusterNode clusterNode3 = node(metrics, ver, "3");
        ClusterNode clusterNode4 = node(metrics, ver, "4");
        ClusterNode clusterNode5 = node(metrics, ver, "5");
        ClusterNode clusterNode6 = node(metrics, ver, "6");

        List<ClusterNode> clusterNodes = new ArrayList<ClusterNode>() {{
            add(clusterNode1);
            add(clusterNode2);
            add(clusterNode3);
            add(clusterNode4);
            add(clusterNode5);
            add(clusterNode6);
        }};

        GridAffinityAssignment gridAffinityAssignment = new GridAffinityAssignment(
            new AffinityTopologyVersion(1, 0),
            new ArrayList<List<ClusterNode>>() {{
                add(new ArrayList<ClusterNode>() {{
                    add(clusterNode1);
                    add(clusterNode2);
                    add(clusterNode3);
                    add(clusterNode4);
                }});
                add(new ArrayList<ClusterNode>() {{
                    add(clusterNode1);
                    add(clusterNode2);
                    add(clusterNode3);
                    add(clusterNode4);
                }});
                add(new ArrayList<ClusterNode>() {{
                    add(clusterNode5);
                    add(clusterNode6);
                }});
            }},
            new ArrayList<>(),
            4
        );

        List<Integer> parts = new ArrayList<Integer>() {{
            add(0);
            add(1);
        }};

        assertTrue(gridAffinityAssignment.primaryPartitions(clusterNode1.id()).containsAll(parts));
        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode1.id()).contains(2));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode1.id()).isEmpty());

        for (int i = 1; i < 4; i++) {
            Set<Integer> primary = gridAffinityAssignment.primaryPartitions(clusterNodes.get(i).id());

            assertTrue(primary.isEmpty());

            Set<Integer> backup = gridAffinityAssignment.backupPartitions(clusterNodes.get(i).id());

            assertTrue(backup.containsAll(parts));
            assertFalse(backup.contains(2));
        }

        assertTrue(gridAffinityAssignment.primaryPartitions(clusterNode5.id()).contains(2));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode5.id()).isEmpty());

        assertFalse(gridAffinityAssignment.primaryPartitions(clusterNode6.id()).contains(2));
        assertTrue(gridAffinityAssignment.backupPartitions(clusterNode6.id()).contains(2));

        assertEquals(4, gridAffinityAssignment.getIds(0).size());

        for (int i = 0; i < 4; i++)
            assertTrue(gridAffinityAssignment.getIds(0).contains(clusterNodes.get(i).id()));

        if (AffinityAssignment.IGNITE_ENABLE_AFFINITY_MEMORY_OPTIMIZATION)
            assertNotSame(gridAffinityAssignment.getIds(0), gridAffinityAssignment.getIds(0));
        else
            assertSame(gridAffinityAssignment.getIds(0), gridAffinityAssignment.getIds(0));

        try {
            gridAffinityAssignment.primaryPartitions(clusterNode1.id()).add(1000);

            fail("Unmodifiable exception expected");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }

        try {
            gridAffinityAssignment.backupPartitions(clusterNode1.id()).add(1000);

            fail("Unmodifiable exception expected");
        }
        catch (UnsupportedOperationException ignored) {
            // Ignored.
        }

        Set<Integer> unwrapped = (Set<Integer>)Whitebox.getInternalState(
            gridAffinityAssignment.primaryPartitions(clusterNode1.id()),
            "c"
        );

        if (AffinityAssignment.IGNITE_ENABLE_AFFINITY_MEMORY_OPTIMIZATION)
            assertTrue(unwrapped instanceof BitSetIntSet);
        else
            assertTrue(unwrapped instanceof HashSet);
    }

    /**
     * Test GridAffinityAssignment logic when backup threshold is reached. Basically partitioned cache case.
     */
    @Test
    public void testBackupsMoreThanThreshold() {
        List<ClusterNode> nodes = new ArrayList<>();

        for(int i = 0; i < 10; i++)
            nodes.add(node(metrics, ver, "1" + i));

        GridAffinityAssignment gridAffinityAssignment = new GridAffinityAssignment(
            new AffinityTopologyVersion(1, 0),
            new ArrayList<List<ClusterNode>>() {{
                add(nodes);
            }},
            new ArrayList<>(),
            10
        );

        assertSame(gridAffinityAssignment.getIds(0), gridAffinityAssignment.getIds(0));
    }

    /**
     *
     */
    private GridAffinityAssignment makeGridAffinityAssignment(int parts, int nodesCount, int backups) {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false, parts);

        List<ClusterNode> nodes = new ArrayList<>();
        for (int i = 0; i < nodesCount; i++) {
            TcpDiscoveryNode node = new TcpDiscoveryNode(
                UUID.randomUUID(),
                Collections.singletonList("127.0.0.1"),
                Collections.singletonList("127.0.0.1"),
                0,
                metrics,
                ver,
                i
            );
            node.setAttributes(new HashMap<>());
            nodes.add(node);
        }

        AffinityFunctionContext ctx = new GridAffinityFunctionContextImpl(
            nodes,
            new ArrayList<>(),
            new DiscoveryEvent(),
            new AffinityTopologyVersion(),
            backups
        );
        List<List<ClusterNode>> assignment = aff.assignPartitions(ctx);

        return new GridAffinityAssignment(
            new AffinityTopologyVersion(1, 0),
            assignment,
            new ArrayList<>(),
            backups
        );
    }

    /**
     *
     * @param metrics Metrics.
     * @param v Version.
     * @param consistentId ConsistentId.
     * @return TcpDiscoveryNode.
     */
    protected TcpDiscoveryNode node(DiscoveryMetricsProvider metrics, IgniteProductVersion v, String consistentId) {
        return new TcpDiscoveryNode(
            UUID.randomUUID(),
            Collections.singletonList("127.0.0.1"),
            Collections.singletonList("127.0.0.1"),
            0,
            metrics,
            v,
            consistentId
        );
    }
}
