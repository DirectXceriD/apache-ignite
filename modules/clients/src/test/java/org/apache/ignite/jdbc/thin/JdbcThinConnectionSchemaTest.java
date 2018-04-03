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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class JdbcThinConnectionSchemaTest extends GridCommonAbstractTest {

    private static final String CLIENT_NODE_NAME = "ClientNode";

    private static final String SERVER_NODE_NAME = "ServerNode";

    private static final String PRESTARTED_CACHE = "PrestartedCache";

    private IgniteEx clientNode;

    private IgniteEx serverNode;

    private static final String SERVER_CACHE = "ServerNodeCache";

    /** Name of the cache, that is created using client Ignite instance, NOT the cache on client. */
    private static final String CLIENT_CACHE = "ClientNodeCache";

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        // Node that we don't connect to contains prestarted cache with table.
        startGrid(SERVER_NODE_NAME, withPrestartedCache(optimize(getConfiguration(SERVER_NODE_NAME))), null);

        startGrid(CLIENT_NODE_NAME, optimize(getConfiguration(CLIENT_NODE_NAME).setClientMode(true)), null);
    }

    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();
        stopAllGrids();
    }

    private CacheConfiguration<Long, UUID> newCacheCfg(String name) {
        CacheConfiguration<Long, UUID> ccfg = new CacheConfiguration<>(name);

        ccfg.setIndexedTypes(Long.class, UUID.class);

        return ccfg;
    }

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        serverNode = grid(SERVER_NODE_NAME);
        clientNode = grid(CLIENT_NODE_NAME);

        serverNode.createCache(newCacheCfg(SERVER_CACHE));
        clientNode.createCache(newCacheCfg(CLIENT_CACHE));
    }

    @Override protected void afterTest() throws Exception {
        serverNode.destroyCache(SERVER_CACHE);
        clientNode.destroyCache(CLIENT_CACHE);

        super.afterTest();
    }

    private int portOf(IgniteEx node) {
        Collection<GridPortRecord> recs = node.context().ports().records();

        GridPortRecord cliLsnrRec = null;

        for (GridPortRecord rec : recs) {
            if (rec.clazz() == ClientListenerProcessor.class)
                return rec.port();
        }

        throw new RuntimeException("Could not find port to connect to node " + node);
    }

    private IgniteConfiguration withPrestartedCache(IgniteConfiguration cfg) throws Exception {
        CacheConfiguration<Long, UUID> ccfg = new CacheConfiguration<Long, UUID>(PRESTARTED_CACHE)
            .setIndexedTypes(Long.class, UUID.class).setCacheMode(CacheMode.PARTITIONED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    public void testNonExistingSchemas() {
        assertSchemaMissed("notExistingSchema");

        assertSchemaMissed("\"Public\"");

        assertSchemaMissed("ServerNodeCache"); // not quoted

        assertSchemaMissed("\"SERVERNODECACHE\"");

        assertSchemaMissed("\"ServerNodeCa\"");

        assertSchemaMissed("\"CLIENTNODECACHE\"");
    }

    /**
     * Connect to one node and check that schema defined in the other node's cache configuration exists.
     */
    public void testPrestartedCacheTable() {
        assertSchemaExist("\"" + PRESTARTED_CACHE + "\"");
    }

    public void testDefaultSchema() {
        assertSchemaExist(""); // implicitly "PUBLIC"

        assertSchemaExist("public");

        assertSchemaExist("Public");

        assertSchemaExist("\"PUBLIC\"");
    }

    public void testExistingSchemas() {
        assertSchemaExist("\"ClientNodeCache\"");

        assertSchemaExist("\"ServerNodeCache\"");
    }

    public void assertSchemaExist(String schema) {
        int port = portOf(clientNode);

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:" + port + "/" + schema)) {
            // do nothing
        }
        catch (SQLException sqlEx) {
            throw new AssertionError("Schema " + schema + " seems to be missed, but it should exist.", sqlEx);
        }
        // todo: create for server node too.
    }

    public void assertSchemaMissed(String schema) {
        Callable<Void> mustThrow = () -> {
            try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/" + schema)) {
                // do nothing
            }

            return null;
        };

        GridTestUtils.assertThrows(log, mustThrow, SQLException.class, "Schema with name");
    }
}
