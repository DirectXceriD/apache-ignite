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

package org.apache.ignite.test;

import java.io.File;
import java.util.Collections;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.console.WebConsoleServer;
import org.apache.ignite.console.config.WebConsoleConfiguration;
import org.apache.ignite.console.routes.AccountRouter;
import org.apache.ignite.console.routes.AdminRouter;
import org.apache.ignite.console.routes.AgentDownloadRouter;
import org.apache.ignite.console.routes.ConfigurationsRouter;
import org.apache.ignite.console.routes.NotebooksRouter;
import org.apache.ignite.console.routes.RestApiRouter;
import org.apache.ignite.console.services.AccountsService;
import org.apache.ignite.console.services.ConfigurationsService;
import org.apache.ignite.console.services.NotebooksService;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Web Console Launcher.
 */
public class WebConsoleLauncher extends AbstractVerticle {
    /**
     * Main entry point.
     *
     * @param args Arguments.
     */
    public static void main(String... args) {
        System.out.println("Starting Ignite Web Console Server...");

        Ignite ignite = startIgnite();

        VertxOptions options = new VertxOptions()
            .setBlockedThreadCheckInterval(1000L * 60L * 60L);
//            .setClusterManager(new IgniteClusterManager(ignite));

        Vertx vertx = Vertx.vertx(options);

//        Vertx.clusteredVertx(options, res -> {
//            if (res.failed()) {
//                ignite.log().error("Failed to start clustered Vertx!");
//
//                return;
//            }
//
//            Vertx vertx = res.result();

            AccountsService accSrvc = new AccountsService(ignite);

            RestApiRouter accRouter = new AccountRouter(ignite, vertx, accSrvc);
            RestApiRouter cfgsRouter = new ConfigurationsRouter(ignite, vertx, new ConfigurationsService(ignite));
            RestApiRouter notebooksRouter = new NotebooksRouter(ignite, vertx);
            RestApiRouter downloadRouter = new AgentDownloadRouter(ignite, vertx, "/your/path", "ignite-web-agent-x.y.z");
            RestApiRouter adminRouter = new AdminRouter(ignite, vertx, accSrvc);

            WebConsoleConfiguration cfg = new WebConsoleConfiguration();

            vertx.deployVerticle(new NotebooksService(ignite));

            vertx.deployVerticle(new WebConsoleServer(
                cfg,
                ignite,
                accRouter,
                cfgsRouter,
                notebooksRouter,
                downloadRouter,
                adminRouter
            ));

            System.out.println("Ignite Web Console Server started");
//        });
    }

    /**
     * Start Ignite.
     *
     * @return Ignite instance.
     */
    private static Ignite startIgnite() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName("Web Console backend");
        cfg.setConsistentId("web-console-backend");
        cfg.setMetricsLogFrequency(0);
        cfg.setLocalHost("127.0.0.1");

        cfg.setWorkDirectory(new File(U.getIgniteHome(), "work-web-console").getAbsolutePath());

        TcpDiscoverySpi discovery = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:60800"));

        discovery.setLocalPort(60800);
        discovery.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discovery);

        DataStorageConfiguration dataStorageCfg = new DataStorageConfiguration();

        DataRegionConfiguration dataRegionCfg = new DataRegionConfiguration();

        dataRegionCfg.setPersistenceEnabled(true);

        dataStorageCfg.setDefaultDataRegionConfiguration(dataRegionCfg);

        cfg.setDataStorageConfiguration(dataStorageCfg);

        cfg.setConnectorConfiguration(null);

        Ignite ignite = Ignition.getOrStart(cfg);

        ignite.cluster().active(true);

        return ignite;
    }
}
