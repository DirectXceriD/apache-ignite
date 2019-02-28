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

package org.apache.ignite.yardstick;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.yardstick.cache.IgnitePutBenchmark;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriver;
import org.yardstickframework.BenchmarkDriverStartUp;
import org.yardstickframework.BenchmarkUtils;

/**
 * Utils.
 */
public class IgniteBenchmarkUtils {
    /**
     * Scheduler executor.
     */
    private static final ScheduledExecutorService exec =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override public Thread newThread(Runnable run) {
                Thread thread = Executors.defaultThreadFactory().newThread(run);

                thread.setDaemon(true);

                return thread;
            }
        });

    /**
     * Utility class constructor.
     */
    private IgniteBenchmarkUtils() {
        // No-op.
    }

    /**
     * @param igniteTx Ignite transaction.
     * @param txConcurrency Transaction concurrency.
     * @param clo Closure.
     * @return Result of closure execution.
     * @throws Exception If failed.
     */
    public static <T> T doInTransaction(IgniteTransactions igniteTx, TransactionConcurrency txConcurrency,
        TransactionIsolation txIsolation, Callable<T> clo) throws Exception {
        while (true) {
            Transaction tx = null;

            try {
                tx = igniteTx.txStart(txConcurrency, txIsolation);
            }
            catch (Exception e){
                BenchmarkUtils.println("Failed to start transaction " + e.getMessage());

                if(tx != null)
                    tx.close();

                continue;
            }

            try {
                T res = clo.call();

                tx.commit();

                return res;
            }
            catch (CacheException e) {
                if (e.getMessage().contains("Cannot serialize transaction due to write conflict"))
                    tx.rollback();

                if (e.getCause() instanceof ClusterTopologyException) {
                    ClusterTopologyException topEx = (ClusterTopologyException)e.getCause();

                    topEx.retryReadyFuture().get();
                }
                else
                    throw e;
            }
            catch (ClusterTopologyException e) {
                e.retryReadyFuture().get();
            }
            catch (TransactionRollbackException | TransactionOptimisticException ignore) {
                // Safe to retry right away.
            }
            finally {
                tx.close();
            }
        }
    }

    /**
     * Starts nodes/driver in single JVM for quick benchmarks testing.
     *
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final String cfg = "modules/yardstick/config/ignite-localhost-config.xml";

        final Class<? extends BenchmarkDriver> benchmark = IgnitePutBenchmark.class;

        final int threads = 1;

        final boolean clientDriverNode = true;

        final int extraNodes = 1;

        final int warmUp = 60;
        final int duration = 120;

        final int range = 100_000;

        final boolean throughputLatencyProbe = false;

        for (int i = 0; i < extraNodes; i++) {
            IgniteConfiguration nodeCfg = Ignition.loadSpringBean(cfg, "grid.cfg");

            nodeCfg.setIgniteInstanceName("node-" + i);
            nodeCfg.setMetricsLogFrequency(0);

            Ignition.start(nodeCfg);
        }

        ArrayList<String> args0 = new ArrayList<>();

        addArg(args0, "-t", threads);
        addArg(args0, "-w", warmUp);
        addArg(args0, "-d", duration);
        addArg(args0, "-r", range);
        addArg(args0, "-dn", benchmark.getSimpleName());
        addArg(args0, "-sn", "IgniteNode");
        addArg(args0, "-cfg", cfg);
        addArg(args0, "-wom", "PRIMARY");

        if (throughputLatencyProbe)
            addArg(args0, "-pr", "ThroughputLatencyProbe");

        if (clientDriverNode)
            args0.add("-cl");

        BenchmarkDriverStartUp.main(args0.toArray(new String[args0.size()]));
    }

    /**
     * @param args Arguments.
     * @param arg Argument name.
     * @param val Argument value.
     */
    private static void addArg(List<String> args, String arg, Object val) {
        args.add(arg);
        args.add(val.toString());
    }

    /**
     * Prints non-system cache sizes during preload.
     *
     * @param node Ignite node.
     * @param cfg Benchmark configuration.
     * @param logsInterval Time interval in milliseconds between printing logs.
     */
    public static PreloadLogger startPreloadLogger(IgniteNode node, BenchmarkConfiguration cfg, long logsInterval) {
        PreloadLogger lgr = new PreloadLogger(node, cfg);

        ScheduledFuture<?> fut = exec.scheduleWithFixedDelay(lgr, 0L, logsInterval, TimeUnit.MILLISECONDS);

        lgr.setFuture(fut);

        BenchmarkUtils.println(cfg, "Preload logger was started.");

        return lgr;
    }

    /**
     * Checks if address list contains only localhost addresses.
     *
     * @param adrList address list.
     * @return {@code true} if address list contains only localhost addresses  or {@code false} otherwise.
     */
    static boolean checkIfOnlyLocalhost(Collection<String> adrList) {
        return countLocalAdr(adrList) == adrList.size();
    }

    /**
     * Checks if address list contains no localhost addresses.
     *
     * @param adrList address list.
     * @return {@code true} if address list contains no localhost addresses or {@code false} otherwise.
     */
    static boolean checkIfNoLocalhost(Collection<String> adrList) {
        return countLocalAdr(adrList) == 0;
    }

    /**
     * Counts localhost addresses in list.
     *
     * @param adrList address list.
     * @return {@code int} Number of localhost addresses in list.
     */
    private static int countLocalAdr(Collection<String> adrList) {
        int locAdrNum = 0;

        for (String adr : adrList) {
            if (adr.contains("127.0.0.1") || adr.contains("localhost"))
                locAdrNum++;
        }

        return locAdrNum;
    }

    /**
     * Parses portRange string.
     *
     * @param portRange {@code String} port range as 'int..int'.
     * @return {@code Collection} List of ports.
     */
    static Collection<Integer> getPortList(String portRange) {
        int firstPort;
        int lastPort;

        try {
            String[] numArr = portRange.split("\\.\\.");

            firstPort = Integer.valueOf(numArr[0]);
            lastPort = numArr.length > 1 ? Integer.valueOf(numArr[1]) : firstPort;
        }
        catch (NumberFormatException e) {
            BenchmarkUtils.println(String.format("Failed to parse PORT_RANGE property: %s; %s",
                portRange, e.getMessage()));

            throw new IllegalArgumentException(String.format("Wrong value for PORT_RANGE property: %s",
                portRange));
        }

        Collection<Integer> res = new HashSet<>();

        for (int port = firstPort; port <= lastPort; port++)
            res.add(port);

        return res;
    }

    /**
     *
     * @param cfg Benchmark configuration.
     */
    public static void setArgsFromProperties(BenchmarkConfiguration cfg, IgniteBenchmarkArguments args){
        Map<String, String> customProps = cfg.customProperties();

        if(customProps.get("IGNITE_CONFIG") != null)
            args.configuration(customProps.get("IGNITE_CONFIG"));

        if(customProps.get("BACKUPS") != null)
            args.backups(Integer.valueOf(customProps.get("BACKUPS")));

        if(args.nodes() == 1){
            if (customProps.get("NODES_NUM") != null)
                args.nodes(Integer.valueOf(customProps.get("NODES_NUM")));
            else {
                int nodesNum = 0;

                String sHosts = customProps.get("SERVER_HOSTS");
                String dHosts = customProps.get("DRIVER_HOSTS");

                if (sHosts != null)
                    nodesNum += sHosts.split(",").length;

                if (dHosts != null)
                    nodesNum += dHosts.split(",").length;

                BenchmarkUtils.println(String.format("Setting nodes num as %d", nodesNum));

                args.nodes(nodesNum);
            }
        }

        if(customProps.get("SYNC_MODE") != null)
            args.syncMode(CacheWriteSynchronizationMode.valueOf(customProps.get("SYNC_MODE")));
    }
}
