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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.cache.CacheException;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.apache.ignite.yardstick.cache.IgniteStreamerZipBenchmark;
import org.apache.ignite.yardstick.cache.model.ZipEntity;
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
            try (Transaction tx = igniteTx.txStart(txConcurrency, txIsolation)) {
                T res = clo.call();

                tx.commit();

                return res;
            }
            catch (CacheException e) {
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
        }
    }

    /**
     * Starts nodes/driver in single JVM for quick benchmarks testing.
     *
     * @param args Command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        final String cfg = "modules/yardstick/config/ignite-base-streamer-config.xml";

        final Class<? extends BenchmarkDriver> benchmark = IgniteStreamerZipBenchmark.class;

        final int threads = 1;

        final boolean clientDriverNode = true;

        final int extraNodes = 3;

        final int warmUp = 0;
        final int duration = 120;

        final int range = 1_000_000;

        final boolean throughputLatencyProbe = false;

        for (int i = 0; i < extraNodes; i++) {
            IgniteConfiguration nodeCfg = Ignition.loadSpringBean(cfg, "base-ignite.cfg");

            nodeCfg.setIgniteInstanceName("node-" + i);
            nodeCfg.setMetricsLogFrequency(0);

            CacheConfiguration ccfg = new CacheConfiguration("streamer-zip");
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            ccfg.setBackups(0);
//            ccfg.setOnheapCacheEnabled(false);
            ccfg.setCacheMode(CacheMode.PARTITIONED);
//            ccfg.setRebalanceThrottle(0);
//            ccfg.setRebalanceBatchSize(5 * 1024 * 1024); //5MB
            ccfg.setRebalanceThreadPoolSize(4);
//            ccfg.setIndexedTypes(String.class, ZipEntity.class);
            QueryEntity qryEntity = new QueryEntity();

            qryEntity.setTableName("ZIP_ENTITY");
            qryEntity.setKeyType("java.lang.String");
            qryEntity.setValueType(ZipEntity.class.getName());

            qryEntity.addQueryField("BUSINESSDATE", "long", "BUSINESSDATE");
            qryEntity.addQueryField("RISKSUBJECTID", "java.lang.String", "RISKSUBJECTID");
            qryEntity.addQueryField("SERIESDATE", "long", "SERIESDATE");
            qryEntity.addQueryField("SNAPVERSION", "java.lang.String", "SNAPVERSION");
            qryEntity.addQueryField("VARTYPE", "java.lang.String", "VARTYPE");

            ccfg.setQueryEntities(Collections.singleton(qryEntity));

            nodeCfg.setCacheConfiguration(ccfg);

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
        addArg(args0, "-cts", 8);
        addArg(args0, "-ct", "NONE");
        addArg(args0, "-sr", "0.9");

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
}
