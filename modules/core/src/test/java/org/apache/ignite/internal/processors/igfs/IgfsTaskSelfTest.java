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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.igfs.mapreduce.*;
import org.apache.ignite.igfs.mapreduce.records.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.igfs.IgfsMode.*;

/**
 * Tests for {@link org.apache.ignite.igfs.mapreduce.IgfsTask}.
 */
public class IgfsTaskSelfTest extends IgfsCommonAbstractTest {
    /** Predefined words dictionary. */
    private static final String[] DICTIONARY = new String[] {"word0", "word1", "word2", "word3", "word4", "word5",
        "word6", "word7"};

    /** File path. */
    private static final IgfsPath FILE = new IgfsPath("/file");

    /** Shared IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Block size: 64 Kb. */
    private static final int BLOCK_SIZE = 64 * 1024;

    /** Total words in file. */
    private static final int TOTAL_WORDS = 2 * 1024 * 1024;

    /** Node count */
    private static final int NODE_CNT = 4;

    /** Repeat count. */
    private static final int REPEAT_CNT = 10;

    /** IGFS. */
    private static IgniteFileSystem igfs;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < NODE_CNT; i++) {
            Ignite g = G.start(config(i));

            if (i + 1 == NODE_CNT)
                igfs = g.fileSystem("igfs");
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        igfs.format();
    }

    /**
     * Create grid configuration.
     *
     * @param idx Node index.
     * @return Grid configuration
     */
    private IgniteConfiguration config(int idx) {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(BLOCK_SIZE);
        igfsCfg.setDefaultMode(PRIMARY);
        igfsCfg.setFragmentizerEnabled(false);

        CacheConfiguration dataCacheCfg = new CacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setDistributionMode(PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(1));
        dataCacheCfg.setBackups(0);

        CacheConfiguration metaCacheCfg = new CacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        IgniteConfiguration cfg = new IgniteConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setGridName("node-" + idx);

        return cfg;
    }

    /**
     * Test task.
     *
     * @throws Exception If failed.
     */
    public void testTask() throws Exception {
        U.sleep(3000); // TODO: Sleep in order to wait for fragmentizing to finish.

        for (int i = 0; i < REPEAT_CNT; i++) {
            String arg = DICTIONARY[new Random(System.currentTimeMillis()).nextInt(DICTIONARY.length)];

            generateFile(TOTAL_WORDS);
            Long genLen = igfs.info(FILE).length();

            IgniteBiTuple<Long, Integer> taskRes = igfs.execute(new Task(),
                new IgfsStringDelimiterRecordResolver(" "), Collections.singleton(FILE), arg);

            assert F.eq(genLen, taskRes.getKey());
            assert F.eq(TOTAL_WORDS, taskRes.getValue());
        }
    }

    /**
     * Test task.
     *
     * @throws Exception If failed.
     */
    public void testTaskAsync() throws Exception {
        U.sleep(3000);

        assertFalse(igfs.isAsync());

        IgniteFileSystem igfsAsync = igfs.withAsync();

        assertTrue(igfsAsync.isAsync());

        for (int i = 0; i < REPEAT_CNT; i++) {
            String arg = DICTIONARY[new Random(System.currentTimeMillis()).nextInt(DICTIONARY.length)];

            generateFile(TOTAL_WORDS);
            Long genLen = igfs.info(FILE).length();

            assertNull(igfsAsync.execute(
                new Task(), new IgfsStringDelimiterRecordResolver(" "), Collections.singleton(FILE), arg));

            IgniteFuture<IgniteBiTuple<Long, Integer>> fut = igfsAsync.future();

            assertNotNull(fut);

            IgniteBiTuple<Long, Integer> taskRes = fut.get();

            assert F.eq(genLen, taskRes.getKey());
            assert F.eq(TOTAL_WORDS, taskRes.getValue());
        }

        igfsAsync.format();

        IgniteFuture<?> fut = igfsAsync.future();

        assertNotNull(fut);

        fut.get();
    }

    /**
     * Generate file with random data and provided argument.
     *
     * @param wordCnt Word count.
     * @throws Exception If failed.
     */
    private void generateFile(int wordCnt)
        throws Exception {
        Random rnd = new Random(System.currentTimeMillis());

        try (OutputStreamWriter writer = new OutputStreamWriter(igfs.create(FILE, true))) {
            int cnt = 0;

            while (cnt < wordCnt) {
                String word = DICTIONARY[rnd.nextInt(DICTIONARY.length)];

                writer.write(word + " ");

                cnt++;
            }
        }
    }

    /**
     * Task.
     */
    private static class Task extends IgfsTask<String, IgniteBiTuple<Long, Integer>> {
        /** {@inheritDoc} */
        @Override public IgfsJob createJob(IgfsPath path, IgfsFileRange range,
            IgfsTaskArgs<String> args) {
            return new Job();
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<Long, Integer> reduce(List<ComputeJobResult> ress) {
            long totalLen = 0;
            int argCnt = 0;

            for (ComputeJobResult res : ress) {
                IgniteBiTuple<Long, Integer> res0 = (IgniteBiTuple<Long, Integer>)res.getData();

                if (res0 != null) {
                    totalLen += res0.getKey();
                    argCnt += res0.getValue();
                }
            }

            return F.t(totalLen, argCnt);
        }
    }

    /**
     * Job.
     */
    private static class Job implements IgfsJob, Serializable {
        @IgniteInstanceResource
        private Ignite ignite;

        @TaskSessionResource
        private ComputeTaskSession ses;

        @JobContextResource
        private ComputeJobContext ctx;

        /** {@inheritDoc} */
        @Override public Object execute(IgniteFileSystem igfs, IgfsFileRange range, IgfsInputStream in)
            throws IOException {
            assert ignite != null;
            assert ses != null;
            assert ctx != null;

            in.seek(range.start());

            byte[] buf = new byte[(int)range.length()];

            int totalRead = 0;

            while (totalRead < buf.length) {
                int b = in.read();

                assert b != -1;

                buf[totalRead++] = (byte)b;
            }

            String str = new String(buf);

            String[] chunks = str.split(" ");

            int ctr = 0;

            for (String chunk : chunks) {
                if (!chunk.isEmpty())
                    ctr++;
            }

            return F.t(range.length(), ctr);
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }
    }
}
