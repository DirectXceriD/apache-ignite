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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.testframework.MvccFeatureChecker;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;

/**
 *
 */
public class IgniteCacheInvokeReadThroughSingleNodeTest extends IgniteCacheInvokeReadThroughAbstractTest {
    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        MvccFeatureChecker.failIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        super.setUp();
    }

    /** {@inheritDoc} */
    @Override protected void startNodes() throws Exception {
        startGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughAtomic() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, ATOMIC, 1, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughAtomicNearCache() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, ATOMIC, 1, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughAtomicReplicated() throws Exception {
        invokeReadThrough(cacheConfiguration(REPLICATED, ATOMIC, 0, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughAtomicLocal() throws Exception {
        invokeReadThrough(cacheConfiguration(LOCAL, ATOMIC, 0, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughTx() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 1, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughTxNearCache() throws Exception {
        invokeReadThrough(cacheConfiguration(PARTITIONED, TRANSACTIONAL, 1, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughTxReplicated() throws Exception {
        invokeReadThrough(cacheConfiguration(REPLICATED, TRANSACTIONAL, 0, false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvokeReadThroughTxLocal() throws Exception {
        invokeReadThrough(cacheConfiguration(LOCAL, TRANSACTIONAL, 0, false));
    }
}
