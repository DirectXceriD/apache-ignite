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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

/**
 * Switch statisticsEnabled flag for specified caches to specified state.
 */
@GridInternal
public class VisorCacheToggleStatisticsTask extends VisorOneNodeTask<VisorCacheToggleStatisticsTaskArg, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorCachesToggleStatisticsJob job(VisorCacheToggleStatisticsTaskArg arg) {
        return new VisorCachesToggleStatisticsJob(arg, debug);
    }

    /**
     * Job that rebalance caches.
     */
    private static class VisorCachesToggleStatisticsJob extends VisorJob<VisorCacheToggleStatisticsTaskArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Caches names.
         * @param debug Debug flag.
         */
        private VisorCachesToggleStatisticsJob(VisorCacheToggleStatisticsTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorCacheToggleStatisticsTaskArg arg) {
            for(String cacheName: arg.getCacheNames())
                ignite.cache(cacheName).enableStatistics(arg.getState());

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorCachesToggleStatisticsJob.class, this);
        }
    }
}