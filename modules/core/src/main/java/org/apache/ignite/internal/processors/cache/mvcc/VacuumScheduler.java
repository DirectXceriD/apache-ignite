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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;

/**
 * Mvcc garbage collection scheduler.
 */
public class VacuumScheduler extends GridWorker {
    /** */
    private final GridKernalContext ctx;

    /** */
    private final long interval;

    /**
     * @param ctx Kernal context.
     * @param log Logger.
     */
    VacuumScheduler(GridKernalContext ctx, IgniteLogger log) {
        super(ctx.igniteInstanceName(), "vacuum-scheduler", log);

        this.ctx = ctx;
        this.interval = ctx.config().getMvccVacuumTimeInterval();
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
        try {
            U.sleep(interval); // initial delay

            while (!isCancelled()) {
                long nextScheduledTime = U.currentTimeMillis() + interval;

                if (log.isDebugEnabled())
                    log.debug("Vacuum started by scheduler.");

                ctx.coordinators().runVacuum().get();

                long delay = nextScheduledTime - U.currentTimeMillis();

                if (delay > 0)
                    U.sleep(delay);
            }
        }
        catch (IgniteInterruptedCheckedException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Error occurred during scheduled vacuum process.", e);

            ctx.coordinators().setVacuumError(e);
        }
    }
}