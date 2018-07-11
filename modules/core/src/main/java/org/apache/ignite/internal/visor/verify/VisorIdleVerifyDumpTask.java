/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.verify;

import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.processors.cache.verify.VerifyBackupPartitionsDumpTask;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.resources.JobContextResource;

/**
 * Task to verify checksums of backup partitions and return all collected information.
 */
@GridInternal
public class VisorIdleVerifyDumpTask extends VisorOneNodeTask<VisorIdleVerifyTaskArg, String> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorIdleVerifyTaskArg, String> job(VisorIdleVerifyTaskArg arg) {
        return new VisorIdleVerifyJob<>(arg, debug, VerifyBackupPartitionsDumpTask.class);
    }

    private static class VisorIdleVerifyJob<ResultT> extends VisorJob<VisorIdleVerifyTaskArg, ResultT> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private ComputeTaskFuture<ResultT> fut;

        /** Auto-inject job context. */
        @JobContextResource
        protected transient ComputeJobContext jobCtx;

        /** Task class for execution */
        private final Class<? extends ComputeTask<VisorIdleVerifyTaskArg, ResultT>> taskClass;

        /**
         * @param arg Argument.
         * @param debug Debug.
         * @param taskClass Task class for execution.
         */
        VisorIdleVerifyJob(VisorIdleVerifyTaskArg arg, boolean debug,
            Class<? extends ComputeTask<VisorIdleVerifyTaskArg, ResultT>> taskClass) {
            super(arg, debug);
            this.taskClass = taskClass;
        }

        /** {@inheritDoc} */
        @Override protected ResultT run(VisorIdleVerifyTaskArg arg) throws IgniteException {
            if (fut == null) {
                fut = ignite.compute().executeAsync(taskClass, arg);

                if (!fut.isDone()) {
                    jobCtx.holdcc();

                    fut.listen(new IgniteInClosure<IgniteFuture<ResultT>>() {
                        @Override public void apply(IgniteFuture<ResultT> f) {
                            jobCtx.callcc();
                        }
                    });

                    return null;
                }
            }

            return fut.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIdleVerifyJob.class, this);
        }
    }
}
