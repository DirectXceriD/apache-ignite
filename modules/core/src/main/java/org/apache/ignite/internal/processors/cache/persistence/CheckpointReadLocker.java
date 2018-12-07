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

package org.apache.ignite.internal.processors.cache.persistence;

/**
 * Checkpoint read locker instance. Used to associate with cache group.
 * A persistence cache group contains thee rerefence to the real database implementation to lock checkpoint.
 * A in-memory cache group contains {@link CheckpointReadLocker#NOOP} instance.
 */
public interface CheckpointReadLocker extends CheckpointLockStateChecker {
    /** No-op implementation of the checkpoint locker. */
    public CheckpointReadLocker NOOP = new CheckpointReadLocker() {
        @Override public boolean checkpointLockIsHeldByThread() {
            return true;
        }

        @Override public void checkpointReadLock() {
            // No-op.
        }

        @Override public void checkpointReadUnlock() {
            // No-op.
        }
    };

    /**
     * Gets the checkpoint read lock. While this lock is held, checkpoint thread will not acquireSnapshotWorker memory
     * state.
     */
    public void checkpointReadLock();

    /**
     * Releases the checkpoint read lock.
     */
    public void checkpointReadUnlock();
}