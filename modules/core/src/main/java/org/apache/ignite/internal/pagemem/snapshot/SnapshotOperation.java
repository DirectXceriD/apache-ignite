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
 *
 */
package org.apache.ignite.internal.pagemem.snapshot;

import java.io.Serializable;
import java.util.Set;

/**
 * Description and parameters of snapshot operation
 */
public class SnapshotOperation implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private final SnapshotOperationType type;

    /**
     * Snapshot ID (the timestamp of snapshot creation).
     */
    private final long snapshotId;

    /** */
    private final Set<String> cacheNames;

    /** Message. */
    private final String msg;

    /** Additional parameter. */
    private final Object extraParam;

    /**
     * @param type Type.
     * @param snapshotId Snapshot id.
     * @param cacheNames Cache names.
     * @param msg
     * @param extraParam Additional parameter.
     */
    public SnapshotOperation(SnapshotOperationType type, long snapshotId, Set<String> cacheNames, String msg, Object extraParam) {
        this.type = type;
        this.snapshotId = snapshotId;
        this.cacheNames = cacheNames;
        this.msg = msg;
        this.extraParam = extraParam;
    }

    /**
     *
     */
    public SnapshotOperationType type() {
        return type;
    }

    /**
     *
     */
    public long snapshotId() {
        return snapshotId;
    }

    /**
     *
     */
    public Set<String> cacheNames() {
        return cacheNames;
    }

    /**
     *
     */
    public String message() {
        return msg;
    }

    /**
     *
     */
    public Object extraParameter() {
        return extraParam;
    }
}
