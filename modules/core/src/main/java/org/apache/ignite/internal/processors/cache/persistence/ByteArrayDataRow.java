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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.tree.DataRow;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;

/**
 * Represents byte array in data pages.
 *
 * TODO FIXME how to dispose of not needed key?
 * Probably needs refactoring of CacheFreeListImpl.
 * Or create dedicated FreeList for system pages (other candidate is PagePartitionCountersIO) ?
 */
public class ByteArrayDataRow extends DataRow {
    public ByteArrayDataRow(CacheGroupContext grp, long link, int part) {
        super(grp, 0, link, part, RowData.NO_KEY, true);
    }

    public ByteArrayDataRow(CacheObjectContext ctx, int part, int grpId, byte[] data) throws IgniteCheckedException {
        super(new KeyCacheObjectImpl((byte)0, ctx.kernalContext().cacheObjects().marshal(ctx, (byte)0), part), new CacheObjectByteArrayImpl(data),
            GridCacheVersionManager.EVICT_VER, part, 0, grpId);
    }
}
