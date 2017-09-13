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

package org.apache.ignite.internal.processors.platform.client.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;

/**
 * Cache get request.
 */
class ClientCacheRequest extends ClientRequest {
    /** Flag: keep binary. */
    private static final byte FLAG_KEEP_BINARY = 1;

    /** Cache ID. */
    private final int cacheId;

    /** Flags. */
    private final byte flags;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    ClientCacheRequest(BinaryRawReader reader) {
        super(reader);

        cacheId = reader.readInt();

        flags = reader.readByte();
    }

    /**
     * Gets the cache for current cache id, with binary mode enabled.
     *
     * @param ctx Kernal context.
     * @return Cache.
     */
    protected IgniteCache cache(ClientConnectionContext ctx) {
        return rawCache(ctx).withKeepBinary();
    }

    /**
     * Gets the cache for current cache id, with binary mode enabled if FLAG_KEEP_BINARY is set.
     *
     * @param ctx Kernal context.
     * @return Cache.
     */
    public IgniteCache cacheWithBinaryFlag(ClientConnectionContext ctx) {
        IgniteCache cache = rawCache(ctx);

        if ((flags & FLAG_KEEP_BINARY) == FLAG_KEEP_BINARY)
            cache = cache.withKeepBinary();

        return cache;
    }

    /**
     * Gets the cache for current cache id, ignoring any flags.
     *
     * @param ctx Kernal context.
     * @return Cache.
     */
    private IgniteCache rawCache(ClientConnectionContext ctx) {
        String cacheName = ctx.kernalContext().cache().context().cacheContext(cacheId).cache().name();

        return ctx.kernalContext().grid().cache(cacheName);
    }
}
