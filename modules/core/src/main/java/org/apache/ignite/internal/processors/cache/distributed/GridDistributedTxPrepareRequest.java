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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Transaction prepare request for optimistic and eventually consistent
 * transactions.
 */
public class GridDistributedTxPrepareRequest extends GridDistributedBaseMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** Thread ID. */
    @GridToStringInclude
    private long threadId;

    /** Transaction concurrency. */
    @GridToStringInclude
    private TransactionConcurrency concurrency;

    /** Transaction isolation. */
    @GridToStringInclude
    private TransactionIsolation isolation;

    /** Commit version for EC transactions. */
    @GridToStringInclude
    private GridCacheVersion writeVer;

    /** Transaction timeout. */
    @GridToStringInclude
    private long timeout;

    /** Invalidation flag. */
    @GridToStringInclude
    private boolean invalidate;

    /** Transaction read set. */
    @GridToStringInclude
    @GridDirectCollection(IgniteTxEntry.class)
    private Collection<IgniteTxEntry> reads;

    /** Transaction write entries. */
    @GridToStringInclude
    @GridDirectCollection(IgniteTxEntry.class)
    private Collection<IgniteTxEntry> writes;

    /** DHT versions to verify. */
    @GridToStringInclude
    @GridDirectTransient
    private Map<IgniteTxKey, GridCacheVersion> dhtVers;

    /** */
    @GridDirectCollection(IgniteTxKey.class)
    private Collection<IgniteTxKey> dhtVerKeys;

    /** */
    @GridDirectCollection(GridCacheVersion.class)
    private Collection<GridCacheVersion> dhtVerVals;

    /** Group lock key, if any. */
    @GridToStringInclude
    @GridDirectTransient
    private IgniteTxKey grpLockKey;

    /** Group lock key bytes. */
    @GridToStringExclude
    private byte[] grpLockKeyBytes;

    /** Partition lock flag. */
    private boolean partLock;

    /** Expected transaction size. */
    private int txSize;

    /** Transaction nodes mapping (primary node -> related backup nodes). */
    @GridDirectTransient
    private Map<UUID, Collection<UUID>> txNodes;

    /** */
    private byte[] txNodesBytes;

    /** One phase commit flag. */
    private boolean onePhaseCommit;

    /** System flag. */
    private boolean sys;

    /** IO policy. */
    private GridIoPolicy plc;

    /**
     * Required by {@link Externalizable}.
     */
    public GridDistributedTxPrepareRequest() {
        /* No-op. */
    }

    /**
     * @param tx Cache transaction.
     * @param reads Read entries.
     * @param writes Write entries.
     * @param grpLockKey Group lock key.
     * @param partLock {@code True} if preparing group-lock transaction with partition lock.
     * @param txNodes Transaction nodes mapping.
     * @param onePhaseCommit One phase commit flag.
     */
    public GridDistributedTxPrepareRequest(
        IgniteInternalTx tx,
        @Nullable Collection<IgniteTxEntry> reads,
        Collection<IgniteTxEntry> writes,
        IgniteTxKey grpLockKey,
        boolean partLock,
        Map<UUID, Collection<UUID>> txNodes,
        boolean onePhaseCommit
    ) {
        super(tx.xidVersion(), 0);

        writeVer = tx.writeVersion();
        threadId = tx.threadId();
        concurrency = tx.concurrency();
        isolation = tx.isolation();
        timeout = tx.timeout();
        invalidate = tx.isInvalidate();
        txSize = tx.size();
        sys = tx.system();
        plc = tx.ioPolicy();

        this.reads = reads;
        this.writes = writes;
        this.grpLockKey = grpLockKey;
        this.partLock = partLock;
        this.txNodes = txNodes;
        this.onePhaseCommit = onePhaseCommit;
    }

    /**
     * @return Transaction nodes mapping.
     */
    public Map<UUID, Collection<UUID>> transactionNodes() {
        return txNodes;
    }

    /**
     * @return System flag.
     */
    public boolean system() {
        return sys;
    }

    /**
     * @return IO policy.
     */
    public GridIoPolicy policy() {
        return plc;
    }

    /**
     * Adds version to be verified on remote node.
     *
     * @param key Key for which version is verified.
     * @param dhtVer DHT version to check.
     */
    public void addDhtVersion(IgniteTxKey key, @Nullable GridCacheVersion dhtVer) {
        if (dhtVers == null)
            dhtVers = new HashMap<>();

        dhtVers.put(key, dhtVer);
    }

    /**
     * @return Map of versions to be verified.
     */
    public Map<IgniteTxKey, GridCacheVersion> dhtVersions() {
        return dhtVers == null ? Collections.<IgniteTxKey, GridCacheVersion>emptyMap() : dhtVers;
    }

    /**
     * @return Thread ID.
     */
    public long threadId() {
        return threadId;
    }

    /**
     * @return Commit version.
     */
    public GridCacheVersion writeVersion() { return writeVer; }

    /**
     * @return Invalidate flag.
     */
    public boolean isInvalidate() { return invalidate; }

    /**
     * @return Transaction timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * @return Concurrency.
     */
    public TransactionConcurrency concurrency() {
        return concurrency;
    }

    /**
     * @return Isolation level.
     */
    public TransactionIsolation isolation() {
        return isolation;
    }

    /**
     * @return Read set.
     */
    public Collection<IgniteTxEntry> reads() {
        return reads;
    }

    /**
     * @return Write entries.
     */
    public Collection<IgniteTxEntry> writes() {
        return writes;
    }

    /**
     * @param reads Reads.
     */
    protected void reads(Collection<IgniteTxEntry> reads) {
        this.reads = reads;
    }

    /**
     * @param writes Writes.
     */
    protected void writes(Collection<IgniteTxEntry> writes) {
        this.writes = writes;
    }

    /**
     * @return Group lock key if preparing group-lock transaction.
     */
    @Nullable public IgniteTxKey groupLockKey() {
        return grpLockKey;
    }

    /**
     * @return {@code True} if preparing group-lock transaction with partition lock.
     */
    public boolean partitionLock() {
        return partLock;
    }

    /**
     * @return Expected transaction size.
     */
    public int txSize() {
        return txSize;
    }

    /**
     * @return One phase commit flag.
     */
    public boolean onePhaseCommit() {
        return onePhaseCommit;
    }

    /** {@inheritDoc}
     * @param ctx*/
    @Override public void prepareMarshal(GridCacheSharedContext ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (writes != null)
            marshalTx(writes, ctx);

        if (reads != null)
            marshalTx(reads, ctx);

        if (grpLockKey != null && grpLockKeyBytes == null)
            grpLockKeyBytes = ctx.marshaller().marshal(grpLockKey);

        if (dhtVers != null) {
            for (IgniteTxKey key : dhtVers.keySet()) {
                GridCacheContext cctx = ctx.cacheContext(key.cacheId());

                key.prepareMarshal(cctx);
            }

            dhtVerKeys = dhtVers.keySet();
            dhtVerVals = dhtVers.values();
        }

        if (txNodes != null)
            txNodesBytes = ctx.marshaller().marshal(txNodes);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext ctx, ClassLoader ldr) throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (writes != null)
            unmarshalTx(writes, false, ctx, ldr);

        if (reads != null)
            unmarshalTx(reads, false, ctx, ldr);

        if (grpLockKeyBytes != null && grpLockKey == null)
            grpLockKey = ctx.marshaller().unmarshal(grpLockKeyBytes, ldr);

        if (dhtVerKeys != null && dhtVers == null) {
            assert dhtVerVals != null;
            assert dhtVerKeys.size() == dhtVerVals.size();

            Iterator<IgniteTxKey> keyIt = dhtVerKeys.iterator();
            Iterator<GridCacheVersion> verIt = dhtVerVals.iterator();

            dhtVers = U.newHashMap(dhtVerKeys.size());

            while (keyIt.hasNext()) {
                IgniteTxKey key = keyIt.next();

                key.finishUnmarshal(ctx.cacheContext(key.cacheId()), ldr);

                dhtVers.put(key, verIt.next());
            }
        }

        if (txNodesBytes != null)
            txNodes = ctx.marshaller().unmarshal(txNodesBytes, ldr);
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!super.writeTo(buf, writer))
            return false;

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 8:
                if (!writer.writeByte("concurrency", concurrency != null ? (byte)concurrency.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 9:
                if (!writer.writeCollection("dhtVerKeys", dhtVerKeys, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 10:
                if (!writer.writeCollection("dhtVerVals", dhtVerVals, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 11:
                if (!writer.writeByteArray("grpLockKeyBytes", grpLockKeyBytes))
                    return false;

                writer.incrementState();

            case 12:
                if (!writer.writeBoolean("invalidate", invalidate))
                    return false;

                writer.incrementState();

            case 13:
                if (!writer.writeByte("isolation", isolation != null ? (byte)isolation.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 14:
                if (!writer.writeBoolean("onePhaseCommit", onePhaseCommit))
                    return false;

                writer.incrementState();

            case 15:
                if (!writer.writeBoolean("partLock", partLock))
                    return false;

                writer.incrementState();

            case 16:
                if (!writer.writeByte("plc", plc != null ? (byte)plc.ordinal() : -1))
                    return false;

                writer.incrementState();

            case 17:
                if (!writer.writeCollection("reads", reads, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 18:
                if (!writer.writeBoolean("sys", sys))
                    return false;

                writer.incrementState();

            case 19:
                if (!writer.writeLong("threadId", threadId))
                    return false;

                writer.incrementState();

            case 20:
                if (!writer.writeLong("timeout", timeout))
                    return false;

                writer.incrementState();

            case 21:
                if (!writer.writeByteArray("txNodesBytes", txNodesBytes))
                    return false;

                writer.incrementState();

            case 22:
                if (!writer.writeInt("txSize", txSize))
                    return false;

                writer.incrementState();

            case 23:
                if (!writer.writeMessage("writeVer", writeVer))
                    return false;

                writer.incrementState();

            case 24:
                if (!writer.writeCollection("writes", writes, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        if (!super.readFrom(buf, reader))
            return false;

        switch (reader.state()) {
            case 8:
                byte concurrencyOrd;

                concurrencyOrd = reader.readByte("concurrency");

                if (!reader.isLastRead())
                    return false;

                concurrency = TransactionConcurrency.fromOrdinal(concurrencyOrd);

                reader.incrementState();

            case 9:
                dhtVerKeys = reader.readCollection("dhtVerKeys", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 10:
                dhtVerVals = reader.readCollection("dhtVerVals", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 11:
                grpLockKeyBytes = reader.readByteArray("grpLockKeyBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 12:
                invalidate = reader.readBoolean("invalidate");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 13:
                byte isolationOrd;

                isolationOrd = reader.readByte("isolation");

                if (!reader.isLastRead())
                    return false;

                isolation = TransactionIsolation.fromOrdinal(isolationOrd);

                reader.incrementState();

            case 14:
                onePhaseCommit = reader.readBoolean("onePhaseCommit");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 15:
                partLock = reader.readBoolean("partLock");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 16:
                byte plcOrd;

                plcOrd = reader.readByte("plc");

                if (!reader.isLastRead())
                    return false;

                plc = GridIoPolicy.fromOrdinal(plcOrd);

                reader.incrementState();

            case 17:
                reads = reader.readCollection("reads", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 18:
                sys = reader.readBoolean("sys");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 19:
                threadId = reader.readLong("threadId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 20:
                timeout = reader.readLong("timeout");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 21:
                txNodesBytes = reader.readByteArray("txNodesBytes");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 22:
                txSize = reader.readInt("txSize");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 23:
                writeVer = reader.readMessage("writeVer");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 24:
                writes = reader.readCollection("writes", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 25;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 25;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return GridToStringBuilder.toString(GridDistributedTxPrepareRequest.class, this,
            "super", super.toString());
    }
}
