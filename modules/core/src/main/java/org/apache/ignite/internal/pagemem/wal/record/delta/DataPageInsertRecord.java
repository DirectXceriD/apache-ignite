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

package org.apache.ignite.internal.pagemem.wal.record.delta;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALReferenceAwareRecord;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Insert into data page.
 */
public class DataPageInsertRecord extends PageDeltaRecord implements WALReferenceAwareRecord {
    /** Actual fragment data size. */
    private final int payloadSize;

    /** WAL reference to {@link DataRecord}. */
    private final WALPointer reference;

    /** Actual fragment data. */
    private byte[] payload;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param payloadSize Remainder of the record.
     */
    public DataPageInsertRecord(
        int grpId,
        long pageId,
        int payloadSize,
        WALPointer reference
    ) {
        super(grpId, pageId);

        this.payloadSize = payloadSize;
        this.reference = reference;
    }

    /**
     * @return Insert record payload.
     */
    public byte[] payload() {
        return payload;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        assert payload != null;

        DataPageIO io = DataPageIO.VERSIONS.forPage(pageAddr);

        io.addRow(pageAddr, payload, pageMem.pageSize());
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_INSERT_RECORD;
    }

    /** {@inheritDoc} */
    @Override public int payloadSize() {
        return payloadSize;
    }

    /** {@inheritDoc} */
    @Override public int offset() {
        return -1;
    }

    /** {@inheritDoc} */
    @Override public void payload(byte[] payload) {
        this.payload = payload;
    }

    /** {@inheritDoc} */
    @Override public WALPointer reference() {
        return reference;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataPageInsertRecord.class, this,
                "payloadSize", payloadSize,
                "super", super.toString());
    }
}
