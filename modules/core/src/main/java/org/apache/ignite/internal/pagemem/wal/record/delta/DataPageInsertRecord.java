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
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.Storable;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.AbstractDataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Insert into data page.
 */
public class DataPageInsertRecord extends PageDeltaRecord implements WALReferenceAwareRecord {
    /** WAL reference to {@link DataRecord}. */
    private WALPointer reference;

    /** Actual fragment data. */
    private byte[] payload;

    /** Row associated with page data. */
    private Storable row;

    /**
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param reference WAL reference to {@link DataRecord}.
     */
    public DataPageInsertRecord(
        int grpId,
        long pageId,
        WALPointer reference
    ) {
        super(grpId, pageId);

        this.reference = reference;
    }

    /**
     * Old constructor for backward compatibility.
     *
     * @param grpId Cache group ID.
     * @param pageId Page ID.
     * @param payload Record payload.
     */
    public DataPageInsertRecord(
            int grpId,
            long pageId,
            byte[] payload
    ) {
        super(grpId, pageId);

        this.payload = payload;
    }

    /**
     * @return Insert record payload.
     */
    public byte[] payload() {
        return payload;
    }

    /** {@inheritDoc} */
    @Override public void applyDelta(PageMemory pageMem, long pageAddr) throws IgniteCheckedException {
        assert payload != null || row != null;

        AbstractDataPageIO<Storable> io = PageIO.getPageIO(pageAddr);

        if (payload != null) {
            io.addRow(pageAddr, payload, pageMem.pageSize());
        }
        else {
            io.addRow(pageId(), pageAddr, row, io.getRowSize(row), pageMem.pageSize());
        }
    }

    /** {@inheritDoc} */
    @Override public RecordType type() {
        return RecordType.DATA_PAGE_INSERT_RECORD;
    }

    /** {@inheritDoc} */
    @Override public void row(Storable row) {
        this.row = row;
    }

    /** {@inheritDoc} */
    @Override public WALPointer reference() {
        return reference;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataPageInsertRecord.class, this,
                "super", super.toString());
    }
}
