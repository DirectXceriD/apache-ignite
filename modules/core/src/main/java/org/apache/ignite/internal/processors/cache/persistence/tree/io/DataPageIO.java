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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.GridStringBuilder;

/**
 * Data pages IO.
 */
public class DataPageIO extends AbstractDataPageIO<CacheDataRow> {
    /** */
    public static final IOVersions<DataPageIO> VERSIONS = new IOVersions<>(
        new DataPageIO(1)
    );

    /**
     * @param ver Page format version.
     */
    protected DataPageIO(int ver) {
        super(T_DATA, ver);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeFragmentData(
        final CacheDataRow row,
        final ByteBuffer buf,
        final int rowOff,
        final int payloadSize
    ) throws IgniteCheckedException {
        DataPageIOUtils.writeFragmentData(row, buf, rowOff, payloadSize, false);
    }

    /** {@inheritDoc} */
    @Override
    protected void writeRowData(
        long pageAddr,
        int dataOff,
        int payloadSize,
        CacheDataRow row,
        boolean newRow
    ) throws IgniteCheckedException {
        DataPageIOUtils.writeRowData(pageAddr, dataOff, payloadSize, row, newRow, false);
    }

    /** {@inheritDoc} */
    @Override public int getRowSize(CacheDataRow row) throws IgniteCheckedException {
        return getRowSize(row, row.cacheId() != 0);
    }

    /** {@inheritDoc} */
    @Override protected void printPage(long addr, int pageSize, GridStringBuilder sb) throws IgniteCheckedException {
        sb.a("DataPageIO [\n");
        printPageLayout(addr, pageSize, sb);
        sb.a("\n]");
    }

    /**
     * @param row Row.
     * @param withCacheId If {@code true} adds cache ID size.
     * @return Entry size on page.
     * @throws IgniteCheckedException If failed.
     */
    public static int getRowSize(CacheDataRow row, boolean withCacheId) throws IgniteCheckedException {
        int len = row.key().valueBytesLength(null);

        if (!row.removed())
            len += row.value().valueBytesLength(null) + CacheVersionIO.size(row.version(), false) + 8;

        return len + (withCacheId ? 4 : 0);
    }
}
