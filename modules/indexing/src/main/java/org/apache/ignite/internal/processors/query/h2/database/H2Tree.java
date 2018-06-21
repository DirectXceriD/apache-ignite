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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.query.h2.H2RowCache;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasInnerIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2ExtrasLeafIO;
import org.apache.ignite.internal.processors.query.h2.database.io.H2RowLinkIO;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2KeyValueRowOnheap;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Row;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.indexing.IndexingQueryCacheFilter;
import org.h2.result.SearchRow;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.jetbrains.annotations.Nullable;

/**
 */
public abstract class H2Tree extends BPlusTree<SearchRow, GridH2Row> {
    /** Whether this is unique index. */
    private final boolean unique;

    /** */
    private final H2RowFactory rowStore;

    /** */
    private final int inlineSize;

    /** */
    private final List<InlineIndexHelper> inlineIdxs;

    public final static AtomicInteger cnt = new AtomicInteger();

    public static volatile H2Tree instance;

    /** */
    private final IndexColumn[] cols;

    /** */
    private final int[] columnIds;

    /** */
    private final Comparator<Value> comp = new Comparator<Value>() {
        @Override public int compare(Value o1, Value o2) {
            return compareValues(o1, o2);
        }
    };

    /** Row cache. */
    private final H2RowCache rowCache;

    /** */
    private volatile boolean linksBasedComparison = true;

    /**
     * Constructor.
     *
     * @param name Tree name.
     * @param unique Whether this is unique index.
     * @param reuseList Reuse list.
     * @param grpId Cache group ID.
     * @param pageMem Page memory.
     * @param wal Write ahead log manager.
     * @param rowStore Row data store.
     * @param metaPageId Meta page ID.
     * @param initNew Initialize new index.
     * @param rowCache Row cache.
     * @param log Logger.
     * @throws IgniteCheckedException If failed.
     */
    protected H2Tree(
        String name,
        boolean unique,
        ReuseList reuseList,
        int grpId,
        PageMemory pageMem,
        IgniteWriteAheadLogManager wal,
        AtomicLong globalRmvId,
        H2RowFactory rowStore,
        long metaPageId,
        boolean initNew,
        IndexColumn[] cols,
        List<InlineIndexHelper> inlineIdxs,
        int inlineSize,
        @Nullable H2RowCache rowCache,
        IgniteLogger log) throws IgniteCheckedException {
        super(name, grpId, pageMem, wal, globalRmvId, metaPageId, reuseList);

        this.unique = unique;

        if (!initNew) {
            // Page is ready - read inline size from it.
            inlineSize = getMetaInlineSize();
        }

        this.inlineSize = inlineSize;

        assert rowStore != null;

        this.rowStore = rowStore;
        this.inlineIdxs = inlineIdxs;
        this.cols = cols;

        this.columnIds = new int[cols.length];

        for (int i = 0; i < cols.length; i++)
            columnIds[i] = cols[i].column.getColumnId();

        this.rowCache = rowCache;

        setIos(H2ExtrasInnerIO.getVersions(inlineSize), H2ExtrasLeafIO.getVersions(inlineSize));

        initTree(initNew, inlineSize);

        if (!getName().toLowerCase().contains("pk"))
            instance = this;

        if (!linksBasedComparison)
            U.warn(log, "Grid has been restored from persistent storage created by older version, falling back " +
                "to indexes containing redundant key data.\nIn order to gain performance increase," +
                "please rebuild your index from scratch - to do so, please remove index.bin files from cache group " +
                "directories that reside in your PDS directory.");
    }

    /** {@inheritDoc} */
    @Override protected void initTree(boolean initNew, int inlineSize) throws IgniteCheckedException {
        if (initNew)
            super.initTree(true, inlineSize);
        else {
            final long metaPage = acquirePage(metaPageId);

            try {
                long pageAddr = readLock(metaPageId, metaPage);

                assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" + U.hexLong(metaPageId) + ']';

                try {
                    linksBasedComparison = BPlusMetaIO.VERSIONS.forPage(pageAddr).useLinksComparison();
                }
                finally {
                    readUnlock(metaPageId, metaPage, pageAddr);
                }
            }
            finally {
                releasePage(metaPageId, metaPage);
            }
        }
    }

    /**
     * Create row from link.
     *
     * @param link Link.
     * @return Row.
     * @throws IgniteCheckedException if failed.
     */
    public GridH2Row createRowFromLink(long link) throws IgniteCheckedException {
        if (rowCache != null) {
            GridH2Row row = rowCache.get(link);

            if (row == null) {
                row = rowStore.getRow(link);

                if (row instanceof GridH2KeyValueRowOnheap)
                    rowCache.put((GridH2KeyValueRowOnheap)row);
            }

            return row;
        }
        else
            return rowStore.getRow(link);
    }

    /** {@inheritDoc} */
    @Override protected GridH2Row getRow(BPlusIO<SearchRow> io, long pageAddr, int idx, Object filter)
        throws IgniteCheckedException {
        if (filter != null) {
            // Filter out not interesting partitions without deserializing the row.
            IndexingQueryCacheFilter filter0 = (IndexingQueryCacheFilter)filter;

            long link = ((H2RowLinkIO)io).getLink(pageAddr, idx);

            int part = PageIdUtils.partId(PageIdUtils.pageId(link));

            if (!filter0.applyPartition(part))
                return null;
        }

        return (GridH2Row)io.getLookupRow(this, pageAddr, idx);
    }

    /**
     * @return Inline size.
     */
    private int inlineSize() {
        return inlineSize;
    }

    /**
     * @return Inline size.
     * @throws IgniteCheckedException If failed.
     */
    private int getMetaInlineSize() throws IgniteCheckedException {
        final long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr = readLock(metaPageId, metaPage); // Meta can't be removed.

            assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" +
                U.hexLong(metaPageId) + ']';

            try {
                BPlusMetaIO io = BPlusMetaIO.VERSIONS.forPage(pageAddr);

                return io.getInlineSize(pageAddr);
            }
            finally {
                readUnlock(metaPageId, metaPage, pageAddr);
            }
        }
        finally {
            releasePage(metaPageId, metaPage);
        }
    }

    /**
     * Puts new row using old row to determine strict row equality.
     * Is meant to facilitate correct and timely secondary index update in absence of cache key present in every index.
     *
     * @param row Row.
     * @param oldRow Previous row yielded by primary key update or {@code null} if cache key was not previously present.
     * @return {@code True} if existing row row has been replaced.
     * @throws IgniteCheckedException if failed.
     */
    public boolean replace(GridH2Row row, @Nullable GridH2Row oldRow) throws IgniteCheckedException {
        Boolean res = (Boolean)doPut(row, oldRow, false);

        return res != null ? res : false;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ForLoopReplaceableByForEach")
    @Override protected int compare(BPlusIO<SearchRow> io, long pageAddr, int idx,
        SearchRow row) throws IgniteCheckedException {

        if (!getName().toLowerCase().contains("pk")) {
            cnt.incrementAndGet();

            GridH2Row r2 = getRow(io, pageAddr, idx);

            System.out.println("Comparison: " + row.getValue(0).getInt() +
                " vs " + r2.getValue(0).getInt() + " (" + ((H2RowLinkIO)io).getLink(pageAddr, idx) + ")");

            print();
        }

        if (inlineSize() == 0) {
            int rowsCmpRes = compareRows(getRow(io, pageAddr, idx), row);

            if (rowsCmpRes != 0)
                return rowsCmpRes;
        }
        else {
            int off = io.offset(idx);

            int fieldOff = 0;

            int lastIdxUsed = 0;

            for (int i = 0; i < inlineIdxs.size(); i++) {
                InlineIndexHelper inlineIdx = inlineIdxs.get(i);

                Value v2 = row.getValue(inlineIdx.columnIndex());

                if (v2 == null)
                    return 0;

                int c = inlineIdx.compare(pageAddr, off + fieldOff, inlineSize() - fieldOff, v2, comp);

                if (c == -2)
                    break;

                lastIdxUsed++;

                if (c != 0)
                    return c;

                fieldOff += inlineIdx.fullSize(pageAddr, off + fieldOff);

                if (fieldOff > inlineSize())
                    break;
            }

            if (lastIdxUsed < cols.length) {
                SearchRow rowData = getRow(io, pageAddr, idx);

                for (int i = lastIdxUsed, len = cols.length; i < len; i++) {
                    IndexColumn col = cols[i];
                    int idx0 = col.column.getColumnId();

                    Value v2 = row.getValue(idx0);

                    if (v2 == null) {
                        // Can't compare further.
                        return 0;
                    }

                    Value v1 = rowData.getValue(idx0);

                    int c = compareValues(v1, v2);

                    if (c != 0)
                        return InlineIndexHelper.fixSort(c, col.sortType);
                }
            }
        }

        // Old mode, everything is compared, stop.
        if (!linksBasedComparison)
            return 0;

        // Compared against H2 row template, stop.
        if (!(row instanceof CacheSearchRow))
            return 0;

        // This is an attempt to insert the same value into unique value; return 0 to force replacement.
        if (unique)
            return 0;

        long link1 = ((H2RowLinkIO)io).getLink(pageAddr, idx);
        long link2 = ((CacheSearchRow)row).link();

        return Long.compare(link1, link2);
    }

    /**
     * Compares two H2 rows.
     *
     * @param r1 Row 1.
     * @param r2 Row 2.
     * @return Compare result: see {@link Comparator#compare(Object, Object)} for values.
     */
    public int compareRows(SearchRow r1, SearchRow r2) {
        if (r1 == r2)
            return 0;

        for (int i = 0, len = cols.length; i < len; i++) {
            int idx = columnIds[i];

            Value v1 = r1.getValue(idx);
            Value v2 = r2.getValue(idx);

            if (v1 == null || v2 == null) {
                // Can't compare further.
                return 0;
            }

            int c = compareValues(v1, v2);

            if (c != 0)
                return InlineIndexHelper.fixSort(c, cols[i].sortType);
        }

        return 0;
    }

    private static void print() {
        StackTraceElement[] elements = Thread.currentThread().getStackTrace();
        for (int i = 2; i < Math.min(30, elements.length); i++) {
            StackTraceElement s = elements[i];
            System.out.println("\tat " + s.getClassName() + "." + s.getMethodName()
                + "(" + s.getFileName() + ":" + s.getLineNumber() + ")");
        }
    }

    /**
     * @param v1 First value.
     * @param v2 Second value.
     * @return Comparison result.
     */
    public abstract int compareValues(Value v1, Value v2);
}
