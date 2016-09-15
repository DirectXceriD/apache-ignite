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

package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlStatement;
import org.apache.ignite.internal.util.lang.GridTriple;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Two step map-reduce style query.
 */
public class GridCacheTwoStepQuery {
    /** */
    public static final int DFLT_PAGE_SIZE = 1000;

    /** */
    @GridToStringInclude
    private List<GridCacheSqlQuery> mapQrys = new ArrayList<>();

    /** */
    @GridToStringInclude
    private GridCacheSqlQuery rdc;

    /** */
    private int pageSize = DFLT_PAGE_SIZE;

    /** */
    private boolean explain;

    /** */
    private Collection<String> spaces;

    /** */
    private Set<String> schemas;

    /** */
    private Set<String> tbls;

    /** */
    private boolean distributedJoins;

    /** */
    private boolean skipMergeTbl;

    /** Whether this query should avoid throwing an exception on duplicate key during INSERT. */
    private boolean skipDuplicateKeys;

    /** */
    private GridSqlStatement initStmt;

    /** */
    private List<Integer> caches;

    /** */
    private List<Integer> extraCaches;

    /**
     * Triple [key; value; new value] this DELETE or UPDATE query is supposed to affect.<p>
     * <b>new value</b> is present only for UPDATE.<p>
     * <b>value</b> in this triple may be null. If this field is null then no matching filter found.
     */
    @Nullable
    @GridToStringInclude
    private GridTriple<GridSqlElement> singleUpdate;

    /**
     * @param schemas Schema names in query.
     * @param tbls Tables in query.
     */
    public GridCacheTwoStepQuery(Set<String> schemas, Set<String> tbls) {
        this.schemas = schemas;
        this.tbls = tbls;
    }

    /**
     * Specify if distributed joins are enabled for this query.
     *
     * @param distributedJoins Distributed joins enabled.
     */
    public void distributedJoins(boolean distributedJoins) {
        this.distributedJoins = distributedJoins;
    }

    /**
     * Check if distributed joins are enabled for this query.
     *
     * @return {@code true} If distributed joind enabled.
     */
    public boolean distributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Initial statement.
     */
    public GridSqlStatement initialStatement() {
        return initStmt;
    }

    /**
     * @param initStmt Initial statement.
     */
    public void initialStatement(GridSqlStatement initStmt) {
        this.initStmt = initStmt;
    }

    /**
     * @return {@code True} if reduce query can skip merge table creation and get data directly from merge index.
     */
    public boolean skipMergeTable() {
        return skipMergeTbl;
    }

    /**
     * @param skipMergeTbl Skip merge table.
     */
    public void skipMergeTable(boolean skipMergeTbl) {
        this.skipMergeTbl = skipMergeTbl;
    }

    /**
     * @return If this is explain query.
     */
    public boolean explain() {
        return explain;
    }

    /**
     * @param explain If this is explain query.
     */
    public void explain(boolean explain) {
        this.explain = explain;
    }

    /**
     * @param pageSize Page size.
     */
    public void pageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * @return Page size.
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * @param qry SQL Query.
     * @return {@code this}.
     */
    public GridCacheTwoStepQuery addMapQuery(GridCacheSqlQuery qry) {
        mapQrys.add(qry);

        return this;
    }

    /**
     * @return Reduce query.
     */
    public GridCacheSqlQuery reduceQuery() {
        return rdc;
    }

    /**
     * @param rdc Reduce query.
     */
    public void reduceQuery(GridCacheSqlQuery rdc) {
        this.rdc = rdc;
    }

    /**
     * @return Map queries.
     */
    public List<GridCacheSqlQuery> mapQueries() {
        return mapQrys;
    }

    /**
     * @return Caches.
     */
    public List<Integer> caches() {
        return caches;
    }

    /**
     * @param caches Caches.
     */
    public void caches(List<Integer> caches) {
        this.caches = caches;
    }

    /**
     * @return Caches.
     */
    public List<Integer> extraCaches() {
        return extraCaches;
    }

    /**
     * @param extraCaches Caches.
     */
    public void extraCaches(List<Integer> extraCaches) {
        this.extraCaches = extraCaches;
    }

    /**
     * @return Spaces.
     */
    public Collection<String> spaces() {
        return spaces;
    }

    /**
     * @param spaces Spaces.
     */
    public void spaces(Collection<String> spaces) {
        this.spaces = spaces;
    }

    /**
     * @return Single item update arguments that given DELETE or UPDATE boils down to, null if it does not
     * or if the given statement is of different type.
     */
    @Nullable public GridTriple<GridSqlElement> singleUpdate() {
        return singleUpdate;
    }

    /**
     * @param singleUpdate Single item update arguments that given DELETE or UPDATE boils down to, null if it does not
     * or if the given statement is of different type.
     */
    public void singleUpdate(@Nullable GridTriple<GridSqlElement> singleUpdate) {
        this.singleUpdate = singleUpdate;
    }

    /**
     * @return Schemas.
     */
    public Set<String> schemas() {
        return schemas;
    }

    /**
     * @return {@code true} if this query should not yield an exception if there was a duplicate key during INSERT.
     */
    public boolean skipDuplicateKeys() {
        return skipDuplicateKeys;
    }

    /**
     * @param skipDuplicateKeys whether this query should not yield an exception on a duplicate key during INSERT.
     */
    public void skipDuplicateKeys(boolean skipDuplicateKeys) {
        this.skipDuplicateKeys = skipDuplicateKeys;
    }

    /**
     * @param args New arguments to copy with.
     * @return Copy.
     */
    public GridCacheTwoStepQuery copy(Object[] args) {
        assert !explain;

        GridCacheTwoStepQuery cp = new GridCacheTwoStepQuery(schemas, tbls);

        cp.caches = caches;
        cp.extraCaches = extraCaches;
        cp.spaces = spaces;
        cp.rdc = rdc != null ? rdc.copy(args) : null;
        cp.skipMergeTbl = skipMergeTbl;
        cp.pageSize = pageSize;
        cp.distributedJoins = distributedJoins;
        cp.initStmt = initStmt;
        cp.singleUpdate = singleUpdate;
        cp.skipDuplicateKeys = skipDuplicateKeys;

        for (int i = 0; i < mapQrys.size(); i++)
            cp.mapQrys.add(mapQrys.get(i).copy(args));

        return cp;
    }

    /**
     * @return Tables.
     */
    public Set<String> tables() {
        return tbls;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTwoStepQuery.class, this);
    }
}