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

package org.apache.ignite.internal.processors.query.h2;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for join partition pruning.
 */
@SuppressWarnings("deprecation")
@RunWith(JUnit4.class)
public class SelectExtractColumnsForSimpleRowSelfTest extends AbstractIndexingCommonTest {
    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder().setShared(true);

    /** Client node name. */
    private static final String CLI_NAME = "cli";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(getConfiguration("srv1"));
        startGrid(getConfiguration("srv2"));

        startGrid(getConfiguration(CLI_NAME).setClientMode(true));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        Ignite cli = client();

        cli.destroyCaches(cli.cacheNames());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER))
            .setLocalHost("127.0.0.1");
    }

    /**
     * Test single table.
     */
    @Test
    public void testSingleTable() {
        List<List<?>> res;

        sql("CREATE TABLE test (id LONG PRIMARY KEY, valStr VARCHAR, valLong LONG)");

        for (int i = 0; i < 100; ++i)
            sql("INSERT INTO test VALUES (?, ?, ?)", i, "val_" + i, i);

        // scan
        res = sql("SELECT * from test");

        assertEquals(100L, res.size());

        // hidden fields
        res = sql("SELECT valStr from test WHERE _key = 5");

        assertEquals("val_5", res.get(0).get(0));

        // simple
        res = sql("SELECT valStr from test WHERE id > 4 AND valLong < 6");

        assertEquals("val_5", res.get(0).get(0));

        res = sql("SELECT valStr from test WHERE valLong < 5");

        assertEquals(5, res.size());

        // wildcard
        res = sql("SELECT * from test WHERE valLong < 5");

        assertEquals(5, res.size());
        assertEquals(3, res.get(0).size());

        // order by
        res = sql("SELECT valStr from test WHERE valLong < 5 ORDER BY id");

        assertEquals(5, res.size());

        res = sql("SELECT valStr from test WHERE valLong < 5 ORDER BY valStr");

        assertEquals(5, res.size());

        res = sql("SELECT valStr from test WHERE valLong < 5 ORDER BY valStr");

        assertEquals(5, res.size());

        // GROUP BY / aggregates
        res = sql("SELECT sum(valLong) from test WHERE valLong < 5 GROUP BY id");
        assertEquals(5, res.size());

        res = sql("SELECT sum(valLong) from test WHERE valLong < 5 GROUP BY id");
        assertEquals(5, res.size());

        res = sql("SELECT sum(id) from test WHERE valLong < 5 GROUP BY valLong");
        assertEquals(5, res.size());

        res = sql("SELECT sum(id) from test WHERE valLong < 5 GROUP BY valLong");
        assertEquals(5, res.size());

        // GROUP BY / having
        res = sql("SELECT id from test WHERE id < 5 GROUP BY id having sum(valLong) < 5");
        assertEquals(5, res.size());
    }

    /**
     * Test single table.
     */
    @Test
    public void testSimpleJoin() {
        List<List<?>> res;

        sql("CREATE TABLE person (id LONG, compId LONG, name VARCHAR, " +
                "PRIMARY KEY (id, compId)) " +
                "WITH \"AFFINITY_KEY=compId\"");

        sql("CREATE TABLE company (id LONG PRIMARY KEY, name VARCHAR)");
        sql("CREATE INDEX idx_company_name ON company (name)");

        long persId = 0;

        for (long compId = 0; compId < 10; ++compId) {
            sql("INSERT INTO company VALUES (?, ?)", compId, "company_" + compId);

            for (long persCnt = 0; persCnt < 10; ++persCnt, ++persId)
                sql("INSERT INTO person VALUES (?, ?, ?)", persId, compId, "person_" + compId + "_" + persId);
        }

        res = sql(
            "SELECT comp.name AS compName, pers.name AS persName FROM company AS comp " +
                "LEFT JOIN person AS pers ON comp.id = pers.compId " +
                "WHERE comp.name='company_1'");

        assertEquals(res.size(), 10);
    }

    /**
     * Test subquery.
     */
    @Test
    public void testSubquery() {
        List<List<?>> res;

        sql(
            "CREATE TABLE person (id LONG, compId LONG, name VARCHAR, " +
                "PRIMARY KEY (id, compId)) " +
                "WITH \"AFFINITY_KEY=compId\"");
        sql("CREATE TABLE company (id LONG PRIMARY KEY, name VARCHAR)");

        long persId = 0;
        for (long compId = 0; compId < 10; ++compId) {
            sql("INSERT INTO company VALUES (?, ?)", compId, "company_" + compId);

            for (long persCnt = 0; persCnt < 10; ++persCnt, ++persId)
                sql("INSERT INTO person VALUES (?, ?, ?)", persId, compId, "person_" + compId + "_" + persId);
        }

        res = sql(
            "SELECT comp.name FROM company AS comp " +
                "WHERE comp.id IN (SELECT MAX(COUNT(pers.id)) FROM person As pers WHERE pers.compId = comp.Id)");

        assertEquals(res.size(), 1);
    }

    /**
     * Test subquery.
     */
    @Test
    public void testSeveralSubqueries() {
        List<List<?>> res;

        sql(
            "CREATE TABLE test (id LONG PRIMARY KEY, " +
                "val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, val3 VARCHAR, val4 VARCHAR, val5 VARCHAR, val6 VARCHAR)");

        for (long i = 0; i < 10; ++i) {
            sql("INSERT INTO test VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                i, "0_" + i, "1_" + i, "2_" + i, "3_" + i, "4_" + i, "5_" + i, "6_" + i);

        }

//        res = sql(
//            "SELECT comp.name FROM company AS comp " +
//                "WHERE comp.id IN (SELECT MAX(COUNT(pers.id)) FROM person As pers WHERE pers.compId = comp.Id)");
//
//        assertEquals(res.size(), 1);
    }

    /**
     * @return Client node.
     */
    private IgniteEx client() {
        return grid(CLI_NAME);
    }

    /**
     * Execute prepared SQL fields query.
     *
     * @param qry Query.
     * @param args Query parameters.
     * @return Result.
     */
    private List<List<?>> sql(String qry, Object... args) {
        return executeSqlFieldsQuery(new SqlFieldsQuery(qry).setArgs(args));
    }

    /**
     * Execute prepared SQL fields query.
     *
     * @param qry Query.
     * @return Result.
     */
    private List<List<?>> executeSqlFieldsQuery(SqlFieldsQuery qry) {
        return client().context().query().querySqlFields(qry, false).getAll();
    }
}
