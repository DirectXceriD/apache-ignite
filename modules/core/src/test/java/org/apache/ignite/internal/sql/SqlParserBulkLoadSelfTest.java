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

package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.util.typedef.X;

/**
 * Tests for SQL parser: COPY command.
 */
public class SqlParserBulkLoadSelfTest extends SqlParserAbstractSelfTest {
    /**
     * Tests for COPY command.
     */
    public void testCopy() {
        assertParseError(null,
            "copy grom \"any.file\" into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"GROM\" (expected: \"FROM\")");

        assertParseError(null,
            "copy from into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"INTO\" (expected: \"[quoted file name]\"");

        assertParseError(null,
            "copy from unquoted into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"UNQUOTED\" (expected: \"[quoted file name]\"");

        assertParseError(null,
            "copy from unquoted.file into Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"UNQUOTED\" (expected: \"[quoted file name]\"");

        assertParseSuccess(null,
            "copy from \"\" into Person (_key, age, firstName, lastName) format csv");

        assertParseSuccess(null,
            "copy from \"d:/copy/from/into/format.csv\" into Person (_key, age, firstName, lastName) format csv");

        assertParseSuccess(null,
            "copy from \"/into\" into Person (_key, age, firstName, lastName) format csv");

        assertParseSuccess(null,
            "copy from \"into\" into Person (_key, age, firstName, lastName) format csv");

        assertParseError(null,
            "copy from \"any.file\" to Person (_key, age, firstName, lastName) format csv",
            "Unexpected token: \"TO\" (expected: \"INTO\")");

        // Column list

        assertParseError(null,
            "copy from \"any.file\" into Person () format csv",
            "Unexpected token: \")\" (expected: \"[identifier]\")");

        assertParseError(null,
            "copy from \"any.file\" into Person (,) format csv",
            "Unexpected token: \",\" (expected: \"[identifier]\")");

        assertParseError(null,
            "copy from \"any.file\" into Person format csv",
            "Unexpected token: \"FORMAT\" (expected: \"(\")");

        // FORMAT

        assertParseError(null,
            "copy from \"any.file\" into Person (_key, age, firstName, lastName)",
            "Unexpected end of command (expected: \"FORMAT\")");

        assertParseError(null,
            "copy from \"any.file\" into Person (_key, age, firstName, lastName) format lsd",
            "Unknown format name: LSD");

        // FORMAT CSV options

        assertParseError(null,
            "copy from \"any.file\" into Person (col1, col2) format csv linesep",
            "Unexpected end of command (expected: \"[quoted string]\")");

        assertParseError(null,
            "copy from \"any.file\" into Person (col1, col2) format csv fieldsep",
            "Unexpected end of command (expected: \"[quoted string]\")");

        assertParseError(null,
            "copy from \"any.file\" into Person (col1, col2) format csv quote",
            "Unexpected end of command (expected: \"[quoted string]\")");

        assertParseError(null,
            "copy from \"any.file\" into Person (col1, col2) format csv escape",
            "Unexpected end of command (expected: \"[quoted string]\")");

        assertParseError(null,
            "copy from \"any.file\" into Person (col1, col2) format csv comment",
            "Unexpected end of command (expected: \"[quoted string]\")");

        assertParseSuccess(null,
            "copy from \"any.file\" into Person (col1, col2)" +
                " format csv fieldsep \":\"" +
                " batch_size 1");

        assertParseSuccess(null,
            "copy from \"any.file\" into Person (col1, col2)" +
                " format csv fieldsep \":\" linesep \"\n\" quote \"\\\"\" escape \"\" comment \";\"" +
                " batch_size 1");
    }

    /**
     * Verifies that SQL is successfully parsed and logs a message with the statement.
     *
     * @param schemaName The schema name.
     * @param sql The SQL statement to verify.
     */
    public static void assertParseSuccess(String schemaName, String sql) {
        new SqlParser(schemaName, sql).nextCommand();

        X.println("Successfully parsed SQL statement: " + sql);
    }
}
