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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerToken;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.ignite.internal.sql.SqlKeyword.AFFINITY_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.ATOMICITY;
import static org.apache.ignite.internal.sql.SqlKeyword.BACKUPS;
import static org.apache.ignite.internal.sql.SqlKeyword.BIGINT;
import static org.apache.ignite.internal.sql.SqlKeyword.BIT;
import static org.apache.ignite.internal.sql.SqlKeyword.BOOL;
import static org.apache.ignite.internal.sql.SqlKeyword.BOOLEAN;
import static org.apache.ignite.internal.sql.SqlKeyword.CACHE_GROUP;
import static org.apache.ignite.internal.sql.SqlKeyword.CACHE_NAME;
import static org.apache.ignite.internal.sql.SqlKeyword.CHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.CHARACTER;
import static org.apache.ignite.internal.sql.SqlKeyword.DATA_REGION;
import static org.apache.ignite.internal.sql.SqlKeyword.DATE;
import static org.apache.ignite.internal.sql.SqlKeyword.DATETIME;
import static org.apache.ignite.internal.sql.SqlKeyword.DEC;
import static org.apache.ignite.internal.sql.SqlKeyword.DECIMAL;
import static org.apache.ignite.internal.sql.SqlKeyword.DOUBLE;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT4;
import static org.apache.ignite.internal.sql.SqlKeyword.FLOAT8;
import static org.apache.ignite.internal.sql.SqlKeyword.IF;
import static org.apache.ignite.internal.sql.SqlKeyword.INT;
import static org.apache.ignite.internal.sql.SqlKeyword.INT2;
import static org.apache.ignite.internal.sql.SqlKeyword.INT4;
import static org.apache.ignite.internal.sql.SqlKeyword.INT8;
import static org.apache.ignite.internal.sql.SqlKeyword.INTEGER;
import static org.apache.ignite.internal.sql.SqlKeyword.KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.KEY_TYPE;
import static org.apache.ignite.internal.sql.SqlKeyword.LONG;
import static org.apache.ignite.internal.sql.SqlKeyword.LONGVARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.MEDIUMINT;
import static org.apache.ignite.internal.sql.SqlKeyword.NCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.NOT;
import static org.apache.ignite.internal.sql.SqlKeyword.NULL;
import static org.apache.ignite.internal.sql.SqlKeyword.NUMBER;
import static org.apache.ignite.internal.sql.SqlKeyword.NUMERIC;
import static org.apache.ignite.internal.sql.SqlKeyword.NVARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.NVARCHAR2;
import static org.apache.ignite.internal.sql.SqlKeyword.PRECISION;
import static org.apache.ignite.internal.sql.SqlKeyword.PRIMARY;
import static org.apache.ignite.internal.sql.SqlKeyword.REAL;
import static org.apache.ignite.internal.sql.SqlKeyword.SIGNED;
import static org.apache.ignite.internal.sql.SqlKeyword.SMALLDATETIME;
import static org.apache.ignite.internal.sql.SqlKeyword.SMALLINT;
import static org.apache.ignite.internal.sql.SqlKeyword.TEMPLATE;
import static org.apache.ignite.internal.sql.SqlKeyword.TIME;
import static org.apache.ignite.internal.sql.SqlKeyword.TIMESTAMP;
import static org.apache.ignite.internal.sql.SqlKeyword.TINYINT;
import static org.apache.ignite.internal.sql.SqlKeyword.UUID;
import static org.apache.ignite.internal.sql.SqlKeyword.VAL_TYPE;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR2;
import static org.apache.ignite.internal.sql.SqlKeyword.VARCHAR_CASESENSITIVE;
import static org.apache.ignite.internal.sql.SqlKeyword.WRAP_KEY;
import static org.apache.ignite.internal.sql.SqlKeyword.WRAP_VALUE;
import static org.apache.ignite.internal.sql.SqlKeyword.WRITE_SYNCHRONIZATION_MODE;
import static org.apache.ignite.internal.sql.SqlKeyword.YEAR;
import static org.apache.ignite.internal.sql.SqlParserUtils.error;
import static org.apache.ignite.internal.sql.SqlParserUtils.errorUnexpectedToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.matchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseEnum;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseIfNotExists;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseInt;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseQualifiedIdentifier;
import static org.apache.ignite.internal.sql.SqlParserUtils.parseString;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipCommaOrRightParenthesis;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipIfMatchesKeyword;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipOptionalEquals;
import static org.apache.ignite.internal.sql.SqlParserUtils.skipToken;
import static org.apache.ignite.internal.sql.SqlParserUtils.tryParseBool;

/**
 * CREATE INDEX command.
 */
public class SqlCreateTableCommand implements SqlCommand {

    /** Schema name. */
    @GridToStringInclude
    private String schemaName;

    /** Table name. */
    @GridToStringInclude
    private String tblName;

    /** IF NOT EXISTS flag. */
    @GridToStringInclude
    private boolean ifNotExists;

    /** Columns. */
    @GridToStringInclude
    private Map<String, SqlColumn> cols;

    /** Primary key column names. */
    @GridToStringInclude
    private Set<String> pkColNames;

    /** Cache name upon which new cache configuration for this table must be based. */
    @GridToStringInclude
    private String templateName;

    /** Name of new cache associated with this table. */
    @GridToStringInclude
    private String cacheName;

    /** Group to put new cache into. */
    @GridToStringInclude
    private String cacheGrp;

    /** Atomicity mode for new cache. */
    @GridToStringInclude
    private CacheAtomicityMode atomicityMode;

    /** Write sync mode. */
    @GridToStringInclude
    private CacheWriteSynchronizationMode writeSyncMode;

    /** Backups number for new cache. */
    @GridToStringInclude
    private int backups;

    /** Name of the column that represents affinity key. */
    @GridToStringInclude
    private String affinityKey;

    /** Forcefully turn single column PK into an Object. */
    @GridToStringInclude
    private Boolean wrapKey;

    /** Forcefully turn single column value into an Object. */
    @GridToStringInclude
    private Boolean wrapVal;

    /** Name of cache key type. */
    @GridToStringInclude
    private String keyTypeName;

    /** Name of cache value type. */
    @GridToStringInclude
    private String valTypeName;

    /** Data region. */
    @GridToStringInclude
    private String dataRegionName;

    /**
     * @return Cache name upon which new cache configuration for this table must be based.
     */
    public String templateName() {
        return templateName;
    }

    /**
     * @param templateName Cache name upon which new cache configuration for this table must be based.
     */
    public void templateName(String templateName) {
        this.templateName = templateName;
    }

    /**
     * @return Name of new cache associated with this table.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @param cacheName Name of new cache associated with this table.
     */
    public void cacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    /**
     * @return Name of cache key type.
     */
    public String keyTypeName() {
        return keyTypeName;
    }

    /**
     * @param keyTypeName Name of cache key type.
     */
    public void keyTypeName(String keyTypeName) {
        this.keyTypeName = keyTypeName;
    }

    /**
     * @return Name of cache value type.
     */
    public String valueTypeName() {
        return valTypeName;
    }

    /**
     * @param valTypeName Name of cache value type.
     */
    public void valueTypeName(String valTypeName) {
        this.valTypeName = valTypeName;
    }

    /**
     * @return Group to put new cache into.
     */
    public String cacheGroup() {
        return cacheGrp;
    }

    /**
     * @param cacheGrp Group to put new cache into.
     */
    public void cacheGroup(String cacheGrp) {
        this.cacheGrp = cacheGrp;
    }

    /**
     * @return Atomicity mode for new cache.
     */
    public CacheAtomicityMode atomicityMode() {
        return atomicityMode;
    }

    /**
     * @param atomicityMode Atomicity mode for new cache.
     */
    public void atomicityMode(CacheAtomicityMode atomicityMode) {
        this.atomicityMode = atomicityMode;
    }

    /**
     * @return Write sync mode for new cache.
     */
    public CacheWriteSynchronizationMode writeSynchronizationMode() {
        return writeSyncMode;
    }

    /**
     * @param writeSyncMode Write sync mode for new cache.
     */
    public void writeSynchronizationMode(CacheWriteSynchronizationMode writeSyncMode) {
        this.writeSyncMode = writeSyncMode;
    }

    /**
     * @return Backups number for new cache.
     */
    public int backups() {
        return backups;
    }

    /**
     * @return Name of the column that represents affinity key.
     */
    public String affinityKey() {
        return affinityKey;
    }

    /**
     * @param affinityKey Name of the column that represents affinity key.
     */
    public void affinityKey(String affinityKey) {
        this.affinityKey = affinityKey;
    }

    /**
     * @return Forcefully turn single column PK into an Object.
     */
    public Boolean wrapKey() {
        return wrapKey;
    }

    /**
     * @param wrapKey Forcefully turn single column PK into an Object.
     */
    public void wrapKey(boolean wrapKey) {
        this.wrapKey = wrapKey;
    }

    /**
     * @return Forcefully turn single column value into an Object.
     */
    public Boolean wrapValue() {
        return wrapVal;
    }

    /**
     * @return Data region name.
     */
    public String dataRegionName() {
        return dataRegionName;
    }

    /**
     * @param wrapVal Forcefully turn single column value into an Object..
     */
    public void wrapValue(boolean wrapVal) {
        this.wrapVal = wrapVal;
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return IF NOT EXISTS flag.
     */
    public boolean ifNotExists() {
        return ifNotExists;
    }

    /**
     * @return Columns.
     */
    public Map<String, SqlColumn> columns() {
        return cols != null ? cols : Collections.<String, SqlColumn>emptyMap();
    }

    /**
     * @return PK column names.
     */
    public Set<String> primaryKeyColumnNames() {
        return pkColNames != null ? pkColNames : Collections.<String>emptySet();
    }

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        ifNotExists = parseIfNotExists(lex);

        SqlQualifiedName tblQName = parseQualifiedIdentifier(lex, IF);

        schemaName = tblQName.schemaName();
        tblName = tblQName.name();

        parseColumnConstraintList(lex);

        parseParameters(lex);

        return this;
    }

    /**
     * Parses columns and constraints list.
     *
     * @param lex The lexer.
     */
    private void parseColumnConstraintList(SqlLexer lex) {
        if (!lex.shift() || lex.tokenType() != SqlLexerTokenType.PARENTHESIS_LEFT)
            throw errorUnexpectedToken(lex, "(");

        while (true) {
            parseColumnOrConstraint(lex);

            if (skipCommaOrRightParenthesis(lex) == SqlLexerTokenType.PARENTHESIS_RIGHT)
                break;
        }
    }

    /**
     * Parses column or constraint.
     *
     * @param lex The lexer.
     */
    private void parseColumnOrConstraint(SqlLexer lex) {
        SqlLexerToken next = lex.lookAhead();

        if (next.tokenType() == SqlLexerTokenType.EOF)
            throw errorUnexpectedToken(lex, PRIMARY, "[column definition]");

        if (matchesKeyword(next, PRIMARY))
            parsePrimaryKeyConstraint(lex);
        else
            parseColumn(lex);
    }

    /**
     * Parses column definition and adds it to column list and primary key list.
     *
     * @param lex The lexer.
     */
    private void parseColumn(SqlLexer lex) {
        String name = parseIdentifier(lex);

        if (lex.shift() && lex.tokenType() == SqlLexerTokenType.DEFAULT) {
            SqlColumn col = null;

            String typTok = lex.token();

            Boolean nullableOpt = parseNullableClause(lex);

            boolean isNullable = (nullableOpt != null) ? nullableOpt : true;

            switch (typTok) {
                case BIT:
                case BOOL:
                case BOOLEAN:
                    col = new SqlColumn(name, SqlColumnType.BOOLEAN, 0, 0, isNullable);

                    break;

                case TINYINT:
                    col = new SqlColumn(name, SqlColumnType.BYTE, 0, 0, isNullable);

                    break;

                case INT2:
                case SMALLINT:
                case YEAR:
                    col = new SqlColumn(name, SqlColumnType.SHORT, 0, 0, isNullable);

                    break;

                case INT:
                case INT4:
                case INTEGER:
                case MEDIUMINT:
                case SIGNED:
                    col = new SqlColumn(name, SqlColumnType.INT, 0, 0, isNullable);

                    break;

                case BIGINT:
                case INT8:
                case LONG:
                    col = new SqlColumn(name, SqlColumnType.LONG, 0, 0, isNullable);

                    break;

                case FLOAT4:
                case REAL:
                    col = new SqlColumn(name, SqlColumnType.FLOAT, 0, 0, isNullable);

                    break;

                case DOUBLE: {
                    SqlLexerToken next = lex.lookAhead();

                    if (matchesKeyword(next, PRECISION))
                        lex.shift();

                    col = new SqlColumn(name, SqlColumnType.DOUBLE, 0, 0, isNullable);

                    break;
                }

                case FLOAT:
                case FLOAT8:
                    col = new SqlColumn(name, SqlColumnType.DOUBLE, 0, 0, isNullable);

                    break;

                case DEC:
                case DECIMAL:
                case NUMBER:
                case NUMERIC: {
                    skipToken(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

                    int scale = parseInt(lex);
                    int precision = 0;

                    if (skipCommaOrRightParenthesis(lex) == SqlLexerTokenType.COMMA) {
                        precision = parseInt(lex);

                        skipToken(lex, SqlLexerTokenType.PARENTHESIS_RIGHT);
                    }

                    col = new SqlColumn(name, SqlColumnType.DECIMAL, scale, precision, isNullable);

                    break;
                }

                case CHAR:
                case CHARACTER:
                case NCHAR: {
                    int precision = parseStringPrecision(lex);

                    col = new SqlColumn(name, SqlColumnType.CHAR, 0, precision, isNullable);

                    break;
                }

                case LONGVARCHAR:
                case NVARCHAR:
                case NVARCHAR2:
                case VARCHAR:
                case VARCHAR2:
                case VARCHAR_CASESENSITIVE: {
                    int precision = parseStringPrecision(lex);

                    col = new SqlColumn(name, SqlColumnType.VARCHAR, 0, precision, isNullable);

                    break;
                }

                case DATE:
                    col = new SqlColumn(name, SqlColumnType.DATE, 0, 0, isNullable);

                    break;

                case TIME:
                    col = new SqlColumn(name, SqlColumnType.TIME, 0, 0, isNullable);

                    break;

                case DATETIME:
                case SMALLDATETIME:
                case TIMESTAMP:
                    col = new SqlColumn(name, SqlColumnType.TIMESTAMP, 0, 0, isNullable);

                    break;

                case UUID:
                    col = new SqlColumn(name, SqlColumnType.UUID, 0, 0, isNullable);

                    break;
            }

            if (col != null) {
                addColumn(lex, col);

                if (matchesKeyword(lex.lookAhead(), PRIMARY)) {
                    if (pkColNames != null)
                        throw error(lex, "PRIMARY KEY is already defined.");

                    pkColNames = new HashSet<>();

                    pkColNames.add(col.name());

                    lex.shift();

                    skipIfMatchesKeyword(lex, KEY);
                }

                return;
            }
        }

        throw errorUnexpectedToken(lex, "[column_type]");
    }

    /**
     * Parses optional NULL and NOT NULL clauses in column definition.
     *
     * @param lex The lexer
     * @return null, if the clause is not found, true if NULL is specified, false if NOT NULL is specified.
     */
    @Nullable private Boolean parseNullableClause(SqlLexer lex) {
        Boolean isNullable = null;

        if (matchesKeyword(lex.lookAhead(), NOT)) {

            lex.shift();
            skipIfMatchesKeyword(lex, NULL);

            isNullable = false;

        } else if (matchesKeyword(lex.lookAhead(), NULL)) {

            lex.shift();

            isNullable = true;
        }

        return isNullable;
    }

    /**
     * Adds column definition to the list of columns.
     *
     * @param lex The lexer.
     * @param col The column.
     */
    private void addColumn(SqlLexer lex, SqlColumn col) {
        if (cols == null)
            cols = new LinkedHashMap<>();

        if (cols.containsKey(col.name()))
            throw error(lex, "Column already defined: " + col.name());

        cols.put(col.name(), col);
    }

    /**
     * Parses primary key constraint.
     *
     * @param lex The lexer.
     */
    private void parsePrimaryKeyConstraint(SqlLexer lex) {
        if (pkColNames != null)
            throw error(lex, "PRIMARY KEY is already defined.");

        pkColNames = new HashSet<>();

        skipIfMatchesKeyword(lex, PRIMARY);
        skipIfMatchesKeyword(lex, KEY);

        skipToken(lex, SqlLexerTokenType.PARENTHESIS_LEFT);

        while (true) {
            String pkColName = parseIdentifier(lex);

            if (!pkColNames.add(pkColName))
                throw error(lex, "Duplicate PK column name: " + pkColName);

            if (skipCommaOrRightParenthesis(lex) == SqlLexerTokenType.PARENTHESIS_RIGHT)
                break;
        }
    }

    /**
     * Parses precision for CHAR and VARCHAR types.
     *
     * @param lex Lexer.
     * @return Precision.
     */
    private static int parseStringPrecision(SqlLexer lex) {
        SqlLexerToken next = lex.lookAhead();

        int res = Integer.MAX_VALUE;

        if (next.tokenType() == SqlLexerTokenType.PARENTHESIS_LEFT) {
            lex.shift();

            res = parseInt(lex);

            skipToken(lex, SqlLexerTokenType.PARENTHESIS_RIGHT);
        }

        return res;
    }

    /**
     * Parses parameter section of the command and updates the internal state.
     *
     * @param lex The lexer.
     */
    private void parseParameters(SqlLexer lex) {
        Set<String> oldParamNames = new HashSet<>();

        while (true) {
            SqlLexerToken token = lex.lookAhead();

            if (token.tokenType() == SqlLexerTokenType.EOF)
                return;

            if (token.tokenType() == SqlLexerTokenType.DEFAULT) {
                switch (token.token()) {
                    case TEMPLATE: {
                        acceptParameterName(lex, token.token(), oldParamNames);

                        templateName = parseString(lex);

                        break;
                    }

                    case BACKUPS: {
                        acceptParameterName(lex, token.token(), oldParamNames);

                        backups = parseInt(lex);

                        if (backups < 0)
                            throw error(lex.currentToken(), "Number of backups should be positive [val=" + backups + "]");

                        break;
                    }

                    case ATOMICITY: {
                        acceptParameterName(lex, token.token(), oldParamNames);

                        atomicityMode = parseEnum(lex, CacheAtomicityMode.class);

                        break;
                    }

                    case WRITE_SYNCHRONIZATION_MODE: {
                        acceptParameterName(lex, token.token(), oldParamNames);

                        writeSyncMode = parseEnum(lex, CacheWriteSynchronizationMode.class);

                        break;
                    }

                    case CACHE_GROUP: {
                        acceptParameterName(lex, token.token(), oldParamNames);

                        cacheGrp = parseString(lex);

                        break;
                    }

                    case AFFINITY_KEY: {
                        acceptParameterName(lex, token.token(), oldParamNames);

                        affinityKey = parseString(lex);

                        if (!cols.containsKey(affinityKey))
                            throw error(lex, "Affinity column is not present in table column list: " + affinityKey);

                        break;
                    }

                    case CACHE_NAME: {
                        acceptParameterName(lex, token.token(), oldParamNames);

                        cacheName = parseString(lex);

                        break;
                    }

                    case DATA_REGION: {
                        acceptParameterName(lex, token.token(), oldParamNames);

                        dataRegionName = parseString(lex);

                        break;
                    }

                    case KEY_TYPE: {
                        acceptParameterName(lex, token.token(), oldParamNames);

                        keyTypeName = parseString(lex);

                        break;
                    }

                    case VAL_TYPE: {
                        acceptParameterName(lex, token.token(), oldParamNames);

                        valTypeName = parseString(lex);

                        break;
                    }

                    case WRAP_KEY: {
                        boolean hasEquals = acceptParameterName(lex, token.token(), oldParamNames);

                        wrapKey = tryParseBool(lex, hasEquals);

                        break;
                    }

                    case WRAP_VALUE: {
                        boolean hasEquals = acceptParameterName(lex, token.token(), oldParamNames);

                        wrapVal = tryParseBool(lex, hasEquals);

                        break;
                    }

                    default:
                        return;
                }
            }
        }
    }

    /**
     * Skip valid parameter name.
     *
     * @param lex Lexer.
     * @param param Token.
     * @param oldParams Already found parameter names.
     * @return {@code True} if lexer was shifted as a result of this call.
     */
    private static boolean acceptParameterName(SqlLexer lex, String param, Set<String> oldParams) {
        if (!oldParams.add(param))
            throw error(lex, "Only one " + param + " clause may be specified.");

        lex.shift();

        return skipOptionalEquals(lex);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlCreateTableCommand.class, this);
    }
}
