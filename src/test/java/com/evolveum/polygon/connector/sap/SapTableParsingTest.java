/*
 * Copyright (c) 2026 IS4IT
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.evolveum.polygon.connector.sap;

import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the lenient RFC_READ_TABLE table-definition parser. These do not require a SAP
 * connection - they exercise {@link SapConfiguration#parseReadTableDefinitions()} directly.
 * <p>
 * The configuration maps are keyed by the ALIAS (object class), with {@link SapConfiguration#getTableNames()}
 * mapping each alias to its SAP table - so the same SAP table can back several object classes.
 */
public class SapTableParsingTest {

    private SapConfiguration parse(String... tables) {
        SapConfiguration config = new SapConfiguration();
        config.setTableReadFunction(SapConfiguration.FN_READ_TABLE);
        config.setTables(tables);
        config.parseReadTableDefinitions();
        return config;
    }

    @Test
    public void testReadTableModeFlag() {
        SapConfiguration config = new SapConfiguration();
        assertFalse(config.isReadTableMode(), "default is the legacy RFC_GET_TABLE_ENTRIES path");

        config.setTableReadFunction(SapConfiguration.FN_READ_TABLE);
        assertTrue(config.isReadTableMode());

        config.setTableReadFunction(SapConfiguration.FN_BBP_READ_TABLE);
        assertTrue(config.isReadTableMode());

        config.setTableReadFunction("Z_MY_READ_TABLE");
        assertTrue(config.isReadTableMode(), "a custom Z-FM also enables the read-table path");

        config.setTableReadFunction(SapConfiguration.FN_GET_TABLE_ENTRIES);
        assertFalse(config.isReadTableMode());
    }

    @Test
    public void testBareTableName() {
        SapConfiguration config = parse("AGR_DEFINE");
        assertEquals("AGR_DEFINE", config.getTableNames().get("AGR_DEFINE"), "alias defaults to table name");
        assertEquals(Collections.emptyList(), config.getTableKeys().get("AGR_DEFINE"), "no key override -> DDIC keys");
        assertEquals(Collections.emptyList(), config.getTableIgnores().get("AGR_DEFINE"));
        assertNull(config.getTableWhere().get("AGR_DEFINE"));
    }

    @Test
    public void testAliasOnly() {
        SapConfiguration config = parse("AGR_DEFINE as ACTIVITYGROUP");
        assertEquals("AGR_DEFINE", config.getTableNames().get("ACTIVITYGROUP"), "alias maps to its SAP table");
        assertEquals(Collections.emptyList(), config.getTableKeys().get("ACTIVITYGROUP"));
    }

    @Test
    public void testLegacyLineStillParses() {
        SapConfiguration config = parse("AGR_DEFINE as ACTIVITYGROUP=MANDT:3:IGNORE,AGR_NAME:30:KEY,PARENT_AGR:30");
        assertEquals("AGR_DEFINE", config.getTableNames().get("ACTIVITYGROUP"));
        assertEquals(Collections.singletonList("AGR_NAME"), config.getTableKeys().get("ACTIVITYGROUP"),
                ":KEY becomes a key override, lengths are ignored");
        assertEquals(Collections.singletonList("MANDT"), config.getTableIgnores().get("ACTIVITYGROUP"));
        assertNull(config.getTableWhere().get("ACTIVITYGROUP"));
    }

    @Test
    public void testKeyOverrideWithoutLength() {
        SapConfiguration config = parse("AGR_DEFINE=AGR_NAME:KEY");
        assertEquals(Collections.singletonList("AGR_NAME"), config.getTableKeys().get("AGR_DEFINE"));
    }

    @Test
    public void testWhereClauseWithEqualsSign() {
        SapConfiguration config = parse("AGR_DEFINE as ACTIVITYGROUP WHERE PARENT_AGR <> '' AND SPRAS = 'E'");
        assertEquals("AGR_DEFINE", config.getTableNames().get("ACTIVITYGROUP"), "WHERE must not break alias parsing");
        assertEquals("PARENT_AGR <> '' AND SPRAS = 'E'", config.getTableWhere().get("ACTIVITYGROUP"),
                "the '=' inside the clause must stay in the WHERE, not split the table definition");
        assertEquals(Collections.emptyList(), config.getTableKeys().get("ACTIVITYGROUP"));
    }

    @Test
    public void testColumnsAndWhereCombined() {
        SapConfiguration config = parse("AGR_DEFINE=AGR_NAME:30:KEY WHERE PARENT_AGR <> ''");
        assertEquals(Collections.singletonList("AGR_NAME"), config.getTableKeys().get("AGR_DEFINE"));
        assertEquals("PARENT_AGR <> ''", config.getTableWhere().get("AGR_DEFINE"));
    }

    @Test
    public void testLowerCaseWhereKeyword() {
        SapConfiguration config = parse("AGR_DEFINE as ACTIVITYGROUP where PARENT_AGR <> ''");
        assertEquals("PARENT_AGR <> ''", config.getTableWhere().get("ACTIVITYGROUP"));
    }

    @Test
    public void testMultipleTables() {
        SapConfiguration config = parse("AGR_DEFINE as ACTIVITYGROUP", "USGRP as GROUP=USERGROUP:KEY");
        assertEquals(new LinkedHashSet<>(Arrays.asList("ACTIVITYGROUP", "GROUP")), config.getTableNames().keySet());
        assertEquals(Collections.singletonList("USERGROUP"), config.getTableKeys().get("GROUP"));
    }

    @Test
    public void testSameTableMultipleAliases() {
        SapConfiguration config = parse(
                "AGR_DEFINE as ACTIVITYGROUP",
                "AGR_DEFINE as AUDITROLES WHERE AGR_NAME LIKE 'SAP_AUDITOR%'");
        // both aliases exist as distinct object classes, both backed by the same SAP table
        assertEquals(new LinkedHashSet<>(Arrays.asList("ACTIVITYGROUP", "AUDITROLES")), config.getTableNames().keySet());
        assertEquals("AGR_DEFINE", config.getTableNames().get("ACTIVITYGROUP"));
        assertEquals("AGR_DEFINE", config.getTableNames().get("AUDITROLES"));
        // and each keeps its own WHERE clause
        assertNull(config.getTableWhere().get("ACTIVITYGROUP"));
        assertEquals("AGR_NAME LIKE 'SAP_AUDITOR%'", config.getTableWhere().get("AUDITROLES"));
    }

    @Test
    public void testDuplicateAliasRejected() {
        assertThrows(ConfigurationException.class,
                () -> parse("AGR_DEFINE as ROLES", "USGRP as ROLES"),
                "two definitions sharing one alias (object class) must be rejected");
    }

    @Test
    public void testParsingIsIdempotent() {
        SapConfiguration config = parse(
                "AGR_DEFINE as ACTIVITYGROUP",
                "AGR_DEFINE as AUDITROLES WHERE AGR_NAME LIKE 'SAP_AUDITOR%'");
        // a second parse must rebuild from scratch, not accumulate / report a false duplicate
        config.parseReadTableDefinitions();
        assertEquals(new LinkedHashSet<>(Arrays.asList("ACTIVITYGROUP", "AUDITROLES")), config.getTableNames().keySet());
        assertEquals("AGR_NAME LIKE 'SAP_AUDITOR%'", config.getTableWhere().get("AUDITROLES"));
    }

    /** A minimally valid configuration so {@link SapConfiguration#validate()} runs (it does no SAP call). */
    private SapConfiguration validatable(String... tables) {
        SapConfiguration config = new SapConfiguration();
        config.setHost("sap.example.com");
        config.setUser("MIDPOINT");
        config.setPlainPassword("secret");
        config.setClient("100");
        config.setTableParameterNames(new String[0]);
        config.setTableReadFunction(SapConfiguration.FN_READ_TABLE);
        config.setTables(tables);
        return config;
    }

    @Test
    public void testValidateIsIdempotent() {
        SapConfiguration config = validatable(
                "AGR_DEFINE as ACTIVITYGROUP",
                "AGR_DEFINE as AUDITROLES WHERE AGR_NAME LIKE 'SAP_AUDITOR%'");
        config.validate();
        config.validate();   // the framework / midPoint may call validate() repeatedly - it must not throw
        assertEquals(new LinkedHashSet<>(Arrays.asList("ACTIVITYGROUP", "AUDITROLES")), config.getTableNames().keySet());
    }

    @Test
    public void testValidateRejectsDuplicateAlias() {
        SapConfiguration config = validatable("AGR_DEFINE as ROLES", "USGRP as ROLES");
        assertThrows(ConfigurationException.class, config::validate,
                "validate() (config verify / test connection) must surface the duplicate alias to the GUI");
    }

    @Test
    public void testValidateAcceptsSubTableForKnownAlias() {
        SapConfiguration config = validatable("AGR_DEFINE as ACTIVITYGROUP");
        config.setSubTables(new String[]{"AGR_TEXTS for ACTIVITYGROUP as Descr=AGR_NAME:MATCH,TEXT"});
        config.validate();
        assertEquals(1, config.getSubTablesMetadata().get("ACTIVITYGROUP").size(),
                "sub-table is attached to its root object class (alias)");
    }

    @Test
    public void testValidateRejectsSubTableForUnknownAlias() {
        SapConfiguration config = validatable("AGR_DEFINE as ACTIVITYGROUP");
        // 'for AGR_DEFINE' is the SAP table name, not the alias - must be rejected (and surfaced in the GUI)
        config.setSubTables(new String[]{"AGR_TEXTS for AGR_DEFINE as Descr=AGR_NAME:MATCH,TEXT"});
        assertThrows(ConfigurationException.class, config::validate,
                "a sub-table whose 'for' alias is not a defined object class must be rejected");
    }
}
