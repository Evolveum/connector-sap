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

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for the sub-table parser, focused on the RFC_READ_TABLE path: an existing
 * RFC_GET_TABLE_ENTRIES sub-table definition stays usable (the fixed-width {@code :<size>} is ignored),
 * a simplified definition without sizes parses the same, and an optional trailing WHERE is supported.
 */
public class SapSubTableParsingTest {

    private TableColumnDefinition column(SubTableMetadata m, String name) {
        return m.getColumns().stream()
                .filter(c -> name.equals(c.getColumnName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no column " + name + " in " + m.getColumns()));
    }

    @Test
    public void legacyDefinitionStillParsesInReadTableMode() {
        SubTableMetadata m = SubTableMetadata.parseConfig(
                "AGR_TEXTS for ACTIVITYGROUP format TSV as ShortDescription="
                        + "MANDT:3:IGNORE,AGR_NAME:30:MATCH,SPRAS:1(\"E\"):IGNORE,LINE:5(\"00000\"):IGNORE,TEXT:80",
                true);

        assertEquals("AGR_TEXTS", m.getTableName());
        assertEquals("ACTIVITYGROUP", m.getRootAlias(), "the 'for' part is the root object class (alias)");
        assertEquals(SubTableMetadata.Format.TSV, m.getFormat());
        assertEquals("ShortDescription", m.getVirtualColumnName());
        assertNull(m.getWhere());

        assertEquals(TableColumnDefinition.Mode.IGNORE, column(m, "MANDT").getMode());
        assertEquals(TableColumnDefinition.Mode.MATCH, column(m, "AGR_NAME").getMode());
        assertEquals(TableColumnDefinition.Mode.IGNORE, column(m, "SPRAS").getMode());
        assertEquals("E", column(m, "SPRAS").getFilterConstant());
        assertEquals("00000", column(m, "LINE").getFilterConstant());
        assertEquals(TableColumnDefinition.Mode.OUTPUT, column(m, "TEXT").getMode());
    }

    @Test
    public void simplifiedDefinitionWithoutSizesParsesTheSame() {
        SubTableMetadata m = SubTableMetadata.parseConfig(
                "AGR_TEXTS for ACTIVITYGROUP format TSV as ShortDescription="
                        + "AGR_NAME:MATCH,SPRAS(\"E\"):IGNORE,LINE(\"00000\"):IGNORE,TEXT",
                true);

        assertEquals(TableColumnDefinition.Mode.MATCH, column(m, "AGR_NAME").getMode());
        assertEquals("E", column(m, "SPRAS").getFilterConstant());
        assertEquals(TableColumnDefinition.Mode.IGNORE, column(m, "SPRAS").getMode());
        assertEquals("00000", column(m, "LINE").getFilterConstant());
        assertEquals(TableColumnDefinition.Mode.OUTPUT, column(m, "TEXT").getMode());
        assertNull(m.getWhere());
    }

    @Test
    public void trailingWhereIsSplitOff() {
        SubTableMetadata m = SubTableMetadata.parseConfig(
                "AGR_TEXTS for ACTIVITYGROUP as Descr=AGR_NAME:MATCH,TEXT WHERE SPRAS = 'E' AND LINE = '00000'",
                true);

        assertEquals("SPRAS = 'E' AND LINE = '00000'", m.getWhere());
        assertEquals(SubTableMetadata.Format.XML, m.getFormat(), "format defaults to XML");
        assertEquals("Descr", m.getVirtualColumnName());
        assertEquals(TableColumnDefinition.Mode.MATCH, column(m, "AGR_NAME").getMode());
        assertEquals(TableColumnDefinition.Mode.OUTPUT, column(m, "TEXT").getMode());
        // the '=' inside the WHERE must not have split the definition
        assertEquals(2, m.getColumns().size());
    }

    @Test
    public void whereCanReferenceRootFieldsForTheJoin() {
        SubTableMetadata m = SubTableMetadata.parseConfig(
                "AGR_TEXTS for ACTIVITYGROUP as Descr=TEXT WHERE AGR_NAME = ACTIVITYGROUP.AGR_NAME AND SPRAS = 'D'",
                true);
        assertEquals(Set.of("AGR_NAME"), m.getRootFieldReferences());
        assertEquals("AGR_NAME = 'SAP_AUDITOR' AND SPRAS = 'D'",
                m.resolveWhere(Map.of("AGR_NAME", "SAP_AUDITOR")));
    }

    @Test
    public void whereCanJoinDifferentlyNamedFields() {
        // root HRP1000 as ORGUNITS; join the differently-named OBJID (sub) to PERNR (root)
        SubTableMetadata m = SubTableMetadata.parseConfig(
                "HRP1001 for ORGUNITS as Roles=STEXT WHERE OBJID = ORGUNITS.PERNR", true);
        assertEquals("ORGUNITS", m.getRootAlias());
        assertEquals(Set.of("PERNR"), m.getRootFieldReferences());
        assertEquals("OBJID = '00012345'", m.resolveWhere(Map.of("PERNR", "00012345")));
    }

    @Test
    public void rootReferenceValueIsQuotedAndEscaped() {
        SubTableMetadata m = SubTableMetadata.parseConfig("T for ROOT as X=F WHERE A = ROOT.B", true);
        assertEquals("A = 'O''Brien'", m.resolveWhere(Map.of("B", "O'Brien")));
    }

    @Test
    public void noRootReferenceLeavesWhereUnchanged() {
        SubTableMetadata m = SubTableMetadata.parseConfig("T for ROOT as X=F:MATCH,G WHERE SPRAS = 'E'", true);
        assertEquals(Set.of(), m.getRootFieldReferences());
        assertEquals("SPRAS = 'E'", m.resolveWhere(Map.of()));
    }

    @Test
    public void legacyModeKeepsFixedWidthLengths() {
        SubTableMetadata m = SubTableMetadata.parseConfig(
                "AGR_TEXTS for ACTIVITYGROUP as Descr=MANDT:3:IGNORE,AGR_NAME:30:MATCH,TEXT:80",
                false);
        // offsets are computed from the lengths in legacy mode (TEXT starts after MANDT(3)+AGR_NAME(30))
        assertEquals(33, column(m, "TEXT").getOffset());
        assertEquals(80, column(m, "TEXT").getLength());
        assertNull(m.getWhere(), "no WHERE handling in legacy mode");
    }
}
