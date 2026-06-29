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

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptionsBuilder;
import org.identityconnectors.framework.common.objects.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Live test for the table-read paths. Runs the same schema / find-all / find-by-key assertions against
 * BOTH implementations: the legacy RFC_GET_TABLE_ENTRIES (positional col:len:KEY config) and the
 * RFC_READ_TABLE path (self-describing config). Requires a real SAP system, driven by a
 * {@code test.properties} on the test classpath (see {@code test.properties.example}); when that file is
 * absent the whole test is skipped (JUnit assumptions), so it stays green without a SAP system.
 */
public class SapReadTableLiveTest {

    private static final Log LOG = Log.getLog(SapReadTableLiveTest.class);

    private static final String CONFIG_FILE = "test.properties";
    private static final int MAX_ROWS = 10;

    private static Properties props;
    /** Set in {@link #loadProps()}: true only when test.properties is present AND SAP is reachable. */
    private static boolean sapReachable;

    /** The two table-read implementations, each with table definitions in the form it expects. */
    static Stream<Arguments> tableReadModes() {
        return Stream.of(
                arguments("RFC_GET_TABLE_ENTRIES",
                        "AGR_DEFINE as ACTIVITYGROUP=MANDT:3:IGNORE,AGR_NAME:30:KEY,PARENT_AGR:30"
                                + ";USGRP as GROUP=MANDT:3:IGNORE,USERGROUP:12:KEY"),
                arguments("RFC_READ_TABLE",
                        "AGR_DEFINE as ACTIVITYGROUP;USGRP as GROUP"));
    }

    @BeforeAll
    static void loadProps() throws Exception {
        props = load(CONFIG_FILE);
        if (props == null) {
            LOG.info("{0} not found on the test classpath - live SAP tests will be skipped", CONFIG_FILE);
            return;
        }
        // test.properties being present only means we know HOW to reach SAP, not that SAP is up. Probe it
        // once so an unreachable SAP skips the tests instead of failing every connection below.
        sapReachable = SapLiveTestSupport.sapReachable(buildConfiguration(props));
    }

    @Test
    public void testConnection() {
        assumeTrue(props != null && sapReachable, "live SAP system not available - skipping");
        SapConnector connector = new SapConnector();
        try {
            connector.init(buildConfiguration(props));
            connector.test();
        } finally {
            dispose(connector);
        }
    }

    /**
     * Schema, find-all and find-by-key against each table-read implementation. find-by-key discovers a
     * real key via find-all and looks it up, expecting exactly that one row - the data-independent
     * counterpart to TestClient.testFindOneActivityGroups.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("tableReadModes")
    public void tableReadWorks(String tableReadFunction, String tables) {
        assumeTrue(props != null && sapReachable, "live SAP system not available - skipping");

        SapConfiguration config = buildConfiguration(props);
        config.setTableReadFunction(tableReadFunction);
        config.setTables(tables.split(";"));

        SapConnector connector = new SapConnector();
        try {
            connector.init(config);

            Schema schema = connector.schema();
            for (String alias : config.getTableNames().keySet()) {
                assertNotNull(schema.findObjectClassInfo(alias),
                        tableReadFunction + ": schema is missing object class for table alias '" + alias + "'");
            }

            for (String alias : config.getTableNames().keySet()) {
                ObjectClass objectClass = new ObjectClass(alias);

                List<ConnectorObject> all = new ArrayList<>();
                connector.executeQuery(objectClass, null, co -> {
                    all.add(co);
                    return all.size() < MAX_ROWS;
                }, new OperationOptionsBuilder().build());
                LOG.info("{0} / {1}: read {2} row(s)", tableReadFunction, alias, all.size());
                if (all.isEmpty()) {
                    LOG.info("{0} / {1}: no rows, skipping find-by-key check", tableReadFunction, alias);
                    continue;
                }

                String uid = all.get(0).getUid().getUidValue();
                List<ConnectorObject> byKey = new ArrayList<>();
                connector.executeQuery(objectClass, new SapFilter(uid), co -> {
                    byKey.add(co);
                    return true;
                }, new OperationOptionsBuilder().build());

                assertEquals(1, byKey.size(),
                        tableReadFunction + ": find-by-key '" + uid + "' on " + alias + " must return exactly one row");
                assertEquals(uid, byKey.get(0).getUid().getUidValue(),
                        tableReadFunction + ": find-by-key returned the wrong row");
                LOG.info("{0} / {1}: find-by-key '{2}' returned exactly one row", tableReadFunction, alias, uid);
            }
        } finally {
            dispose(connector);
        }
    }

    /**
     * Validates the OPTIONS WHERE splitting against live SAP: a long WHERE (long enough to force the
     * &gt;72-character wrapping) that contains quoted literals with embedded spaces must reach
     * RFC_READ_TABLE intact - the wrapping must neither cut a token nor split inside a quoted value.
     * We discover a real key {@code V} from AGR_DEFINE and run a logically-equivalent but tricky query
     * <pre>AGR_NAME = 'V' AND AGR_NAME IN ('AA BB', 'CC DD', ... , 'V')</pre>
     * whose IN-list is long enough to be wrapped and whose entries carry spaces inside quotes. Had the
     * splitter broken a quote or a token, SAP would raise a syntax error or match the wrong rows;
     * instead we assert it returns exactly the same single row as the plain {@code AGR_NAME = 'V'} query.
     */
    @Test
    public void whereSplittingPreservesQuotedSpaces() {
        assumeTrue(props != null && sapReachable, "live SAP system not available - skipping");

        String tableDef = "AGR_DEFINE as ACTIVITYGROUP"; // RFC_READ_TABLE: key (AGR_NAME) read from DDIC

        List<String> baseline = findUids(tableDef);
        assumeTrue(!baseline.isEmpty(), "AGR_DEFINE has no readable rows - skipping");
        String v = baseline.get(0);
        String vSql = v.replace("'", "''");

        // IN-list of spaced, quoted entries, long enough that the segment exceeds 72 characters and is
        // wrapped; it ends with the real key so the AND stays satisfiable and the result equals 'V'.
        StringBuilder inList = new StringBuilder();
        for (String entry : new String[]{"AA BB", "CC DD", "EE FF", "GG HH", "II JJ", "KK LL", "MM NN"}) {
            inList.append('\'').append(entry).append("', ");
        }
        inList.append('\'').append(vSql).append('\'');
        String trickyWhere = "AGR_NAME = '" + vSql + "' AND AGR_NAME IN (" + inList + ")";
        assumeTrue(trickyWhere.length() > 72, "tricky WHERE is not long enough to force OPTIONS wrapping");

        List<String> result = findUids(tableDef + " WHERE " + trickyWhere);
        assertEquals(List.of(v), result,
                "a long WHERE with spaced, quoted IN-list entries must return exactly the discovered key '"
                        + v + "' - the splitter must not cut a token or split inside a quote");
        LOG.info("WHERE splitting preserved quoted spaces: tricky query returned exactly '{0}'", v);
    }

    /** find-all over a single RFC_READ_TABLE table definition, returning up to {@link #MAX_ROWS} uids. */
    private static List<String> findUids(String tableDef) {
        SapConfiguration config = buildConfiguration(props);
        config.setTableReadFunction(SapConfiguration.FN_READ_TABLE);
        config.setTables(new String[]{tableDef});

        SapConnector connector = new SapConnector();
        List<String> uids = new ArrayList<>();
        try {
            connector.init(config);
            String alias = config.getTableNames().keySet().iterator().next();
            connector.executeQuery(new ObjectClass(alias), null, co -> {
                uids.add(co.getUid().getUidValue());
                return uids.size() < MAX_ROWS;
            }, new OperationOptionsBuilder().build());
        } finally {
            dispose(connector);
        }
        return uids;
    }

    private static void dispose(SapConnector connector) {
        try {
            connector.dispose();
        } catch (Exception e) {
            LOG.warn("connector.dispose() failed: {0}", e);
        }
    }

    private static Properties load(String fileName) throws Exception {
        try (InputStream in = SapReadTableLiveTest.class.getClassLoader().getResourceAsStream(fileName)) {
            if (in == null) {
                return null;
            }
            Properties properties = new Properties();
            properties.load(in);
            return properties;
        }
    }

    private static SapConfiguration buildConfiguration(Properties p) {
        SapConfiguration c = new SapConfiguration();
        if (p.containsKey("loadBalancing")) {
            c.setLoadBalancing(Boolean.parseBoolean(p.getProperty("loadBalancing")));
        }
        c.setHost(p.getProperty("host"));
        if (p.containsKey("port")) {
            c.setPort(p.getProperty("port"));
        }
        c.setUser(p.getProperty("user"));
        c.setPlainPassword(p.getProperty("password"));
        if (p.containsKey("logonGroup")) {
            c.setLogonGroup(p.getProperty("logonGroup"));
        }
        c.setSystemId(p.getProperty("r3name"));
        if (p.containsKey("systemNumber")) {
            c.setSystemNumber(p.getProperty("systemNumber"));
        }
        c.setClient(p.getProperty("client"));
        if (p.containsKey("lang")) {
            c.setLang(p.getProperty("lang"));
        }
        if (p.containsKey("sncMode")) {
            c.setSncMode(p.getProperty("sncMode"));
        }
        if (p.containsKey("sncMyName")) {
            c.setSncMyName(p.getProperty("sncMyName"));
        }
        if (p.containsKey("sncLibrary")) {
            c.setSncLibrary(p.getProperty("sncLibrary"));
        }
        if (p.containsKey("sncPartnerName")) {
            c.setSncPartnerName(p.getProperty("sncPartnerName"));
        }
        if (p.containsKey("sncQoP")) {
            c.setSncQoP(p.getProperty("sncQoP"));
        }
        return c;
    }
}
