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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Live midPoint-side tests for the SAP connector.
 * <p>
 * Each test creates a concrete SAP resource that inherits the deploy-time resource template (so the
 * connection settings stay single-sourced in {@code test.properties} / the template) and adds its own
 * table settings, then asserts the resulting behaviour through midPoint's REST API: the object classes
 * that show up in the generated resource schema and the objects they return from SAP.
 * <p>
 * These exercise the whole stack (midPoint -> connector -> JCo -> SAP), so they need the {@code docker/}
 * test rig (see {@code docker/README.md}):
 * <ul>
 *   <li>a running midPoint with the SAP connector + JCo deployed and the resource template imported
 *       ({@code docker/deploy-connector.sh} does all of this), and</li>
 *   <li>that midPoint able to reach the SAP system configured in the template.</li>
 * </ul>
 * When midPoint is not reachable or the template is missing the tests are skipped (JUnit assumptions),
 * so the suite stays green without the rig.
 * <p>
 * Cleanup is done at the START of a run, not the end: {@code @BeforeAll} purges resources left by a
 * previous run (those named with {@link #RESOURCE_NAME_PREFIX}). The resources a run creates are left in
 * midPoint afterwards, so you can inspect and test them manually in the GUI; the next run removes them. For
 * a fully pristine midPoint (also wiping the connector/template), recreate the rig - see {@code docker/README.md}.
 * <p>
 * The midPoint endpoint and credentials default to the docker rig and can be overridden in
 * {@code test.properties}: {@code midpoint.url}, {@code midpoint.user}, {@code midpoint.password},
 * {@code midpoint.templateOid}.
 */
public class SapResourceLiveTest {

    private static final Log LOG = Log.getLog(SapResourceLiveTest.class);

    private static final String NS_COMMON = "http://midpoint.evolveum.com/xml/ns/public/common/common-3";
    private static final String NS_ICFC = "http://midpoint.evolveum.com/xml/ns/public/connector/icf-1/connector-schema-3";
    private static final String NS_QUERY = "http://prism.evolveum.com/xml/ns/public/query-3";
    private static final String NS_RI = "http://midpoint.evolveum.com/xml/ns/public/resource/instance-3";
    private static final String NS_CFG =
            "http://midpoint.evolveum.com/xml/ns/public/connector/icf-1/bundle/"
                    + "com.evolveum.polygon.connector-sap/com.evolveum.polygon.connector.sap.SapConnector";

    private static final String DEFAULT_TEMPLATE_OID = "f698ab61-55f4-4eec-bba4-81da4b9f52d8";

    /** Resources created by these tests share this name prefix so they can be purged before a re-run. */
    private static final String RESOURCE_NAME_PREFIX = "zz-test-sap-";

    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static String restUrl;       // .../midpoint/ws/rest
    private static String authHeader;    // Basic ...
    private static String templateOid;
    private static boolean rigAvailable;

    @BeforeAll
    static void setUp() {
        Properties p = loadProperties("test.properties");
        String url = prop(p, "midpoint.url", "http://localhost:11080/midpoint");
        String user = prop(p, "midpoint.user", "administrator");
        String password = prop(p, "midpoint.password", "T3stPw890uio");
        templateOid = prop(p, "midpoint.templateOid", DEFAULT_TEMPLATE_OID);

        restUrl = url.replaceAll("/+$", "") + "/ws/rest";
        authHeader = "Basic " + Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));

        // The template object merely being fetchable proves midPoint is up - it does NOT prove SAP is
        // reachable. Gate availability in two steps so that an unreachable SAP system skips the live tests
        // instead of failing every test connection assertion (which would turn a missing environment into a
        // BUILD FAILURE rather than a handful of skipped tests).
        if (!templateResourcePresent()) {
            rigAvailable = false;
            LOG.info("midPoint at {0} not reachable or template {1} missing - midPoint resource tests will be skipped",
                    restUrl, templateOid);
            return;
        }
        // Clean up resources from a PREVIOUS run now, at the start. We deliberately do NOT delete them
        // afterwards, so the resources this run creates stay in midPoint for manual inspection/testing.
        purgeTestResources();
        // Second step: confirm SAP itself answers through the connector before declaring the rig available.
        rigAvailable = sapReachableViaTemplate();
        if (!rigAvailable) {
            LOG.info("midPoint at {0} is up but SAP is not reachable (template test connection failed) -"
                    + " midPoint resource tests will be skipped", restUrl);
        }
    }

    /**
     * Configuring a {@code <cfg:tables>} alias against a SAP table the connector user cannot read
     * (or that does not exist at all) must cause the resource's test connection to fail with a
     * clear, human-readable message that names the unreachable table - not a green-light test
     * connection followed by an error on first search. Exercises the per-table read probe added
     * to {@code SapConnector#test()} alongside the configured {@code tableReadFunction} check and
     * the DD03L probe.
     */
    @Test
    public void testConnectionFailsWhenAConfiguredTableIsUnreadable() throws Exception {
        assumeTrue(rigAvailable, "midPoint test rig not available - skipping");

        // A SAP table that does not exist - the probe must fail and surface that name in the message.
        String bogusTable = "ZZNONEXISTENT_TBL_FOR_TESTS";
        String oid = createTemplateBasedResource("zz-test-sap-bad-table",
                bogusTable + " as BOGUS");
        java.net.http.HttpResponse<String> r = send("POST", "/resources/" + oid + "/test", null, null);
        assertEquals(200, r.statusCode(),
                "resource test should respond with HTTP 200 even when the test result is fatal_error;"
                        + " HTTP " + r.statusCode() + " body=" + r.body());
        String status = xpathString(parse(r.body()), "/*/*[local-name()='status'][1]");
        assertFalse("success".equals(status),
                "test connection must NOT report success when a configured table is unreadable;"
                        + " got status=" + status);
        String body = r.body();
        assertTrue(body.contains(bogusTable),
                "test result must mention the unreadable table name '" + bogusTable + "'"
                        + " in the diagnostic message; got: " + body);
    }

    /**
     * A table defined as {@code AGR_TEXTS as AUDITROLES WHERE AGR_NAME LIKE 'SAP_AUDITOR%' AND SPRAS = 'D'
     * AND LINE = '00000'} must (a) produce an AUDITROLES object class in the generated resource schema and
     * (b) return only the rows matching that WHERE clause (SAP_AUDITOR* roles, German short-description line).
     */
    @Test
    public void auditRolesObjectTypeIsGeneratedAndFiltered() throws Exception {
        assumeTrue(rigAvailable, "midPoint test rig not available - skipping");

        String tables = "AGR_TEXTS as AUDITROLES WHERE AGR_NAME LIKE 'SAP_AUDITOR%' AND SPRAS = 'D' and LINE = '00000'";
        String oid = createTemplateBasedResource("zz-test-sap-auditroles", tables);
        testResource(oid);

        // (a) the AUDITROLES object type shows up in the generated resource schema
        TreeSet<String> objectClasses = generatedObjectClasses(oid);
        assertTrue(objectClasses.contains("CustomAUDITROLESObjectClass"),
                "resource schema must contain the AUDITROLES object class, got: " + objectClasses);

        // (b) querying it returns only the rows the WHERE clause selects
        List<String> names = searchObjectNames(oid, "ri:CustomAUDITROLESObjectClass");
        LOG.info("AUDITROLES returned {0} object(s)", names.size());
        assertFalse(names.isEmpty(),
                "expected at least one SAP_AUDITOR* role on the test system, but the query returned none "
                        + "(connection problem or the WHERE clause did not match)");
        for (String name : names) {
            assertTrue(name.startsWith("SAP_AUDITOR"),
                    "WHERE clause not applied: '" + name + "' does not match AGR_NAME LIKE 'SAP_AUDITOR%'");
            assertTrue(name.endsWith(":D:00000"),
                    "WHERE clause not applied: '" + name + "' is not the German (SPRAS=D) first line (LINE=00000)");
        }
    }

    /**
     * The same SAP table must be usable for several object types. Maps AGR_DEFINE twice, with different
     * WHERE clauses, and checks that both object classes are generated and that each applies its own
     * filter independently (the narrower one is a strict subset of the broader one).
     */
    @Test
    public void sameTableCanBackMultipleObjectTypes() throws Exception {
        assumeTrue(rigAvailable, "midPoint test rig not available - skipping");

        String oid = createTemplateBasedResource("zz-test-sap-multialias",
                "AGR_DEFINE as AUDITROLES WHERE AGR_NAME LIKE 'SAP_AUDITOR%'",
                "AGR_DEFINE as AUDITADMINROLES WHERE AGR_NAME LIKE 'SAP_AUDITOR_ADMIN%'");
        testResource(oid);

        // both object classes are generated from the same SAP table
        Set<String> objectClasses = generatedObjectClasses(oid);
        assertTrue(objectClasses.contains("CustomAUDITROLESObjectClass"),
                "missing AUDITROLES object class: " + objectClasses);
        assertTrue(objectClasses.contains("CustomAUDITADMINROLESObjectClass"),
                "the same SAP table did not yield a second, independent object class: " + objectClasses);

        List<String> auditRoles = searchObjectNames(oid, "ri:CustomAUDITROLESObjectClass");
        List<String> auditAdmin = searchObjectNames(oid, "ri:CustomAUDITADMINROLESObjectClass");
        LOG.info("same table AGR_DEFINE -> AUDITROLES={0}, AUDITADMINROLES={1}", auditRoles.size(), auditAdmin.size());

        // each object type applies its own WHERE
        assertFalse(auditRoles.isEmpty(), "expected SAP_AUDITOR* roles on the test system");
        assertFalse(auditAdmin.isEmpty(), "expected SAP_AUDITOR_ADMIN* roles on the test system");
        for (String name : auditRoles) {
            assertTrue(name.startsWith("SAP_AUDITOR"), "AUDITROLES WHERE not applied: " + name);
        }
        for (String name : auditAdmin) {
            assertTrue(name.startsWith("SAP_AUDITOR_ADMIN"), "AUDITADMINROLES WHERE not applied: " + name);
        }
        // the two filters are independent: the narrower object type is a strict subset of the broader one
        assertTrue(new TreeSet<>(auditRoles).containsAll(auditAdmin),
                "the narrower object type must be a subset of the broader one (both read AGR_DEFINE)");
        assertTrue(auditRoles.size() > auditAdmin.size(),
                "the two WHERE clauses on the same table must select different counts (got "
                        + auditRoles.size() + " and " + auditAdmin.size() + ")");
    }

    /**
     * A sub-table must work in RFC_READ_TABLE mode: it joins related rows of another SAP table to each root
     * object and exposes them as a multi-valued attribute. Here AGR_TEXTS is joined to a few roles on AGR_NAME
     * (filtered to the German short-description line) and returned as the ShortDescription attribute.
     */
    @Test
    public void subTableExposesRelatedRowsAsAttribute() throws Exception {
        assumeTrue(rigAvailable, "midPoint test rig not available - skipping");

        // a narrow root (a handful of roles) keeps the per-root sub-query count - and the test - small
        String config =
                "            <cfg:tables>"
                        + xmlText("AGR_DEFINE as AUDITADMINROLES WHERE AGR_NAME LIKE 'SAP_AUDITOR_ADMIN%'")
                        + "</cfg:tables>\n"
                + "            <cfg:subTables>"
                        + xmlText("AGR_TEXTS for AUDITADMINROLES format TSV as ShortDescription="
                                + "AGR_NAME:MATCH,SPRAS(\"D\"):IGNORE,LINE(\"00000\"):IGNORE,TEXT")
                        + "</cfg:subTables>\n";
        String oid = createResourceWithConfig("zz-test-sap-subtable", config);
        testResource(oid);

        // the sub-table is exposed as an attribute on the object class
        assertTrue(generatedObjectClasses(oid).contains("CustomAUDITADMINROLESObjectClass"),
                "missing AUDITADMINROLES object class");

        List<String> roles = searchObjectNames(oid, "ri:CustomAUDITADMINROLESObjectClass");
        List<String> descriptions = searchAttributeValues(oid, "ri:CustomAUDITADMINROLESObjectClass", "ShortDescription");
        LOG.info("AUDITADMINROLES roles={0}, ShortDescriptions={1}", roles.size(), descriptions.size());

        assertFalse(roles.isEmpty(), "expected at least one SAP_AUDITOR_ADMIN* role on the test system");
        assertFalse(descriptions.isEmpty(),
                "the AGR_TEXTS sub-table returned no ShortDescription - the join or filter did not match");
        for (String description : descriptions) {
            assertFalse(description.isBlank(), "ShortDescription from the sub-table should not be blank");
        }
    }

    /**
     * A sub-table can express its join through the WHERE clause by referencing a root field as
     * {@code <rootTable>.<field>} (which also allows joining differently-named fields). Here AGR_TEXTS is
     * joined with {@code WHERE AGR_NAME = AUDITADMINROLES.AGR_NAME} instead of an AGR_NAME:MATCH column.
     */
    @Test
    public void subTableCanJoinThroughWhereRootReference() throws Exception {
        assumeTrue(rigAvailable, "midPoint test rig not available - skipping");

        String config =
                "            <cfg:tables>"
                        + xmlText("AGR_DEFINE as AUDITADMINROLES WHERE AGR_NAME LIKE 'SAP_AUDITOR_ADMIN%'")
                        + "</cfg:tables>\n"
                + "            <cfg:subTables>"
                        + xmlText("AGR_TEXTS for AUDITADMINROLES format TSV as ShortDescription=TEXT "
                                + "WHERE AGR_NAME = AUDITADMINROLES.AGR_NAME AND SPRAS = 'D' AND LINE = '00000'")
                        + "</cfg:subTables>\n";
        String oid = createResourceWithConfig("zz-test-sap-subtable-ref", config);
        testResource(oid);

        List<String> descriptions = searchAttributeValues(oid, "ri:CustomAUDITADMINROLESObjectClass", "ShortDescription");
        LOG.info("WHERE-ref sub-table ShortDescriptions={0}", descriptions.size());
        assertFalse(descriptions.isEmpty(),
                "the <root>.<field> WHERE join returned no ShortDescription - the substitution did not match");
        for (String description : descriptions) {
            assertFalse(description.isBlank(), "ShortDescription from the WHERE-ref sub-table should not be blank");
        }
    }

    // --- resource lifecycle helpers --------------------------------------------------------------

    /**
     * Creates a concrete resource that inherits the template (connection + defaults) and adds the given
     * {@code tables} definitions. Returns the new OID.
     */
    private String createTemplateBasedResource(String name, String... tablesDefs) throws Exception {
        StringBuilder cfg = new StringBuilder();
        for (String t : tablesDefs) {
            cfg.append("            <cfg:tables>").append(xmlText(t)).append("</cfg:tables>\n");
        }
        return createResourceWithConfig(name, cfg.toString());
    }

    /**
     * Creates a concrete resource inheriting the template, with the given raw {@code <cfg:...>} body. The
     * resource keeps a stable name (so it is easy to find in the GUI afterwards) and is left in midPoint
     * after the run; the next run purges it by its {@link #RESOURCE_NAME_PREFIX} name.
     */
    private String createResourceWithConfig(String name, String configProperties) throws Exception {
        String oid = UUID.randomUUID().toString();
        String body = "<resource xmlns=\"" + NS_COMMON + "\" xmlns:c=\"" + NS_COMMON + "\" oid=\"" + oid + "\">\n"
                + "    <name>" + xmlText(name) + "</name>\n"
                + "    <super><resourceRef oid=\"" + templateOid + "\"/></super>\n"
                + "    <connectorConfiguration xmlns:icfc=\"" + NS_ICFC + "\">\n"
                + "        <icfc:configurationProperties xmlns:cfg=\"" + NS_CFG + "\">\n"
                + configProperties
                + "        </icfc:configurationProperties>\n"
                + "    </connectorConfiguration>\n"
                + "</resource>\n";

        HttpResponse<String> r = send("POST", "/resources", body, "application/xml");
        assertEquals(201, r.statusCode(), "creating resource failed: HTTP " + r.statusCode() + " - " + r.body());
        return oid;
    }

    /** Runs the resource test connection (which also generates and stores the schema) and asserts success. */
    private void testResource(String oid) throws Exception {
        HttpResponse<String> r = send("POST", "/resources/" + oid + "/test", null, null);
        assertEquals(200, r.statusCode(), "resource test connection failed: HTTP " + r.statusCode() + " - " + r.body());
        String status = xpathString(parse(r.body()), "/*/*[local-name()='status'][1]");
        assertEquals("success", status, "resource test connection did not succeed: " + r.body());
    }

    /** Object class names (xsd complexType names) in the resource's generated schema. */
    private TreeSet<String> generatedObjectClasses(String oid) throws Exception {
        HttpResponse<String> r = send("GET", "/resources/" + oid, null, null);
        assertEquals(200, r.statusCode(), "fetching resource failed: HTTP " + r.statusCode());
        NodeList names = xpathNodes(parse(r.body()), "//*[local-name()='complexType']/@name");
        TreeSet<String> result = new TreeSet<>();
        for (int i = 0; i < names.getLength(); i++) {
            result.add(names.item(i).getNodeValue());
        }
        return result;
    }

    /** Searches the resource for the given object class (live, on SAP) and returns the parsed shadow list. */
    private Document searchShadows(String resourceOid, String objectClassQName) throws Exception {
        String query = "<query xmlns=\"" + NS_QUERY + "\" xmlns:c=\"" + NS_COMMON + "\" xmlns:ri=\"" + NS_RI + "\">\n"
                + "  <filter>\n"
                + "    <and>\n"
                + "      <ref><path>resourceRef</path><value oid=\"" + resourceOid + "\"/></ref>\n"
                + "      <equal><path>objectClass</path><value>" + objectClassQName + "</value></equal>\n"
                + "    </and>\n"
                + "  </filter>\n"
                + "</query>\n";
        HttpResponse<String> r = send("POST", "/shadows/search", query, "application/xml");
        assertEquals(200, r.statusCode(), "shadow search failed: HTTP " + r.statusCode() + " - " + r.body());
        return parse(r.body());
    }

    /** The shadow names returned by the object-class search. */
    private List<String> searchObjectNames(String resourceOid, String objectClassQName) throws Exception {
        return textNodes(searchShadows(resourceOid, objectClassQName),
                "//*[local-name()='object']/*[local-name()='name']/text()");
    }

    /** Values of the given resource attribute (local name) across all shadows from the search. */
    private List<String> searchAttributeValues(String resourceOid, String objectClassQName, String attribute) throws Exception {
        return textNodes(searchShadows(resourceOid, objectClassQName),
                "//*[local-name()='attributes']/*[local-name()='" + attribute + "']/text()");
    }

    private static List<String> textNodes(Document doc, String expr) throws Exception {
        NodeList nodes = xpathNodes(doc, expr);
        List<String> values = new ArrayList<>();
        for (int i = 0; i < nodes.getLength(); i++) {
            values.add(nodes.item(i).getNodeValue());
        }
        return values;
    }

    // --- low-level REST + XML helpers ------------------------------------------------------------

    private static boolean templateResourcePresent() {
        try {
            return send("GET", "/resources/" + templateOid, null, null).statusCode() == 200;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Probes whether SAP itself is reachable through midPoint - not just that midPoint is up. Creates a
     * throwaway resource that inherits the template (so it carries the full SAP connection config), runs its
     * test connection, and reports whether that connection succeeded. The probe resource is deleted again
     * immediately; should the delete fail it keeps the {@link #RESOURCE_NAME_PREFIX} so the next run purges it.
     * Best-effort: any failure (midPoint refusing the create, SAP unreachable, a malformed result) yields
     * false, which makes the live tests skip rather than fail.
     */
    private static boolean sapReachableViaTemplate() {
        String oid = UUID.randomUUID().toString();
        String body = "<resource xmlns=\"" + NS_COMMON + "\" xmlns:c=\"" + NS_COMMON + "\" oid=\"" + oid + "\">\n"
                + "    <name>" + RESOURCE_NAME_PREFIX + "reachability-probe</name>\n"
                + "    <super><resourceRef oid=\"" + templateOid + "\"/></super>\n"
                + "    <connectorConfiguration xmlns:icfc=\"" + NS_ICFC + "\">\n"
                + "        <icfc:configurationProperties xmlns:cfg=\"" + NS_CFG + "\"/>\n"
                + "    </connectorConfiguration>\n"
                + "</resource>\n";
        try {
            HttpResponse<String> create = send("POST", "/resources", body, "application/xml");
            if (create.statusCode() != 201) {
                LOG.warn("reachability probe: creating probe resource returned HTTP {0} - treating rig as unavailable: {1}",
                        create.statusCode(), create.body());
                return false;
            }
        } catch (Exception e) {
            LOG.warn("reachability probe: creating probe resource failed: {0}", e);
            return false;
        }
        try {
            HttpResponse<String> test = send("POST", "/resources/" + oid + "/test", null, null);
            if (test.statusCode() != 200) {
                return false;
            }
            return "success".equals(xpathString(parse(test.body()), "/*/*[local-name()='status'][1]"));
        } catch (Exception e) {
            LOG.warn("reachability probe: SAP test connection failed: {0}", e);
            return false;
        } finally {
            try {
                send("DELETE", "/resources/" + oid, null, null);
            } catch (Exception e) {
                LOG.warn("reachability probe: deleting probe resource {0} failed (next run will purge it): {1}", oid, e);
            }
        }
    }

    /**
     * Deletes resources created by a previous run (those named with {@link #RESOURCE_NAME_PREFIX}), leaving
     * the template and anything else untouched. Best-effort: failures are logged, not fatal.
     */
    private static void purgeTestResources() {
        try {
            HttpResponse<String> r = send("POST", "/resources/search", "<query xmlns=\"" + NS_QUERY + "\"/>", "application/xml");
            if (r.statusCode() != 200) {
                LOG.warn("purge: listing resources returned HTTP {0} - skipping pre-run cleanup", r.statusCode());
                return;
            }
            NodeList resources = xpathNodes(parse(r.body()), "//*[local-name()='object'][@oid]");
            for (int i = 0; i < resources.getLength(); i++) {
                org.w3c.dom.Node node = resources.item(i);
                String oid = node.getAttributes().getNamedItem("oid").getNodeValue();
                NodeList nameNodes = (NodeList) XPathFactory.newInstance().newXPath()
                        .evaluate("./*[local-name()='name']/text()", node, XPathConstants.NODESET);
                String name = nameNodes.getLength() > 0 ? nameNodes.item(0).getNodeValue() : "";
                if (name.startsWith(RESOURCE_NAME_PREFIX)) {
                    HttpResponse<String> d = send("DELETE", "/resources/" + oid, null, null);
                    LOG.info("purge: deleted leftover test resource {0} ({1}) -> HTTP {2}", name, oid, d.statusCode());
                }
            }
        } catch (Exception e) {
            LOG.warn("purge: pre-run cleanup failed: {0}", e);
        }
    }

    private static HttpResponse<String> send(String method, String path, String body, String contentType) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(restUrl + path))
                .header("Authorization", authHeader)
                .header("Accept", "application/xml");
        if (body == null) {
            b.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            b.header("Content-Type", contentType)
                    .method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        }
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static Document parse(String xml) throws Exception {
        DocumentBuilderFactory f = DocumentBuilderFactory.newInstance();
        f.setNamespaceAware(true);
        return f.newDocumentBuilder().parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    private static String xpathString(Document doc, String expr) throws Exception {
        return (String) XPathFactory.newInstance().newXPath().evaluate(expr, doc, XPathConstants.STRING);
    }

    private static NodeList xpathNodes(Document doc, String expr) throws Exception {
        return (NodeList) XPathFactory.newInstance().newXPath().evaluate(expr, doc, XPathConstants.NODESET);
    }

    private static String xmlText(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static Properties loadProperties(String fileName) {
        try (InputStream in = SapResourceLiveTest.class.getClassLoader().getResourceAsStream(fileName)) {
            if (in == null) {
                return null;
            }
            Properties p = new Properties();
            p.load(in);
            return p;
        } catch (Exception e) {
            return null;
        }
    }

    private static String prop(Properties p, String key, String def) {
        if (p == null) {
            return def;
        }
        String v = p.getProperty(key);
        return (v == null || v.isBlank()) ? def : v.trim();
    }
}
