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

import com.sap.conn.jco.JCoException;
import org.identityconnectors.common.logging.Log;

/**
 * Shared helpers for the live tests that talk to a real SAP system directly (everything except the
 * midPoint-mediated {@link SapResourceLiveTest}, which has its own REST-based reachability probe).
 * <p>
 * The live tests only run when a {@code test.properties} is present <em>and</em> the SAP system it points
 * at is actually reachable. The presence of {@code test.properties} alone does not imply SAP is up, so this
 * class centralises the "is SAP reachable?" probe and the classification of a connection failure: an
 * unreachable SAP makes the live tests skip instead of failing every connection attempt, which would turn a
 * missing environment into a BUILD FAILURE rather than a handful of skipped tests.
 */
final class SapLiveTestSupport {

    private static final Log LOG = Log.getLog(SapLiveTestSupport.class);

    private SapLiveTestSupport() {
    }

    /**
     * True when the throwable chain carries a JCo communication error ({@link JCoException#JCO_ERROR_COMMUNICATION}),
     * i.e. SAP could not be reached at all (host down, gateway refused the connection, name resolution failed).
     * Such failures are an environment problem, not a connector defect. Other JCo error groups (system failure,
     * permission, ...) indicate a reachable-but-unhappy SAP and are deliberately NOT matched, so they still fail
     * the test loudly instead of being silently skipped.
     */
    static boolean isSapUnreachable(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c instanceof JCoException && ((JCoException) c).getGroup() == JCoException.JCO_ERROR_COMMUNICATION) {
                return true;
            }
        }
        return false;
    }

    /**
     * Probes whether the SAP system described by {@code config} can be reached, by running a connection-only
     * {@code test()} (a JCo {@code ping}) on a throwaway connector. Returns {@code true} when the ping succeeds
     * and {@code false} on a communication failure (SAP unreachable). Any other failure is rethrown, so a
     * reachable-but-misconfigured SAP still surfaces as a real error rather than being skipped.
     * <p>
     * BAPI / function-module / table permission probing is disabled for the duration of the check (and restored
     * afterwards) so the result reflects pure connectivity, not the resource's configured object classes. The
     * probe connector is disposed when done; {@code dispose()} does not unregister the shared JCo destination
     * provider, so it does not disturb connectors created by the actual tests.
     */
    static boolean sapReachable(SapConfiguration config) {
        Boolean previousBapiProbe = config.getTestBapiFunctionPermission();
        config.setTestBapiFunctionPermission(Boolean.FALSE);
        SapConnector connector = new SapConnector();
        try {
            connector.init(config);
            connector.test();
            return true;
        } catch (RuntimeException e) {
            if (isSapUnreachable(e)) {
                LOG.info("SAP not reachable ({0}) - live SAP tests will be skipped", e.getMessage());
                return false;
            }
            throw e;
        } finally {
            config.setTestBapiFunctionPermission(previousBapiProbe);
            try {
                connector.dispose();
            } catch (RuntimeException e) {
                LOG.warn("connector.dispose() after the reachability probe failed: {0}", e);
            }
        }
    }
}
