/*
 * Copyright (c) 2010-2016 Evolveum
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

import com.sap.conn.jco.JCo;
import com.sap.conn.jco.ext.DestinationDataProvider;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.identityconnectors.common.StringUtil.isBlank;
import static org.identityconnectors.common.StringUtil.isNotEmpty;

public class SapConfiguration extends AbstractConfiguration {

    private static final String BASE_ACCOUNT_QUERY_SEPARATOR = ",";

	private static final Log LOG = Log.getLog(SapConfiguration.class);

    private Boolean loadBalancing = false;

    private String host;

    private String port = "3334";

    private String user;

    private GuardedString password;

    private String logonGroup = "SPACE"; // default logon group shipped with every SAP NW application server

    /**
     * r3Name in SAP
     */
    private String systemId;

    private String systemNumber = "00";

    private String client;

    private String destinationName;

    private String lang = "EN";

    private String poolCapacity = "1";

    private String peakLimit = "0";

    private String sncLibrary = null;

    private String sncMode = "0";

    public static final String SNC_MODE_ON = "1";

    private String sncMyName = null;

    private String sncPartnerName = null;

    private String sncQoP = null;

    private String x509Cert = null;

    private String cpicTrace = null;

    private String trace = null;

    private String traceLevel = "0";
    
    private String tracePath = null;

    /**
     * throw exception when data will be truncated in SAP and not fit to actual atribute size
     */
    private Boolean failWhenTruncating = true;

    /**
     * throw exception when SAP return a warning message after BAPI call
     */
    private Boolean failWhenWarning = true;

    /**
     * use transaction in SAP when create/update
     */
    private Boolean useTransaction = true;

    /**
     * test BAPI functions visibility when test connection
     */
    private Boolean testBapiFunctionPermission = true;

    /**
     * if true we use SUSR_USER_CHANGE_PASSWORD_RFC BAPI to change password and user don't need to change his password at next logon
     */
    private Boolean changePasswordAtNextLogon = false;

    /**
     * if true, read also login information over SUSR_GET_ADMIN_USER_LOGIN_INFO
     */
    private Boolean alsoReadLoginInfo = false;

    /**
     * newer ConnId framework support native names instead of icfs:UID & icfs:NAME
     */
    private Boolean useNativeNames = false;

    /**
     * if this is true every activitygroup which is assigned to a accounts will be hidden from the result object
     */
    private Boolean hideIndirectActivitygroups = false;

    /**
     * Default config:
     * Because the AGR_NAME field only stores AGR_NAMEs it has no information on the current from/to dates in the actual
     * activitygroups XML field and changes to AGR_NAMES will simply overwrite everything in the SAP with empty from/to
     * values.
     * SAP generates the current date as a from date, so if you have a role change (AGR_NAME) on another date, every
     * role in the SAP will be deleted and added again. All the information about the actual date of assignment will be lost.
     * This can generate a lot of audit entries in the SAP.
     *<br/>
     * Enable this configuration to merge changes to AGR_NAME with the actual values inside ACTIVITYGROUPS in the SAP.
     * The current state of the user will be loaded from SAP and the existing data will be used if nothing has been
     * provided by midpoint. To propagate actual changes in the ACTIVITYGROUPS from/to fields the change algorithm will
     * check if the attributes from the midpoint are xml attributes. If yes it will simply use those values because they
     * have the best accuracy. If not it will use the current data from the SAP for those values.
     *
     */
    private Boolean mergeAgrNameWithExistingAcitivitygroupsValue = false;

    /**
     * definition of any tables in SAP to read his data, for example:
     * * AGR_DEFINE as ACTIVITYGROUP - AGR_DEFINE is table name in SAP, ACTIVITYGROUP is his alias in connector
     * * MANDT:3:IGNORE - MANDT is his first column with length 3 and in connector is ignored
     * * AGR_NAME:30:KEY - AGR_NAME is his second column with length 30 and is his key (icfs:UID and also his icfs:NAME)
     * * PARENT_AGR:30 - PARENT_AGR is his third column with length 30 characters
     * * next columns are ignored
     */
    private String[] tables = {"AGR_DEFINE as ACTIVITYGROUP=MANDT:3:IGNORE,AGR_NAME:30:KEY,PARENT_AGR:30", "USGRP as GROUP=MANDT:3:IGNORE,USERGROUP:12:KEY"};

    /**
     * SAP function module used to read the tables defined above.
     * <ul>
     *     <li>{@code RFC_GET_TABLE_ENTRIES} (default) keeps the legacy behaviour: each table line must
     *         use the positional {@code col:len[:KEY|:IGNORE]} syntax and lengths are mandatory.</li>
     *     <li>{@code RFC_READ_TABLE} (or {@code BBP_RFC_READ_TABLE} / a custom Z-FM with the same
     *         OPTIONS/FIELDS/DATA/ET_DATA interface) enables the self-describing path: column lengths
     *         are read from SAP, keys default to the DDIC primary key (MANDT excluded), an optional
     *         trailing {@code WHERE <clause>} is passed as OPTIONS, and ET_DATA / server-side sort are
     *         used automatically when the target system supports them.</li>
     * </ul>
     * Legacy table lines keep working when switching to RFC_READ_TABLE (lengths are ignored, :KEY becomes
     * a key override, :IGNORE still drops the column). The reverse switch may break, because RFC_READ_TABLE
     * lines do not have to carry lengths.
     */
    public static final String FN_GET_TABLE_ENTRIES = "RFC_GET_TABLE_ENTRIES";
    public static final String FN_READ_TABLE = "RFC_READ_TABLE";
    public static final String FN_BBP_READ_TABLE = "BBP_RFC_READ_TABLE";

    private String tableReadFunction = FN_GET_TABLE_ENTRIES;

    /** matches the optional trailing " WHERE <clause>" in a table definition (RFC_READ_TABLE mode) */
    private static final Pattern TABLE_WHERE_PATTERN = Pattern.compile("(?i)\\s+WHERE\\s+");

    /**
     * Defines additional tables, that should be queried for each table result.
     * <br/>
     * The {@code for <rootAlias>} part references the object class (the "tables" alias), not the SAP table
     * name, so a sub-table attaches to exactly one object type even when several share a SAP table. With
     * {@code tables = AGR_DEFINE as ACTIVITYGROUP} the sub-table reads {@code ... for ACTIVITYGROUP ...}.
     * <br/>
     * In RFC_READ_TABLE mode (see {@code tableReadFunction}) the same definitions are reused, but the
     * fixed-width {@code :<size>} of each column is ignored (data is read by field name, not by offset),
     * so you only need the MATCH and filter columns plus the OUTPUT columns; an optional trailing
     * {@code WHERE <clause>} may be appended for extra filtering. The MATCH columns become a server-side
     * join on the root row value and the {@code ("value")} filter constants become WHERE equalities, e.g.
     * {@code AGR_TEXTS for ACTIVITYGROUP format TSV as ShortDescription=AGR_NAME:MATCH,SPRAS("E"):IGNORE,LINE("00000"):IGNORE,TEXT}.
     * <br/>
     * Since a {@code ("value")} filter is just a static condition, you can equivalently write it in the
     * trailing WHERE instead, in which case the filtered columns need not be listed at all - the example
     * above is the same as
     * {@code AGR_TEXTS for ACTIVITYGROUP format TSV as ShortDescription=AGR_NAME:MATCH,TEXT WHERE SPRAS = 'E' AND LINE = '00000'}.
     * <br/>
     * The WHERE may also reference a root field as {@code <rootAlias>.<field>}; it is replaced with that
     * field's value from the current root row before the sub-query runs. This expresses the join directly
     * and, unlike a MATCH column, works across fields with different names in the two tables (SAP often
     * names the same logical content differently, e.g. VALIDFROM vs BEGDA or PERNR vs OBJID), e.g.
     * {@code AGR_TEXTS for ACTIVITYGROUP format TSV as ShortDescription=TEXT WHERE AGR_NAME = ACTIVITYGROUP.AGR_NAME AND SPRAS = 'E' AND LINE = '00000'}
     * or, for a root {@code HRP1000 as ORGUNITS}, {@code HRP1001 for ORGUNITS ... =STEXT WHERE OBJID = ORGUNITS.PERNR}.
     * <br/>
     * Each config item has this pattern:
     * <br/>
     * {@code <tableDefinition>=<columnDefinition>[;<columnDefinition>[...]]}
     * <br/>
     * The {@code tableDefinition} uses this pattern:
     * {@code <tableName> for <rootAlias>[ format <formatType>][ as <virtualColumnName>]}
     * <br/>
     * The {@code columnDefinition} uses this pattern:
     * {@code <columnName>:<size>[("<filterValue>")][:<syncMode>]}
     * <br/>
     * <br/>
     * These restrictions apply for the table definition:
     * <ul>
     *     <li>{@code tableName}: name of a SAP table</li>
     *     <li>{@code rootAlias}: the object class (the alias from the "tables" configuration) whose objects
     *         this sub-table is attached to</li>
     *     <li>{@code formatType}:
     *         <ul>
     *             <li>{@code XML} (default if unset; each column name will be returned as individual XML tag)</li>
     *             <li>{@code TSV} (TAB-separated values)</li>
     *         </ul>
     *     </li>
     *     <li>{@code virtualColumnName}: name of the ConnId attribute in the containing ObjectClass, that will contain the query sub-result</li>
     * </ul>
     * These restrictions apply for the column definition:
     * <ul>
     *     <li>{@code columnName}: should be a column name of the SAP table, but is manly used to structure the output</li>
     *     <li>{@code size}: number of characters of this column</li>
     *     <li>{@code filterValue}: can be used to select only rows with a predefined constant value. The value has to be enclosed by ("...")</li>
     *     <li>{@code syncMode}:
     *         <ul>
     *             <li>{@code OUTPUT} (default if unset; will include the column in the result)</li>
     *             <li>{@code IGNORE} (will ignore the column)</li>
     *             <li>{@code MATCH} (value has to match with the identically named column in the root result; This should be specified for the key column because of prefix-matching issues in the BAPI call. To join on a differently named root field, reference it in the WHERE as {@code <rootAlias>.<field>} instead - RFC_READ_TABLE only)</li>
     *         </ul>
     *     </li>
     * </ul>
     * <br/>
     * This is an example to load the short description of an ActivityGroup:
     * <br/>
     * {@code AGR_TEXTS for ACTIVITYGROUP format TSV as ShortDescription=MANDT:3:IGNORE,AGR_NAME:30:MATCH,SPRAS:1("E"):IGNORE,LINE:5("00000"):IGNORE,TEXT:80}
     * <br/>
     * This queries the row with language "E" (english) and line number "00000" of AGR_TEXTS, selects the TEXT column
     * and returns it as ConnId attribute "ShortDescription" (by SAP convention, the line 0 represents the short description
     * of a role, all following lines combined are the long description).
     * <br/>
     * <br/>
     * This is an example to load the names of all single roles, that are referenced by a composite role:
     * <br/>
     * {@code AGR_AGRS for ACTIVITYGROUP format TSV=MANDT:3:IGNORE,AGR_NAME:30:MATCH,CHILD_AGR:30,ATTRIBUTES:10:IGNORE}
     * <br/>
     * <br/>
     * This is an example to load all role flags as XML (role flags include for example the value COLL_AGR="X" for composite roles):
     * <br/>
     * {@code AGR_FLAGS for ACTIVITYGROUP format XML=MANDT:3:IGNORE,AGR_NAME:30:MATCH,FLAG_TYPE:10,MISC:52:IGNORE,FLAG_VALUE:32}
     */
    private String[] subTables = {};

    /**
     * this tables also have a simplified representation of his tables over his keys to comfortable use in mappings, for example ACTIVITYGROUPS.AGR_NAME
     */
    private String[] tableParameterNames = {"PROFILES", "ACTIVITYGROUPS", "GROUPS"};

    /**
     * this params are set as read only params for the connection to restrict what could be read , for example ISLOCKED", "LASTMODIFIED", "SNC", "ADMINDATA", "IDENTITY"
     */
    private String[] readOnlyParams = {};

    // The maps below are keyed by ALIAS (the object class), not by SAP table name, so the same SAP table
    // can back several object types (e.g. AGR_DEFINE as ACTIVITYGROUP and AGR_DEFINE as AUDITROLES with
    // different WHERE clauses). The underlying SAP table for an alias is in tableNames.
    /**
     * per object class (alias): its columns and their length (legacy RFC_GET_TABLE_ENTRIES mode)
     */
    private Map<String, Map<String, Integer>> tableMetadatas = new LinkedHashMap<String, Map<String, Integer>>();
    /**
     * per object class (alias): which columns are the key
     */
    private Map<String, List<String>> tableKeys = new LinkedHashMap<String, List<String>>();
    /**
     * per object class (alias): which columns are ignored
     */
    private Map<String, List<String>> tableIgnores = new LinkedHashMap<String, List<String>>();
    /**
     * midPoint objectClass (alias) to SAP table name mapping
     */
    private Map<String, String> tableNames = new LinkedHashMap<String, String>();

    /**
     * optional WHERE clause per object class (alias), passed as RFC_READ_TABLE OPTIONS (RFC_READ_TABLE mode only)
     */
    private Map<String, String> tableWhere = new LinkedHashMap<String, String>();

    /**
     * Extra tables, that should be fetched for each table row. Keyed by the root SAP table name.
     */
    private Map<String, List<SubTableMetadata>> subTablesMetadata = new LinkedHashMap<>();

    /**
     * which SAP error codes are considered as non-fatal, for example 025 (Company address cannot be selected), 410 (Maintenance of user user1 locked by user midpoint)
     */
    private String[] nonFatalErrorCodes = {};

    /**
     * Specify how strictly should connector react to password change errors (e.g. user password is too short for SAP).
     */
    private Boolean pwdChangeErrorIsFatal = true;

    /**
     * Simple filter added to all account queries to limit the accounts read by BAPI_USER_GETLIST.
     * Format is "option,parameter,value", commas are currently not escaped. 
     * E.g. "CP,USERNAME,1CD*" to limit the query to all accounts with username prefix 1CD. 
     */
	private String baseAccountQuery = null;

	/** 
	 * Include GLOB_LOCK in account status evaluation.
	 */
	private boolean considerGlobalLock = false;

    @Override
    public void validate() {
        if (isBlank(host)) {
            throw new ConfigurationException("host is empty");
        }
        if (isBlank(x509Cert)) {
            if (isBlank(user)) {
                throw new ConfigurationException("username is empty");
            }
            if (isBlank(getPlainPassword())) {
                throw new ConfigurationException("password is empty");
            }
        }
        if (client == null) {
            throw new ConfigurationException("client is empty");
        }

        if (isReadTableMode()) {
            parseReadTableDefinitions();
        } else {
            parseTableDefinitions();
        }
        parseSubTableDefinitions();

        checkParameterNames();
        
        parseBaseAccountQuery();
    }

    public SapFilter parseBaseAccountQuery() {
		
		if(baseAccountQuery == null) {
			return null;
		} else {
			String[] fields = baseAccountQuery.split(BASE_ACCOUNT_QUERY_SEPARATOR);
			if(fields.length != 3) {
				throw new ConfigurationException("Parameter baseAccountQuery is not empty but is not in the form 'option,parameter,value' - " + baseAccountQuery);
			}
			
			return new SapFilter(fields[0], fields[1], fields[2]);
		}
	}

	private void checkParameterNames() {
        // if empty, update length
        if (tableParameterNames != null && tableParameterNames.length == 1 && "".equals(tableParameterNames[0])) {
            tableParameterNames = new String[0];
        }

        for (String table : tableParameterNames) {
            boolean ok = false;
            for (String supportedTable : SapConnector.TABLETYPE_PARAMETER_LIST) {
                if (supportedTable.equals(table)) {
                    ok = true;
                }
            }
            if (!ok) {
                throw new ConfigurationException("Parameter name " + table + " not exists");
            }
        }
    }

    void parseTableDefinitions() {
        clearTableMaps();
        // if empty, update length
        if (tables != null && tables.length == 1 && "".equals(tables[0])) {
            tables = new String[0];
        }

        if (tables != null) {
            for (String tableDef : tables) {
                String[] table = tableDef.split("=");
                if (table == null || table.length != 2) {
                    throw new ConfigurationException("please use correct read only table definition, for example: 'AGR_DEFINE as ACTIVITYGROUP=MANDT:3:IGNORE,AGR_NAME:30:KEY,PARENT_AGR:30', got: " + table);
                }
                String tableName = table[0];
                String tableAlias = table[0];
                // table has alias
                if (table[0].contains(" as ")) {
                    String[] tableAs = table[0].split(" as ");
                    tableName = tableAs[0];
                    tableAlias = tableAs[1];
                }

                String[] columnsMetaDatas = table[1].split(",");
                if (columnsMetaDatas == null || columnsMetaDatas.length == 0) {
                    throw new ConfigurationException("please put at least one column definition, for example: 'AGR_DEFINE=MANDT:3' (MANDT:3), got: " + columnsMetaDatas);
                }

                Map<String, Integer> tableMetadata = new LinkedHashMap<String, Integer>();
                List<String> keys = new LinkedList<String>();
                List<String> ignore = new LinkedList<String>();
                for (String columnsMetaData : columnsMetaDatas) {
                    String[] column = columnsMetaData.split(":");
                    if (column == null || column.length < 2) {
                        throw new ConfigurationException("please put correct column definition columnName:length, for example: 'AGR_DEFINE=MANDT:3' (MANDT:3), got: " + column);
                    }
                    String columnName = column[0];
                    Integer columnLength = 0;
                    try {
                        columnLength = Integer.valueOf(column[1]);
                    } catch (NumberFormatException ex) {
                        throw new ConfigurationException("please put correct column length, for example: 'AGR_DEFINE=MANDT:3' (3), got: " + column[1]);
                    }

                    tableMetadata.put(columnName, columnLength);
                    // actual column is a key?
                    if (column.length == 3) {
                        String key = column[2];
                        if ("KEY".equalsIgnoreCase(key)) {
                            keys.add(columnName);
                        } else if ("IGNORE".equalsIgnoreCase(key)) {
                            ignore.add(columnName);
                        }
                    }
                }

                if (keys.size() == 0) {
                    throw new ConfigurationException("please select at least one column as a KEY for example: 'AGR_NAME:30:KEY'");
                }

                registerObjectType(tableAlias, tableName);
                tableMetadatas.put(tableAlias, tableMetadata);
                tableKeys.put(tableAlias, keys);
                tableIgnores.put(tableAlias, ignore);
            }
        }
    }

    /** Registers an alias -&gt; SAP table mapping, rejecting a duplicate alias (it would shadow an object class). */
    private void registerObjectType(String alias, String tableName) {
        if (tableNames.containsKey(alias)) {
            throw new ConfigurationException("duplicate object type alias '" + alias
                    + "' in the 'tables' configuration; each alias (object class) must be unique");
        }
        tableNames.put(alias, tableName);
    }

    /** Resets the per-object-class table maps so parsing (hence validate()) can be repeated idempotently. */
    private void clearTableMaps() {
        tableNames.clear();
        tableMetadatas.clear();
        tableKeys.clear();
        tableIgnores.clear();
        tableWhere.clear();
    }

    void parseSubTableDefinitions() {
        subTablesMetadata.clear();
        if (subTables == null) {
            subTables = new String[0];
        }

        // runs after parseTableDefinitions()/parseReadTableDefinitions() in validate(), so tableNames
        // (alias -> SAP table) is populated and the root alias can be checked
        for (String def : subTables) {
            SubTableMetadata metadata = SubTableMetadata.parseConfig(def, isReadTableMode());

            if (!tableNames.containsKey(metadata.getRootAlias())) {
                throw new ConfigurationException("sub-table '" + def + "' refers to root object class '"
                        + metadata.getRootAlias() + "' (after 'for'), which is not defined in 'tables'; "
                        + "use one of " + tableNames.keySet());
            }
            subTablesMetadata.computeIfAbsent(metadata.getRootAlias(), a -> new ArrayList<>()).add(metadata);
        }
    }

    public boolean isReadTableMode() {
        return tableReadFunction != null && !FN_GET_TABLE_ENTRIES.equalsIgnoreCase(tableReadFunction.trim());
    }

    /**
     * Lenient parser used in RFC_READ_TABLE mode. Grammar per line:
     * <pre>TABLE [as ALIAS] [= col[:len][:KEY|:IGNORE], ...] [WHERE &lt;clause&gt;]</pre>
     * Column lengths are ignored (read from SAP), {@code :KEY} overrides the DDIC key, {@code :IGNORE}
     * drops a column. Legacy RFC_GET_TABLE_ENTRIES table lines are accepted unchanged.
     */
    void parseReadTableDefinitions() {
        clearTableMaps();
        if (tables != null && tables.length == 1 && "".equals(tables[0])) {
            tables = new String[0];
        }
        if (tables == null) {
            return;
        }
        for (String tableDef : tables) {
            String def = tableDef.trim();

            // 1) split off optional trailing WHERE first, so '=' inside the clause is safe
            String where = null;
            Matcher m = TABLE_WHERE_PATTERN.matcher(def);
            if (m.find()) {
                where = def.substring(m.end()).trim();
                def = def.substring(0, m.start()).trim();
            }

            // 2) split off optional "= columns"
            String left = def;
            String columnsPart = null;
            int eq = def.indexOf('=');
            if (eq >= 0) {
                left = def.substring(0, eq).trim();
                columnsPart = def.substring(eq + 1).trim();
            }

            // 3) table name and optional alias
            String tableName = left;
            String tableAlias = left;
            int asIdx = indexOfIgnoreCase(left, " as ");
            if (asIdx >= 0) {
                tableName = left.substring(0, asIdx).trim();
                tableAlias = left.substring(asIdx + 4).trim();
            }
            if (tableName.isEmpty()) {
                throw new ConfigurationException("empty table name in tables definition: " + tableDef);
            }
            if (tableAlias.isEmpty()) {
                tableAlias = tableName;
            }

            // 4) optional columns: lengths ignored, KEY/IGNORE honored
            List<String> keys = new LinkedList<String>();
            List<String> ignore = new LinkedList<String>();
            if (columnsPart != null && !columnsPart.isEmpty()) {
                for (String columnDef : columnsPart.split(",")) {
                    String[] parts = columnDef.trim().split(":");
                    if (parts.length == 0 || parts[0].trim().isEmpty()) {
                        continue;
                    }
                    String columnName = parts[0].trim();
                    for (int i = 1; i < parts.length; i++) {
                        String flag = parts[i].trim();
                        if ("KEY".equalsIgnoreCase(flag)) {
                            keys.add(columnName);
                        } else if ("IGNORE".equalsIgnoreCase(flag)) {
                            ignore.add(columnName);
                        }
                        // a numeric length is intentionally ignored in RFC_READ_TABLE mode
                    }
                }
            }

            registerObjectType(tableAlias, tableName);
            tableKeys.put(tableAlias, keys);
            tableIgnores.put(tableAlias, ignore);
            if (where != null && !where.isEmpty()) {
                tableWhere.put(tableAlias, where);
            }
            // tableMetadatas is intentionally left empty in this mode; columns/lengths are read from SAP
        }
    }

    private static int indexOfIgnoreCase(String haystack, String needle) {
        return haystack.toLowerCase().indexOf(needle.toLowerCase());
    }

    @Override
    public String toString() {
        return "SapConfiguration{" +
                "loadBalancing='" + loadBalancing + '\'' +
                ", host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", user='" + user + '\'' +
                ", logonGroup='" + logonGroup + '\'' +
                ", systemId='" + systemId + '\'' +
                ", systemNumber='" + systemNumber + '\'' +
                ", client='" + client + '\'' +
                ", destinationName='" + destinationName + '\'' +
                ", lang='" + lang + '\'' +
                ", poolCapacity='" + poolCapacity + '\'' +
                ", peakLimit='" + peakLimit + '\'' +
                ", sncLibrary='" + sncLibrary + '\'' +
                ", sncMode='" + sncMode + '\'' +
                ", sncMyName='" + sncMyName + '\'' +
                ", sncPartnerName='" + sncPartnerName + '\'' +
                ", sncQoP='" + sncQoP + '\'' +
                ", x509Cert='" + x509Cert + '\'' +
                ", cpicTrace='" + cpicTrace + '\'' +
                ", trace='" + trace + '\'' +
                ", readOnlyParams='" + Arrays.toString(readOnlyParams) + '\'' +
                ", failWhenTruncating=" + failWhenTruncating +
                ", failWhenWarning=" + failWhenWarning +
                ", useTransaction=" + useTransaction +
                ", testBapiFunctionPermission=" + testBapiFunctionPermission +
                ", changePasswordAtNextLogon=" + changePasswordAtNextLogon +
                ", alsoReadLoginInfo=" + alsoReadLoginInfo +
                ", useNativeNames=" + useNativeNames +
                ", tables=" + Arrays.toString(tables) +
                ", tableParameterNames=" + Arrays.toString(tableParameterNames) +
                ", tableMetadatas=" + tableMetadatas +
                ", tableKeys=" + tableKeys +
                ", tableIgnores=" + tableIgnores +
                ", tableNames=" + tableNames +
                ", tableReadFunction='" + tableReadFunction + '\'' +
                ", tableWhere=" + tableWhere +
                ", hideIndirectActivitygroups=" + hideIndirectActivitygroups +
                ", nonFatalErrorCodes='" + Arrays.toString(nonFatalErrorCodes) +
                ", pwdChangeErrorIsFatal=" + pwdChangeErrorIsFatal +
                ", subTables=" + Arrays.toString(subTables) +
                ", subTablesMetadata=" + subTablesMetadata +
                '}';
    }

    @ConfigurationProperty(order = 1, displayMessageKey = "sap.config.loadBalancing",
            helpMessageKey = "sap.config.loadBalancing.help")
    public Boolean getLoadBalancing() {
        return loadBalancing;
    }

    public void setLoadBalancing(Boolean loadBalancing) {
        this.loadBalancing = loadBalancing;
    }

    @ConfigurationProperty(order = 2, displayMessageKey = "sap.config.host",
            helpMessageKey = "sap.config.host.help")
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @ConfigurationProperty(order = 3, displayMessageKey = "sap.config.port",
            helpMessageKey = "sap.config.port.help")
    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    @ConfigurationProperty(order = 4, displayMessageKey = "sap.config.user",
            helpMessageKey = "sap.config.user.help")
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    @ConfigurationProperty(order = 5, displayMessageKey = "sap.config.password",
            helpMessageKey = "sap.config.password.help")
    public GuardedString getPassword() {
        return password;
    }

    public void setPassword(GuardedString password) {
        this.password = password;
    }

    void setPlainPassword(String plainPassword) {
        this.password = new GuardedString(plainPassword.toCharArray());
    }

    @ConfigurationProperty(order = 6, displayMessageKey = "sap.config.logonGroup",
            helpMessageKey = "sap.config.logonGroup.help")
    public String getLogonGroup() {
        return logonGroup;
    }

    public void setLogonGroup(String logonGroup) {
        this.logonGroup = logonGroup;
    }

    @ConfigurationProperty(order = 7, displayMessageKey = "sap.config.systemId",
            helpMessageKey = "sap.config.systemId.help")
    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    @ConfigurationProperty(order = 8, displayMessageKey = "sap.config.systemNumber",
            helpMessageKey = "sap.config.systemNumber.help")
    public String getSystemNumber() {
        return systemNumber;
    }

    public void setSystemNumber(String systemNumber) {
        this.systemNumber = systemNumber;
    }

    @ConfigurationProperty(order = 9, displayMessageKey = "sap.config.client",
            helpMessageKey = "sap.config.client.help")
    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    @ConfigurationProperty(order = 10, displayMessageKey = "sap.config.destinationName",
            helpMessageKey = "sap.config.destinationName.help")
    public String getDestinationName() {
        return destinationName;
    }

    public String getFinalDestinationName() {
        if (isNotEmpty(destinationName))
            return destinationName;

        return getSystemId()+getSystemNumber()+getClient()+getUser();
    }

    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    @ConfigurationProperty(order = 11, displayMessageKey = "sap.config.lang",
            helpMessageKey = "sap.config.lang.help")
    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    @ConfigurationProperty(order = 12, displayMessageKey = "sap.config.failWhenTruncating",
            helpMessageKey = "sap.config.failWhenTruncating.help")
    public Boolean getFailWhenTruncating() {
        return failWhenTruncating;
    }

    public void setFailWhenTruncating(Boolean failWhenTruncating) {
        this.failWhenTruncating = failWhenTruncating;
    }

    @ConfigurationProperty(order = 13, displayMessageKey = "sap.config.failWhenWarning",
            helpMessageKey = "sap.config.failWhenWarning.help")
    public Boolean getFailWhenWarning() {
        return failWhenWarning;
    }

    public void setFailWhenWarning(Boolean failWhenWarning) {
        this.failWhenWarning = failWhenWarning;
    }

    @ConfigurationProperty(order = 14, displayMessageKey = "sap.config.useTransaction",
            helpMessageKey = "sap.config.useTransaction.help")
    public Boolean getUseTransaction() {
        return useTransaction;
    }

    public void setUseTransaction(Boolean useTransaction) {
        this.useTransaction = useTransaction;
    }

    @ConfigurationProperty(order = 15, displayMessageKey = "sap.config.testBapiFunctionPermission",
            helpMessageKey = "sap.config.testBapiFunctionPermission.help")
    public Boolean getTestBapiFunctionPermission() {
        return testBapiFunctionPermission;
    }

    public void setTestBapiFunctionPermission(Boolean testBapiFunctionPermission) {
        this.testBapiFunctionPermission = testBapiFunctionPermission;
    }

    @ConfigurationProperty(order = 16, displayMessageKey = "sap.config.tables",
            helpMessageKey = "sap.config.tables.help")
    public String[] getTables() {
        return tables;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }

    @ConfigurationProperty(order = 17, displayMessageKey = "sap.config.tableParameterNames",
            helpMessageKey = "sap.config.tableParameterNames.help")
    public String[] getTableParameterNames() {
        return tableParameterNames;
    }

    public void setTableParameterNames(String[] tableParameterNames) {
        this.tableParameterNames = tableParameterNames;
    }

    @ConfigurationProperty(order = 18, displayMessageKey = "sap.config.changePasswordAtNextLogon",
            helpMessageKey = "sap.config.changePasswordAtNextLogon.help")
    public Boolean getChangePasswordAtNextLogon() {
        return changePasswordAtNextLogon;
    }

    public void setChangePasswordAtNextLogon(Boolean changePasswordAtNextLogon) {
        this.changePasswordAtNextLogon = changePasswordAtNextLogon;
    }

    @ConfigurationProperty(order = 19, displayMessageKey = "sap.config.alsoReadLoginInfo",
            helpMessageKey = "sap.config.alsoReadLoginInfo.help")
    public Boolean getAlsoReadLoginInfo() {
        return alsoReadLoginInfo;
    }

    public void setAlsoReadLoginInfo(Boolean alsoReadLoginInfo) {
        this.alsoReadLoginInfo = alsoReadLoginInfo;
    }

    @ConfigurationProperty(order = 20, displayMessageKey = "sap.config.useNativeNames",
            helpMessageKey = "sap.config.useNativeNames.help")
    public Boolean getUseNativeNames() {
        return useNativeNames;
    }

    public void setUseNativeNames(Boolean useNativeNames) {
        this.useNativeNames = useNativeNames;
    }

    @ConfigurationProperty(order = 21, displayMessageKey = "sap.config.poolCapacity",
            helpMessageKey = "sap.config.poolCapacity.help")
    public String getPoolCapacity() {
        return poolCapacity;
    }

    public void setPoolCapacity(String poolCapacity) {
        this.poolCapacity = poolCapacity;
    }

    @ConfigurationProperty(order = 22, displayMessageKey = "sap.config.peakLimit",
            helpMessageKey = "sap.config.peakLimit.help")
    public String getPeakLimit() {
        return peakLimit;
    }

    public void setPeakLimit(String peakLimit) {
        this.peakLimit = peakLimit;
    }

    @ConfigurationProperty(order = 23, displayMessageKey = "sap.config.sncLibrary",
            helpMessageKey = "sap.config.sncLibrary.help")
    public String getSncLibrary() {
        return sncLibrary;
    }

    public void setSncLibrary(String sncLibrary) {
        this.sncLibrary = sncLibrary;
    }

    @ConfigurationProperty(order = 24, displayMessageKey = "sap.config.sncMode",
            helpMessageKey = "sap.config.sncMode.help")
    public String getSncMode() {
        return sncMode;
    }

    public void setSncMode(String sncMode) {
        this.sncMode = sncMode;
    }

    @ConfigurationProperty(order = 25, displayMessageKey = "sap.config.sncMyName",
            helpMessageKey = "sap.config.sncMyName.help")
    public String getSncMyName() {
        return sncMyName;
    }

    public void setSncMyName(String sncMyName) {
        this.sncMyName = sncMyName;
    }

    @ConfigurationProperty(order = 26, displayMessageKey = "sap.config.sncPartnerName",
            helpMessageKey = "sap.config.sncPartnerName.help")
    public String getSncPartnerName() {
        return sncPartnerName;
    }

    public void setSncPartnerName(String sncPartnerName) {
        this.sncPartnerName = sncPartnerName;
    }

    @ConfigurationProperty(order = 27, displayMessageKey = "sap.config.sncQoP",
            helpMessageKey = "sap.config.sncQoP.help")
    public String getSncQoP() {
        return sncQoP;
    }

    public void setSncQoP(String sncQoP) {
        this.sncQoP = sncQoP;
    }

    @ConfigurationProperty(order = 28, displayMessageKey = "sap.config.x509Cert",
            helpMessageKey = "sap.config.x509Cert.help")
    public String getX509Cert() {
        return x509Cert;
    }

    public void setX509Cert(String x509Cert) {
        this.x509Cert = x509Cert;
    }

    @ConfigurationProperty(order = 29, displayMessageKey = "sap.config.cpicTrace",
            helpMessageKey = "sap.config.cpicTrace.help")
    public String getCpicTrace() {
        return cpicTrace;
    }

    public void setCpicTrace(String cpicTrace) {
        this.cpicTrace = cpicTrace;
    }

    @ConfigurationProperty(order = 30, displayMessageKey = "sap.config.trace",
            helpMessageKey = "sap.config.trace.help")
    public String getTrace() {
        return trace;
    }

    public void setTrace(String trace) {
        this.trace = trace;
    }

    @ConfigurationProperty(order = 31, displayMessageKey = "sap.config.traceLevel",
            helpMessageKey = "sap.config.traceLevel.help")
    public String getTraceLevel() {
        return traceLevel;
    }

    public void setTraceLevel(String traceLevel) {
        this.traceLevel = traceLevel;
    }

    @ConfigurationProperty(order = 32, displayMessageKey = "sap.config.tracePath",
            helpMessageKey = "sap.config.tracePath.help")
    public String getTracePath() {
        return tracePath;
    }

    public void setTracePath(String tracePath) {
        this.tracePath = tracePath;
    }

    @ConfigurationProperty(order = 33, displayMessageKey = "sap.config.read.only.params",
            helpMessageKey = "sap.config.read.only.params.help")
    public String[] getReadOnlyParams() {
        return readOnlyParams;
    }
    public void setReadOnlyParams(String[] readOnlyParams) {
    	this.readOnlyParams = readOnlyParams;
    }

    @ConfigurationProperty(order = 34, displayMessageKey = "sap.config.hideIndirectActivitygroups",
            helpMessageKey = "sap.config.hideIndirectActivitygroups.help")
    public Boolean getHideIndirectActivitygroups() {
        return hideIndirectActivitygroups;
    }
    public void setHideIndirectActivitygroups(Boolean hideIndirectActivitygroups) {
        this.hideIndirectActivitygroups = hideIndirectActivitygroups;
    }
    @ConfigurationProperty(order = 34, displayMessageKey = "sap.config.nonFatalErrorCodes",
            helpMessageKey = "sap.config.nonFatalErrorCodes.help")
    public String[] getNonFatalErrorCodes() {
        return nonFatalErrorCodes;
    }

    public void setNonFatalErrorCodes(String[] nonFatalErrorCodes) {
        this.nonFatalErrorCodes = nonFatalErrorCodes;
    }

    @ConfigurationProperty(order = 35, displayMessageKey = "sap.config.pwdChangeErrorIsFatal",
            helpMessageKey = "sap.config.pwdChangeErrorIsFatal.help")
    public Boolean getPwdChangeErrorIsFatal() {
        return pwdChangeErrorIsFatal;
    }

    public void setPwdChangeErrorIsFatal(Boolean pwdChangeErrorIsFatal) {
        this.pwdChangeErrorIsFatal = pwdChangeErrorIsFatal;
    }

    @ConfigurationProperty(order = 36, displayMessageKey = "sap.config.mergeAgrNameWithExistingAcitivitygroupsValue",
                           helpMessageKey = "sap.config.mergeAgrNameWithExistingAcitivitygroupsValue.help")
    public Boolean getMergeAgrNameWithExistingAcitivitygroupsValue() {
        return mergeAgrNameWithExistingAcitivitygroupsValue;
    }
    public void setMergeAgrNameWithExistingAcitivitygroupsValue(Boolean mergeAgrNameWithExistingAcitivitygroupsValue) {
        this.mergeAgrNameWithExistingAcitivitygroupsValue = mergeAgrNameWithExistingAcitivitygroupsValue;
    }

    @ConfigurationProperty(order = 37, displayMessageKey = "sap.config.baseAccountQuery",
            helpMessageKey = "sap.config.baseAccountQuery.help")
    public String getBaseAccountQuery() {
        return baseAccountQuery;
    }

    public void setBaseAccountQuery(String baseAccountQuery) {
        this.baseAccountQuery = baseAccountQuery;
	}

	@ConfigurationProperty(order = 38, displayMessageKey = "sap.config.considerGlobalLock",
			helpMessageKey = "sap.config.considerGlobalLock.help")
	public boolean getConsiderGlobalLock() {
		return this.considerGlobalLock;
	}

	public void setConsiderGlobalLock(boolean considerGlobalLock) {
		this.considerGlobalLock = considerGlobalLock;
	}

    @ConfigurationProperty(order = 39, displayMessageKey = "sap.config.subTables",
                           helpMessageKey = "sap.config.subTables.help")
    public String[] getSubTables() {
        return subTables;
    }

    public void setSubTables(String[] subTables) {
        this.subTables = subTables;
    }

    @ConfigurationProperty(order = 40, displayMessageKey = "sap.config.tableReadFunction",
            helpMessageKey = "sap.config.tableReadFunction.help")
    public String getTableReadFunction() {
        return tableReadFunction;
    }

    public void setTableReadFunction(String tableReadFunction) {
        this.tableReadFunction = tableReadFunction;
    }

    private String getPlainPassword() {
        final StringBuilder sb = new StringBuilder();
        if (password != null) {
            password.access(new GuardedString.Accessor() {
                @Override
                public void access(char[] chars) {
                    sb.append(new String(chars));
                }
            });
        } else {
            return null;
        }
        return sb.toString();
    }

    public Properties getDestinationProperties() {
        //adapt parameters in order to configure a valid destination
        Properties connectProperties = new Properties();

        if (loadBalancing) {
            connectProperties.setProperty(DestinationDataProvider.JCO_MSHOST, host);
            connectProperties.setProperty(DestinationDataProvider.JCO_GROUP, logonGroup);
        } else {
            connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, host);
        }

        connectProperties.setProperty(DestinationDataProvider.JCO_SYSNR, systemNumber);
        connectProperties.setProperty(DestinationDataProvider.JCO_R3NAME, systemId);
        connectProperties.setProperty(DestinationDataProvider.JCO_CLIENT, client);
        connectProperties.setProperty(DestinationDataProvider.JCO_LANG, lang);
        connectProperties.setProperty(DestinationDataProvider.JCO_POOL_CAPACITY, poolCapacity);
        connectProperties.setProperty(DestinationDataProvider.JCO_PEAK_LIMIT, peakLimit);
        connectProperties.setProperty(DestinationDataProvider.JCO_SNC_MODE, sncMode);

        if (isNotEmpty(user)) {
            connectProperties.setProperty(DestinationDataProvider.JCO_USER, user);
        }
        if (isNotEmpty(getPlainPassword())) {
            connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, getPlainPassword());
        }
        if (isNotEmpty(sncLibrary)) {
            connectProperties.setProperty(DestinationDataProvider.JCO_SNC_LIBRARY, sncLibrary);
        }
        if (isNotEmpty(sncMyName)) {
            connectProperties.setProperty(DestinationDataProvider.JCO_SNC_MYNAME, sncMyName);
        }
        if (isNotEmpty(sncPartnerName)) {
            connectProperties.setProperty(DestinationDataProvider.JCO_SNC_PARTNERNAME, sncPartnerName);
        }
        if (isNotEmpty(sncQoP)) {
            connectProperties.setProperty(DestinationDataProvider.JCO_SNC_QOP, sncQoP);
        }
        if (isNotEmpty(x509Cert)) {
            connectProperties.setProperty(DestinationDataProvider.JCO_X509CERT, x509Cert);
        }
        if (isNotEmpty(cpicTrace)) {
            connectProperties.setProperty(DestinationDataProvider.JCO_CPIC_TRACE, cpicTrace);
        }
        if (isNotEmpty(trace)) {
            connectProperties.setProperty(DestinationDataProvider.JCO_TRACE, trace);
        }
        if (tracePath != null) {
            JCo.setProperty("jco.trace_path", tracePath);
        }
        if (traceLevel != null) {
            JCo.setProperty("jco.trace_level", traceLevel);
        }

        return connectProperties;
    }

    public Map<String, Map<String, Integer>> getTableMetadatas() {
        return tableMetadatas;
    }

    public Map<String, List<String>> getTableKeys() {
        return tableKeys;
    }

    public Map<String, List<String>> getTableIgnores() {
        return tableIgnores;
    }

    public Map<String, String> getTableNames() {
        return tableNames;
    }

    public Map<String, String> getTableWhere() {
        return tableWhere;
    }

    public Map<String, List<SubTableMetadata>> getSubTablesMetadata() {
        return subTablesMetadata;
    }
}