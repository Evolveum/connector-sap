/*
 * Copyright (c) 2010-2016 Evolveum
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

import static org.identityconnectors.common.StringUtil.isBlank;
import static org.identityconnectors.common.StringUtil.isNotEmpty;

public class SapConfiguration extends AbstractConfiguration {

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
     * definition of any tables in SAP to read his data, for example:
     * * AGR_DEFINE as ACTIVITYGROUP - AGR_DEFINE is table name in SAP, ACTIVITYGROUP is his alias in connector
     * * MANDT:3:IGNORE - MANDT is his first column with length 3 and in connector is ignored
     * * AGR_NAME:30:KEY - AGR_NAME is his second column with length 30 and is his key (icfs:UID and also his icfs:NAME)
     * * PARENT_AGR:30 - PARENT_AGR is his third column with length 30 characters
     * * next columns are ignored
     */
    private String[] tables = {"AGR_DEFINE as ACTIVITYGROUP=MANDT:3:IGNORE,AGR_NAME:30:KEY,PARENT_AGR:30", "USGRP as GROUP=MANDT:3:IGNORE,USERGROUP:12:KEY"};

    /**
     * this tables also have a simplified representation of his tables over his keys to comfortable use in mappings, for example ACTIVITYGROUPS.AGR_NAME
     */
    private String[] tableParameterNames = {"PROFILES", "ACTIVITYGROUPS", "GROUPS"};

    /**
     * this params are set as read only params for the connection to restrict what could be read , for example ISLOCKED", "LASTMODIFIED", "SNC", "ADMINDATA", "IDENTITY"
     */
    private String[] readOnlyParams = {};

    /**
     * which SAP table which columns and which length has
     */
    private Map<String, Map<String, Integer>> tableMetadatas = new LinkedHashMap<String, Map<String, Integer>>();
    /**
     * which columns are in which SAP table keys
     */
    private Map<String, List<String>> tableKeys = new LinkedHashMap<String, List<String>>();
    /**
     * which columns are ignored
     */
    private Map<String, List<String>> tableIgnores = new LinkedHashMap<String, List<String>>();
    /**
     * SAP table name to midPoint objectClass mapping
     */
    private Map<String, String> tableAliases = new LinkedHashMap<String, String>();

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

        parseTableDefinitions();

        checkParameterNames();
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

                tableMetadatas.put(tableName, tableMetadata);
                tableKeys.put(tableName, keys);
                tableIgnores.put(tableName, ignore);
                tableAliases.put(tableName, tableAlias);
            }
        }
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
                ", tableAliases=" + tableAliases +
                ", hideIndirectActivitygroups=" + hideIndirectActivitygroups +
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

    public Map<String, String> getTableAliases() {
        return tableAliases;
    }
}