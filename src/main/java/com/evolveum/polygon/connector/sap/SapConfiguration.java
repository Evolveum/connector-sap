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

import com.sap.conn.jco.ext.DestinationDataProvider;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import static org.identityconnectors.common.StringUtil.isBlank;
import static org.identityconnectors.common.StringUtil.isNotEmpty;

public class SapConfiguration extends AbstractConfiguration {

    private static final Log LOG = Log.getLog(SapConfiguration.class);

    private String host;

    private String port = "3334";

    private String user;

    private GuardedString password;

    private String systemId; //r3Name

    private String systemNumber = "00";

    private String client;

    private String lang = "EN";

    private String poolCapacity = "1";

    private String peakLimit = "0";

    private String sncLibrary = null;

    private String sncMode = "0";

    private String sncMyName = null;

    private String sncPartnerName = null;

    private String sncQoP = null;

    private String x509Cert = null;

    private Boolean failWhenTruncating = true;

    private Boolean failWhenWarning = false;

    private Boolean useTransaction = true;

    private Boolean testBapiFunctionPermission = true;

    private Boolean changePasswordAtNextLogon = false;

    private Boolean alsoReadLoginInfo = false;

    private Boolean useNativeNames = false;

    private String[] tables = {"AGR_DEFINE as ACTIVITYGROUP=MANDT:3:IGNORE,AGR_NAME:30:KEY,PARENT_AGR:30", "USGRP as GROUP=MANDT:3:IGNORE,USERGROUP:12:KEY"};

    private String[] tableParameterNames = {"PROFILES", "ACTIVITYGROUPS", "GROUPS"};

    // what SAP table what columns and what lengt has
    Map<String, Map<String, Integer>> tableMetadatas = new LinkedHashMap<String, Map<String, Integer>>();
    // what columns are SAP table keys
    Map<String, List<String>> tableKeys = new LinkedHashMap<String, List<String>>();
    // what columns are ignored
    Map<String, List<String>> tableIgnores = new LinkedHashMap<String, List<String>>();
    // SAP table name to midPoint objectClass
    Map<String, String> tableAliases = new LinkedHashMap<String, String>();

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
        if (tableParameterNames!=null && tableParameterNames.length==1 && "".equals(tableParameterNames[0])) {
            tableParameterNames = new String[0];
        }

        for (String table : tableParameterNames) {
            boolean ok = false;
            for (String supportedTable : SapConnector.TABLETYPE_PARAMETER_LIST) {
                if (supportedTable.equals(table)) {
                    ok = true;
                }
            }
            if (!ok){
                throw new ConfigurationException("Parameter name "+ table+" not exists");
            }
        }
    }

    void parseTableDefinitions() {
        // if empty, update length
        if (tables!=null && tables.length==1 && "".equals(tables[0])) {
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
                        }
                        else if ("IGNORE".equalsIgnoreCase(key)) {
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
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", user='" + user + '\'' +
                ", systemId='" + systemId + '\'' +
                ", systemNumber='" + systemNumber + '\'' +
                ", client='" + client + '\'' +
                ", lang='" + lang + '\'' +
                ", failWhenTruncating=" + failWhenTruncating +
                ", failWhenWarning=" + failWhenWarning +
                ", useTransaction=" + useTransaction +
                ", testBapiFunctionPermission=" + testBapiFunctionPermission +
                ", tables=" + Arrays.toString(tables) +
                ", tableParameterNames=" + Arrays.toString(tableParameterNames) +
                ", changePasswordAtNextLogon=" + changePasswordAtNextLogon +
                ", alsoReadLoginInfo=" + alsoReadLoginInfo +
                ", useNativeNames=" + useNativeNames +
                '}';
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.host",
            helpMessageKey = "sap.config.host.help")
    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.port",
            helpMessageKey = "sap.config.port.help")
    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.user",
            helpMessageKey = "sap.config.user.help")
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.password",
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

    @ConfigurationProperty(displayMessageKey = "sap.config.systemId",
            helpMessageKey = "sap.config.systemId.help")
    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.systemNumber",
            helpMessageKey = "sap.config.systemNumber.help")
    public String getSystemNumber() {
        return systemNumber;
    }

    public void setSystemNumber(String systemNumber) {
        this.systemNumber = systemNumber;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.client",
            helpMessageKey = "sap.config.client.help")
    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.lang",
            helpMessageKey = "sap.config.lang.help")
    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.setFailWhenTruncating",
            helpMessageKey = "sap.config.setFailWhenTruncating.help")
    public Boolean getFailWhenTruncating() {
        return failWhenTruncating;
    }

    public void setFailWhenTruncating(Boolean failWhenTruncating) {
        this.failWhenTruncating = failWhenTruncating;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.getFailWhenWarning",
            helpMessageKey = "sap.config.getFailWhenWarning.help")
    public Boolean getFailWhenWarning() {
        return failWhenWarning;
    }

    public void setFailWhenWarning(Boolean failWhenWarning) {
        this.failWhenWarning = failWhenWarning;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.useTransaction",
            helpMessageKey = "sap.config.useTransaction.help")
    public Boolean getUseTransaction() {
        return useTransaction;
    }

    public void setUseTransaction(Boolean useTransaction) {
        this.useTransaction = useTransaction;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.testBapiFunctionPermission",
            helpMessageKey = "sap.config.testBapiFunctionPermission.help")
    public Boolean getTestBapiFunctionPermission() {
        return testBapiFunctionPermission;
    }

    public void setTestBapiFunctionPermission(Boolean testBapiFunctionPermission) {
        this.testBapiFunctionPermission = testBapiFunctionPermission;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.tables",
            helpMessageKey = "sap.config.tables.help")
    public String[] getTables() {
        return tables;
    }

    public void setTables(String[] tables) {
        this.tables = tables;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.tableParameterNames",
            helpMessageKey = "sap.config.tableParameterNames.help")
    public String[] getTableParameterNames() {
        return tableParameterNames;
    }

    public void setTableParameterNames(String[] tableParameterNames) {
        this.tableParameterNames = tableParameterNames;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.changePasswordAtNextLogon",
            helpMessageKey = "sap.config.changePasswordAtNextLogon.help")
    public Boolean getChangePasswordAtNextLogon() {
        return changePasswordAtNextLogon;
    }

    public void setChangePasswordAtNextLogon(Boolean changePasswordAtNextLogon) {
        this.changePasswordAtNextLogon = changePasswordAtNextLogon;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.alsoReadLoginInfo",
            helpMessageKey = "sap.config.alsoReadLoginInfo.help")
    public Boolean getAlsoReadLoginInfo() {
        return alsoReadLoginInfo;
    }

    public void setAlsoReadLoginInfo(Boolean alsoReadLoginInfo) {
        this.alsoReadLoginInfo = alsoReadLoginInfo;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.useNativeNames",
            helpMessageKey = "sap.config.useNativeNames.help")
    public Boolean getUseNativeNames() {
        return useNativeNames;
    }

    public void setUseNativeNames(Boolean useNativeNames) {
        this.useNativeNames = useNativeNames;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.poolCapacity",
            helpMessageKey = "sap.config.poolCapacity.help")
    public String getPoolCapacity() {
        return poolCapacity;
    }

    public void setPoolCapacity(String poolCapacity) {
        this.poolCapacity = poolCapacity;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.peakLimit",
            helpMessageKey = "sap.config.peakLimit.help")
    public String getPeakLimit() {
        return peakLimit;
    }

    public void setPeakLimit(String peakLimit) {
        this.peakLimit = peakLimit;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.sncLibrary",
            helpMessageKey = "sap.config.sncLibrary.help")
    public String getSncLibrary() {
        return sncLibrary;
    }

    public void setSncLibrary(String sncLibrary) {
        this.sncLibrary = sncLibrary;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.sncMode",
            helpMessageKey = "sap.config.sncMode.help")
    public String getSncMode() {
        return sncMode;
    }

    public void setSncMode(String sncMode) {
        this.sncMode = sncMode;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.sncMyName",
            helpMessageKey = "sap.config.sncMyName.help")
    public String getSncMyName() {
        return sncMyName;
    }

    public void setSncMyName(String sncMyName) {
        this.sncMyName = sncMyName;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.sncPartnerName",
            helpMessageKey = "sap.config.sncPartnerName.help")
    public String getSncPartnerName() {
        return sncPartnerName;
    }

    public void setSncPartnerName(String sncPartnerName) {
        this.sncPartnerName = sncPartnerName;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.sncQoP",
            helpMessageKey = "sap.config.sncQoP.help")
    public String getSncQoP() {
        return sncQoP;
    }

    public void setSncQoP(String sncQoP) {
        this.sncQoP = sncQoP;
    }

    @ConfigurationProperty(displayMessageKey = "sap.config.x509Cert",
            helpMessageKey = "sap.config.x509Cert.help")
    public String getX509Cert() {
        return x509Cert;
    }

    public void setX509Cert(String x509Cert) {
        this.x509Cert = x509Cert;
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
        connectProperties.setProperty(DestinationDataProvider.JCO_ASHOST, host);
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

        return connectProperties;
    }


}