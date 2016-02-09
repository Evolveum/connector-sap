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

public class SapConfiguration extends AbstractConfiguration {

    private static final Log LOG = Log.getLog(SapConfiguration.class);

    private String host;

    private String port = "3334";

    private String user;

    private GuardedString password;

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
                ", activityGroupsWithDates=" + activityGroupsWithDates +
                ", testBapiFunctionPermission=" + testBapiFunctionPermission +
                ", tables=" + Arrays.toString(tables) +
                '}';
    }

    private String systemId; //r3Name

    private String systemNumber = "00";

    private String client;

    private String lang = "EN";

    private Boolean failWhenTruncating = true;

    private Boolean failWhenWarning = false;

    private Boolean useTransaction = false;

    private Boolean activityGroupsWithDates = false;

    private Boolean testBapiFunctionPermission = true;

    private String[] tables;

    Map<String, Map<String, Integer>> tableMetadatas = new LinkedHashMap<String, Map<String, Integer>>();
    Map<String, List<String>> tableKeys = new LinkedHashMap<String, List<String>>();

    @Override
    public void validate() {
        if (isBlank(host)) {
            throw new ConfigurationException("host is empty");
        }
        if (isBlank(user)) {
            throw new ConfigurationException("username is empty");
        }
        if (isBlank(getPlainPassword())) {
            throw new ConfigurationException("password is empty");
        }
        if (client == null) {
            throw new ConfigurationException("client is empty");
        }

        parseTableDefinitions();

        try {
            new URL(host);
        } catch (MalformedURLException e) {
            throw new ConfigurationException("Malformed host: " + host, e);
        }
    }

    void parseTableDefinitions() {
        if (tables != null) {
            for (String tableDef : tables) {
                String[] table = tableDef.split("=");
                if (table == null || table.length != 2) {
                    throw new ConfigurationException("please use correct read only table definition, for example: 'AGR_DEFINE=MANDT:3:KEY,AGR_NAME:30:KEY,PARENT_AGR:30,CREATE_USR:12', got: " + table);
                }

                String tableName = table[0];
                String[] columnsMetaDatas = table[1].split(",");
                if (columnsMetaDatas == null || columnsMetaDatas.length == 0) {
                    throw new ConfigurationException("please put at least one column definition, for example: 'AGR_DEFINE=MANDT:3' (MANDT:3), got: " + columnsMetaDatas);
                }

                Map<String, Integer> tableMetadata = new LinkedHashMap<String, Integer>();
                List<String> keys = new LinkedList<String>();
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
                    }
                }

                if (keys.size() == 0) {
                    throw new ConfigurationException("please select at least one column as a KEY for example: 'AGR_NAME:30:KEY'");
                }

                tableMetadatas.put(tableName, tableMetadata);
                tableKeys.put(tableName, keys);
            }
        }
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

    @ConfigurationProperty(displayMessageKey = "sap.config.activityGroupsWithDates",
            helpMessageKey = "sap.config.activityGroupsWithDates.help")
    public Boolean getActivityGroupsWithDates() {
        return activityGroupsWithDates;
    }

    public void setActivityGroupsWithDates(Boolean activityGroupsWithDates) {
        this.activityGroupsWithDates = activityGroupsWithDates;
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
        connectProperties.setProperty(DestinationDataProvider.JCO_USER, user);
        connectProperties.setProperty(DestinationDataProvider.JCO_PASSWD, getPlainPassword());
        connectProperties.setProperty(DestinationDataProvider.JCO_LANG, lang);
        return connectProperties;
    }


}