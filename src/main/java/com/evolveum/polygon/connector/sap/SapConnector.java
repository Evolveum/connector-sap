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

import com.sap.conn.jco.*;
import org.identityconnectors.common.Base64;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.FrameworkUtil;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.*;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@ConnectorClass(displayNameKey = "sap.connector.display", configurationClass = SapConfiguration.class)
public class SapConnector implements Connector, TestOp, SchemaOp, SearchOp<SapFilter>, CreateOp, DeleteOp, UpdateOp,
        SyncOp, ScriptOnConnectorOp {

    private static final Log LOG = Log.getLog(SapConnector.class);

    private static final String SEPARATOR = "."; // between structure and his attributes, for example ADDRESS.FIRSTNAME
    // used BAPI functions in connector
    private static final String[] BAPI_FUNCTION_LIST = {"BAPI_USER_GETLIST", "BAPI_USER_GET_DETAIL", "BAPI_USER_CREATE1",
            "BAPI_TRANSACTION_COMMIT", "BAPI_TRANSACTION_ROLLBACK", "BAPI_USER_DELETE",
            "BAPI_USER_CHANGE", "BAPI_USER_LOCK", "BAPI_USER_UNLOCK", "BAPI_USER_ACTGROUPS_ASSIGN",
            "RFC_GET_TABLE_ENTRIES", "SUSR_USER_CHANGE_PASSWORD_RFC", "SUSR_GENERATE_PASSWORD",

            /* "SUSR_LOGIN_CHECK_RFC", "PASSWORD_FORMAL_CHECK" */


            /*"BAPI_ADDRESSORG_GETDETAIL", "BAPI_ORGUNITEXT_DATA_GET", */ /* BAPI to Read Organization Addresses, Get data on organizational unit  */

            /*"BAPI_OUTEMPLOYEE_GETLIST", "BAPI_EMPLOYEE_GETDATA", */ /* List of employees in a payroll area, Find Personnel Numbers for Specified Search Criteria */


            /*"BAPI_USER_PROFILES_ASSIGN", "BAPI_USER_PROFILES_DELETE" */ /* NON CUA landscape */
            /*"BAPI_USER_LOCPROFILES_ASSIGN", "BAPI_USER_LOCPROFILES_READ", "BAPI_USER_LOCPROFILES_DELETE" */ /* CUA landscape*/
            /*"BAPI_USER_LOCACTGROUPS_READ", "BAPI_USER_LOCACTGROUPS_DELETE", "BAPI_USER_LOCACTGROUPS_ASSIGN" */ /* CUA landscape*/

            /*"BAPI_USER_EXISTENCE_CHECK", */ /* handled over Exception and parameter RETURN */
            /*"BAPI_USER_ACTGROUPS_DELETE", */ /* replaced with BAPI_USER_ACTGROUPS_ASSIGN */
            /*"BAPI_USER_DISPLAY",*/ /*Don't need, using BAPI_USER_GET_DETAIL */
            /*"SUSR_BAPI_USER_LOCK", "SUSR_BAPI_USER_UNLOCK", "BAPI_USER_CREATE" */ /*DO NOT USE !*/
    };

    // supported structures reading & writing
    private static final String[] ACCOUNT_PARAMETER_LIST = {"ADDRESS", "DEFAULTS", "UCLASS", "LOGONDATA", "ALIAS", "COMPANY", "REF_USER"};
    // supported structures for only for reading
    private static final String[] READ_ONLY_ACCOUNT_PARAMETER_LIST = {"ISLOCKED", "LASTMODIFIED", "SNC", "ADMINDATA", "IDENTITY"};
    // only create attributes, not to modify, becouse ADDRESSX don't contains these fiels
    private static final String[] CREATE_ONLY_ATTRIBUTES = {"ADDRESS"+SEPARATOR+"COUNTY_CODE", "ADDRESS"+SEPARATOR+"COUNTY",
            "ADDRESS"+SEPARATOR+"TOWNSHIP_CODE", "ADDRESS"+SEPARATOR+"TOWNSHIP", "DEFAULTS"+SEPARATOR+"CATTKENNZ"};

    // LOGONDATA
    private static final String GLTGV = "GLTGV";        //User valid from, AttributeInfo ENABLE_DATE, "LOGONDATA.GLTGV"
    private static final String GLTGB = "GLTGB";        //User valid to, AttributeInfo DISABLE_DATE; "LOGONDATA.GLTGB"

    //ISLOCKED
    private static final String LOCAL_LOCK = "LOCAL_LOCK";     // Local_Lock - Logon generally locked for the local system

    private static final String USERNAME = "USERNAME";

    // PASSWORD
    private static final String BAPIPWD = "BAPIPWD";

    public static final String ACTIVITYGROUPS = "ACTIVITYGROUPS";

    public static final SimpleDateFormat SAP_DF = new SimpleDateFormat("yyyy-MM-dd");
    public static final SimpleDateFormat DATE_TIME = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private static final String SELECT = "X";

    private SapConfiguration configuration;
    private JCoDestination destination;
    private Map<String, Integer> sapAttributesLength = new HashMap<String, Integer>();
    private Map<String, String> sapAttributesType = new HashMap<String, String>();

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        this.configuration = (SapConfiguration) configuration;

        CustomDestinationDataProvider myProvider = CustomDestinationDataProvider.getInstance();

        //register the provider with the JCo environment;
        //catch IllegalStateException if an instance is already registered
        try {
            if (!com.sap.conn.jco.ext.Environment.isDestinationDataProviderRegistered()) {
                com.sap.conn.jco.ext.Environment.registerDestinationDataProvider(myProvider);
            }
        } catch (IllegalStateException providerAlreadyRegisteredException) {
            //somebody else registered its implementation,
            //stop the execution
            throw new ConnectorIOException(providerAlreadyRegisteredException.getMessage(), providerAlreadyRegisteredException);
        }
        String propertiesId = CustomDestinationDataProvider.getNextId();
        //set properties for the destination
        myProvider.setDestinationProperties(propertiesId, ((SapConfiguration) configuration).getDestinationProperties());

        // create destination & ping it
        try {
            this.destination = JCoDestinationManager.getDestination(propertiesId);
            this.destination.ping();
        } catch (JCoException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }

        // initialize read only tables
        ((SapConfiguration) configuration).parseTableDefinitions();
        // read schema
        schema();

        LOG.info("Initialization finished, configuration: {0}", this.configuration.toString());
    }

    @Override
    public void dispose() {
        this.configuration = null;
        if ((this.destination != null) && (JCoContext.isStateful(this.destination))) {
            try {
                JCoContext.end(this.destination);
            } catch (JCoException jcoe) {
                throw new ConnectorIOException(jcoe.getMessage(), jcoe);
            }
        }
    }

    @Override
    public void test() {
        try {
            this.destination.ping();
            if (configuration.getTestBapiFunctionPermission()) {
                List<String> notFoundFunctions = new LinkedList<String>();
                for (String function : BAPI_FUNCTION_LIST) {
                    if (!configuration.getUseTransaction() && function.contains("_TRANSACTION_")) {
                        continue;
                    }
                    JCoFunction jcoFunc = this.destination.getRepository().getFunction(function);
                    if (jcoFunc == null)
                        notFoundFunctions.add(function);
                }
                if (notFoundFunctions.size() > 0) {
                    throw new ConfigurationException("these BAPI functions are not accessible: " + notFoundFunctions);
                }
                // testing creation of transaction
                if (configuration.getUseTransaction()) {
                    JCoContext.begin(destination);
                    JCoContext.end(destination);
                }
            }
        } catch (JCoException e) {
            throw new ConfigurationException(e.getMessage(), e);
        }
    }

    @Override
    public Schema schema() {
        SchemaBuilder builder = new SchemaBuilder(SapConnector.class);

        builder.defineObjectClass(schemaAccount());

        buildTableObjectClasses(builder);

        return builder.build();
    }

    private void buildTableObjectClasses(SchemaBuilder builder) {
        for (Map.Entry<String, Map<String, Integer>> table : configuration.tableMetadatas.entrySet()) {
            String tableName = table.getKey();
            Map<String, Integer> columnsMetadata = table.getValue();

            ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
            objClassBuilder.setType(tableName);

            for (Map.Entry<String, Integer> column : columnsMetadata.entrySet()) {
                String columnName = column.getKey();
                AttributeInfoBuilder attributeInfoBuilder = new AttributeInfoBuilder(columnName);
                attributeInfoBuilder.setCreateable(false);
                attributeInfoBuilder.setUpdateable(false);
                objClassBuilder.addAttributeInfo(attributeInfoBuilder.build());
            }

            builder.defineObjectClass(objClassBuilder.build());
        }
    }

    private ObjectClassInfo schemaAccount() {
        ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();

        try {
            String function = "BAPI_USER_GET_DETAIL";
            getSchemaFromBapiFunction(function, ACCOUNT_PARAMETER_LIST, objClassBuilder, false);
            getSchemaFromBapiFunction(function, READ_ONLY_ACCOUNT_PARAMETER_LIST, objClassBuilder, true);
        } catch (Exception e) {
            throw new ConnectorException("Error when parse user schema from SAP: " + e, e);
        }
        // __NAME__ and __UID__ is default and has same value
        sapAttributesType.put(USERNAME, "java.lang.String");
        sapAttributesLength.put(USERNAME, 12);

        AttributeInfoBuilder attributeActivityGroups = new AttributeInfoBuilder(ACTIVITYGROUPS);
        attributeActivityGroups.setMultiValued(true);
        objClassBuilder.addAttributeInfo(attributeActivityGroups.build());

        objClassBuilder.addAttributeInfo(OperationalAttributeInfos.ENABLE);     // LOCK / UNLOCK - ISLOCKED.LOCAL_LOCK
        objClassBuilder.addAttributeInfo(OperationalAttributeInfos.ENABLE_DATE); // LOGONDATA.GLTGV
        objClassBuilder.addAttributeInfo(OperationalAttributeInfos.DISABLE_DATE); //LOGONDATA.GLTGB

        AttributeInfoBuilder passwordAIB = new AttributeInfoBuilder(OperationalAttributes.PASSWORD_NAME, GuardedString.class);
        passwordAIB.setReadable(false); // write only
        passwordAIB.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(passwordAIB.build());
        sapAttributesType.put(BAPIPWD, "java.lang.String");
        sapAttributesLength.put(BAPIPWD, 40);

        return objClassBuilder.build();
    }

    private List<String> getAllStructureParameters(JCoParameterList epl) {
        List<String> parameters = new LinkedList<String>();
        JCoListMetaData lmd = epl.getListMetaData();
        for (int i = 0; i < lmd.getFieldCount(); i++) {
            if (lmd.isStructure(i)) {
                parameters.add(lmd.getName(i));
            }
        }

        return parameters;
    }

    private void getSchemaFromBapiFunction(String bapiFunction, String[] parameterList, ObjectClassInfoBuilder objClassBuilder, boolean readOnly) throws JCoException, ClassNotFoundException {
        JCoFunction function = destination.getRepository().getFunction(bapiFunction);
        if (function == null)
            throw new RuntimeException(bapiFunction + " not found in SAP.");

        List<String> parameters = null;
        JCoParameterList epl = function.getExportParameterList();
        if (parameterList == null) {
            LOG.ok("reading all structure parameters from ExportParameterList");
            parameters = getAllStructureParameters(epl);
        } else {
            parameters = Arrays.asList(parameterList);
        }

        for (String param : parameters) {
            JCoStructure structure = epl.getStructure(param);
            JCoRecordMetaData rmd = structure.getRecordMetaData();
            LOG.ok("STRUCTURE for " + param + ":");
            for (int r = 0; r < rmd.getFieldCount(); r++) {
                String name = rmd.getName(r);
                Integer length = rmd.getLength(r);
                String attrName = param + SEPARATOR + name;
                String className = rmd.getClassNameOfField(r);
                Class classs = null;
                try {
                    classs = Class.forName(className);
                } catch (ClassNotFoundException cnfe) {
                    if (!"byte[]".equals(className)) {
                        LOG.warn("Not supported class type: " + attrName + "\t" + length + "\t" + className + "\t" + rmd.getRecordTypeName(r) + "\t" + rmd.getTypeAsString(r) + ", ex: " + cnfe);
                    }
                }
                if (classs != null && FrameworkUtil.isSupportedAttributeType(classs)) {
                    objClassBuilder.addAttributeInfo(createAttributeInfo(param, attrName, classs, readOnly));
                } else if ("java.util.Date".equals(className)) {
                    objClassBuilder.addAttributeInfo(createAttributeInfo(param, attrName, Long.class, readOnly));
                    LOG.ok(className + " symulated as java.lang.Long over connector for: " + attrName);
                } else if ("byte[]".equals(className)) {
                    objClassBuilder.addAttributeInfo(createAttributeInfo(param, attrName, byte[].class, readOnly));
                } else {
                    objClassBuilder.addAttributeInfo(createAttributeInfo(param, attrName, String.class, readOnly));
                    LOG.warn("TODO: implement better support for " + className + ", attribute " + attrName + " if you need it, I'm using java.lang.String");
                }
                this.sapAttributesLength.put(attrName, length);
                this.sapAttributesType.put(attrName, className);

                LOG.ok(attrName + "\t" + length + "\t" + className + "\t" + rmd.getRecordTypeName(r) + "\t" + rmd.getTypeAsString(r));
            }
        }
    }


    private AttributeInfo createAttributeInfo(String structure, String attrName, Class classs, boolean readOnly) {
        AttributeInfoBuilder attributeInfoBuilder = new AttributeInfoBuilder(attrName, classs);
        if (readOnly) {
            attributeInfoBuilder.setCreateable(false);
            attributeInfoBuilder.setUpdateable(false);
        }
        else if (contains(CREATE_ONLY_ATTRIBUTES, structure+SEPARATOR+attrName)) {
            attributeInfoBuilder.setCreateable(true);
            attributeInfoBuilder.setUpdateable(false);
        }

        return attributeInfoBuilder.build();
    }

    @Override
    public FilterTranslator<SapFilter> createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {
        return new SapFilterTranslator();
    }

    @Override
    public void executeQuery(ObjectClass objectClass, SapFilter query, ResultsHandler handler, OperationOptions options) {
        LOG.info("executeQuery: {0}, options: {1}, objectClass: {2}", query, options, objectClass);
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {

            executeAccountQuery(objectClass, query, handler, options);

        } else {
            String found = null;
            for (String tableName : configuration.tableMetadatas.keySet()) {
                if (objectClass.is(tableName)) {
                    found = tableName;
                }
            }

            if (found == null) {
                throw new UnsupportedOperationException("Unsupported object class " + objectClass + ", expected: " + configuration.tableMetadatas);
            }

            executeTableQuery(found, query, handler, options);
        }

    }

    private void executeTableQuery(String tableName, SapFilter query, ResultsHandler handler, OperationOptions options) {
        try {
            if (query != null && query.getByNameContains() != null) {
                throw new UnsupportedOperationException("In table: "+tableName+" we can't use CONTAINS operation in query");
            } else {
                // find all or find by key

                JCoFunction function = destination.getRepository().getFunction("RFC_GET_TABLE_ENTRIES");
                if (function == null)
                    throw new RuntimeException("RFC_GET_TABLE_ENTRIES not found in SAP.");

                function.getImportParameterList().setValue("TABLE_NAME", tableName);
                // find by key
                if (query != null && query.getByName() != null) {
                    function.getImportParameterList().setValue("GEN_KEY", query.getByName());
                    LOG.ok("query by Key: "+query.getByName()+" on table: "+tableName);
                }
                // else find all

                function.execute(destination);

                JCoTable entries = function.getTableParameterList().getTable("ENTRIES");

                LOG.info("Entries: " + function.getExportParameterList().getValue("NUMBER_OF_ENTRIES"));

                entries.firstRow();
                if (entries.getNumRows() > 0) {
                    do {
                        String value = entries.getString("WA");
                        int index = 0;

                        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
                        List<String> keys = new LinkedList<String>();

                        for (Map.Entry<String, Integer> entry : configuration.tableMetadatas.get(tableName).entrySet()) {
                            String column = entry.getKey();
                            Integer length = entry.getValue();

                            String columnValue = value.substring(index, index + length).trim();
                            addAttr(builder, column, columnValue);
                            if (configuration.tableKeys.get(tableName).contains(column)) {
                                keys.add(columnValue);
                            }

                            index += length;
                        }

                        StringBuilder concatenatedKey = new StringBuilder();
                        for (String key : keys) {
                            if (concatenatedKey.length() != 0) {
                                concatenatedKey.append(":");
                            }
                            concatenatedKey.append(key);
                        }

                        builder.setUid(concatenatedKey.toString());
                        builder.setName(concatenatedKey.toString());

                        ObjectClass objectClass = new ObjectClass(tableName);
                        builder.setObjectClass(objectClass);

                        ConnectorObject build = builder.build();
                        LOG.ok("ConnectorObject: {0}", build);
                        handler.handle(build);

                    } while (entries.nextRow());
                }
            }
        } catch (JCoException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private void executeAccountQuery(ObjectClass objectClass, SapFilter query, ResultsHandler handler, OperationOptions options) {
        try {
            // find by NAME (or UID - same as name)
            if (query != null && query.getByName() != null) {

                JCoFunction function = destination.getRepository().getFunction("BAPI_USER_GET_DETAIL");
                function.getImportParameterList().setValue("USERNAME", query.getByName());
                executeFunction(function);

                ConnectorObject connectorObject = convertUserToConnectorObject(function);
                handler.handle(connectorObject);

            } // find by name contains
            else if (query != null && query.getByNameContains() != null) {
                JCoFunction function = destination.getRepository().getFunction("BAPI_USER_GETLIST");
                JCoTable exp = function.getTableParameterList().getTable("SELECTION_EXP");
                exp.appendRow();
                exp.setValue("PARAMETER", "USERNAME");
                exp.setValue("OPTION", "CP");
                exp.setValue("LOW", "*" + query.getByNameContains() + "*"); // like '%NAME%'

                executeFunction(function);

                JCoTable userList = function.getTableParameterList().getTable("USERLIST");
                LOG.info("Number of users to read details: " + userList.getNumRows());
                int count = 0;
                if (userList.getNumRows() > 0) {
                    do {
                        if (++count % 10 == 0) {
                            LOG.ok("processing " + count + "/" + userList.getNumRows());
                        }
                        JCoFunction functionDetail = destination.getRepository().getFunction("BAPI_USER_GET_DETAIL");
                        functionDetail.getImportParameterList().setValue("USERNAME", userList.getString("USERNAME"));

                        executeFunction(functionDetail);

                        ConnectorObject connectorObject = convertUserToConnectorObject(functionDetail);
                        boolean finish = !handler.handle(connectorObject);
                        if (finish)
                            break;

                    } while (userList.nextRow());
                }

                // find all
            } else {
                JCoFunction function = destination.getRepository().getFunction("BAPI_USER_GETLIST");

                final Integer pageSize = options == null ? null : options.getPageSize(); // 0 = return all
                if (pageSize != null && pageSize > 0) {
                    // Paged Search
                    final Integer pagedResultsOffset =
                            null != options.getPagedResultsOffset() ? Math.max(0, options
                                    .getPagedResultsOffset()) : 0;
                    function.getImportParameterList().setValue("MAX_ROWS", pagedResultsOffset + pageSize);
                    executeFunction(function);
                    JCoTable userList = function.getTableParameterList().getTable("USERLIST");
                    LOG.info("Number of users to read details: " + pageSize + ", offset: " + pagedResultsOffset + ", returned from SAP: " + userList.getNumRows());

                    int index = 0;
                    int handled = 0;
                    if (userList.getNumRows() > 0) {
                        do {
                            if (pagedResultsOffset > index) {
                                index++;
                                continue;
                            }

                            if (handled >= pageSize) {
                                break;
                            }

                            if (handled % 10 == 0) {
                                LOG.ok("processing " + handled + "/" + pageSize);
                            }

                            JCoFunction functionDetail = destination.getRepository().getFunction("BAPI_USER_GET_DETAIL");
                            functionDetail.getImportParameterList().setValue("USERNAME", userList.getString("USERNAME"));

                            executeFunction(functionDetail);

                            ConnectorObject connectorObject = convertUserToConnectorObject(functionDetail);
                            if (handler.handle(connectorObject)) {
                                handled++;
                            } else {
                                LOG.ok("finishing read");
                                break;
                            }
                        } while (userList.nextRow());
                    }

                } else {
                    // normal search
                    executeFunction(function);
                    JCoTable userList = function.getTableParameterList().getTable("USERLIST");
                    LOG.info("Number of users to read details: " + userList.getNumRows());
                    int count = 0;
                    if (userList.getNumRows() > 0) {
                        do {
                            if (++count % 10 == 0) {
                                LOG.ok("processing " + count + "/" + userList.getNumRows());
                            }
                            JCoFunction functionDetail = destination.getRepository().getFunction("BAPI_USER_GET_DETAIL");
                            functionDetail.getImportParameterList().setValue("USERNAME", userList.getString("USERNAME"));

                            executeFunction(functionDetail);

                            ConnectorObject connectorObject = convertUserToConnectorObject(functionDetail);
                            boolean finish = !handler.handle(connectorObject);
                            if (finish) {
                                LOG.ok("finishing read");
                                break;
                            }

                        } while (userList.nextRow());
                    }
                }
            }

        } catch (JCoException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private List<String> executeFunction(JCoFunction function) throws JCoException {
        function.execute(destination);

        return parseReturnMessages(function.getTableParameterList());
    }

    private void getDataFromBapiFunction(JCoFunction function, String[] parameterList, ConnectorObjectBuilder builder) throws JCoException {
        List<String> parameters = null;
        JCoParameterList epl = function.getExportParameterList();
        if (parameterList == null) {
            LOG.ok("reading all structure parameters from ExportParameterList");
            parameters = getAllStructureParameters(epl);
        } else {
            parameters = Arrays.asList(parameterList);
        }

        for (String param : parameters) {
            JCoStructure structure = epl.getStructure(param);
            JCoRecordMetaData rmd = structure.getRecordMetaData();
//            LOG.ok("reading data from STRUCTURE"+param);
            for (int r = 0; r < rmd.getFieldCount(); r++) {
                String name = rmd.getName(r);
                String className = rmd.getClassNameOfField(r);
                String attrName = param + SEPARATOR + name;
                boolean initialized = structure.isInitialized(name);
                if (initialized) // maybe not null? (not :()
                {
                    if ("java.lang.String".equals(className)) {
                        addAttr(builder, attrName, structure.getString(name));
                    } else if ("java.util.Date".equals(className)) {
                        addAttr(builder, attrName, structure.getDate(name));
                    } else if ("java.math.BigDecimal".equals(className)) {
                        addAttr(builder, attrName, structure.getBigDecimal(name));
                    } else if ("byte[]".equals(className)) {
                        addAttr(builder, attrName, structure.getByteArray(name));
                    } else {
                        addAttr(builder, attrName, structure.getString(name));
                        LOG.warn("TODO: implement better className: " + className + " for attribute: " + attrName);
                    }
                }
            }
        }
    }

    private ConnectorObject convertUserToConnectorObject(JCoFunction function) throws JCoException {
        String userName = function.getImportParameterList().getString("USERNAME");

        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
        builder.setUid(userName);
        builder.setName(userName);

        getDataFromBapiFunction(function, ACCOUNT_PARAMETER_LIST, builder);
        getDataFromBapiFunction(function, READ_ONLY_ACCOUNT_PARAMETER_LIST, builder);

        JCoStructure islocked = function.getExportParameterList().getStructure("ISLOCKED");
        Boolean enable = "U".equals(islocked.getString(LOCAL_LOCK)); // U - unlocked, L - locked
        addAttr(builder, OperationalAttributes.ENABLE_NAME, enable);
        // we don't have BAPI method to unlock only this
//        Boolean lock_out = "U".equals(islocked.getString("WRNG_LOGON")); // U - unlocked, L - locked
//        addAttr(builder, OperationalAttributes.LOCK_OUT_NAME, lock_out);

        JCoStructure logonData = function.getExportParameterList().getStructure("LOGONDATA");
        Date gltgv = logonData.getDate(GLTGV);
        addAttr(builder, OperationalAttributes.ENABLE_DATE_NAME, gltgv == null ? null : gltgv.getTime());
        Date gltgb = logonData.getDate(GLTGB);
        addAttr(builder, OperationalAttributes.DISABLE_DATE_NAME, gltgb == null ? null : gltgb.getTime());

        ActivityGroups activityGroups = new ActivityGroups(function.getTableParameterList().getTable("ACTIVITYGROUPS"));
        builder.addAttribute(AttributeBuilder.build(ACTIVITYGROUPS, activityGroups.getValues(configuration.getActivityGroupsWithDates())));

        ConnectorObject connectorObject = builder.build();

        LOG.ok("convertUserToConnectorObject, user: {0}" +
                        "\n\tconnectorObject: {1}, " +
                        "\n\tactivityGroups: {2}",
                userName, connectorObject, activityGroups);

        return connectorObject;
    }

    public List<String> parseReturnMessages(JCoParameterList tpl) {
        if (tpl == null)
            return null;

        JCoTable returnList = tpl.getTable("RETURN");
        boolean error = false;
        boolean warning = false;
        List<String> ret = new LinkedList<String>();

        returnList.firstRow();
        if (returnList.getNumRows() > 0) {
            do {
                String message = returnList.getString("MESSAGE");
                String type = returnList.getString("TYPE");
                String number = returnList.getString("NUMBER");

                if ("E".equals(type)) {
                    error = true;
                    if ("224".equals(number)) { // User XXX already exists , alternative is BAPI_USER_EXISTENCE_CHECK function
                        throw new AlreadyExistsException(message + ", returnList: " + returnList.toXML());
                    }
                    if ("124".equals(number)) { // User XXX does not exist
                        throw new UnknownUidException(message);
                    }
                    if ("187".equals(number)) { // Password is not long enough (minimum length: 8 characters)
                        throw new InvalidPasswordException(message);
                    }
                    if ("290".equals(number)) { // Please enter an initial password
                        throw new InvalidPasswordException(message);
                    }
                } else if ("W".equals(type)) {
                    warning = true;
                }
                ret.add(type + ":" + number + ":" + message);
            } while (returnList.nextRow());
        }

        if (error) {
            throw new ConnectorException(ret + ", XML representation: \n" + tpl.toXML());
        } else if (warning && configuration != null && configuration.getFailWhenWarning()) {
            throw new ConnectorException(ret + ", XML representation: \n" + tpl.toXML());
        }

        LOG.ok("Return messages: " + ret);
        return ret;
    }

    private <T> void addAttr(ConnectorObjectBuilder builder, String attrName, T attrVal) {
        // null value or "" (empty string) we don't return
        boolean isNull = false;
        if (attrVal == null) {
            isNull = true;
        }
        else if (attrVal instanceof String && StringUtil.isEmpty((String)attrVal)) {
            isNull = true;
        }

        if (!isNull) {
            if (attrVal instanceof Date) {
                builder.addAttribute(attrName, ((Date) attrVal).getTime());
            } else if (attrVal instanceof byte[]) {
                builder.addAttribute(attrName, Base64.encode((byte[]) attrVal));
            } else {
                builder.addAttribute(attrName, attrVal);
            }
        }
    }


    @Override
    public Uid create(ObjectClass objectClass, Set<Attribute> attributes, OperationOptions options) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {    // __ACCOUNT__
            boolean needRollback = false;
            try {
                if (configuration.getUseTransaction()) {
                    JCoContext.begin(destination);
                }

                Uid uid = createUser(attributes);

                if (configuration.getUseTransaction()) {
                    transactionCommit();
                }

                return uid;

            } catch (ConnectorException ce) {
                if (configuration.getUseTransaction()) {
                    needRollback = true;
                }
                throw ce; // handled in framework
            } catch (Exception e) {
                if (configuration.getUseTransaction()) {
                    needRollback = true;
                }
                // need to recreate
                throw new ConnectorIOException(e.getMessage(), e);
            } finally {
                if (configuration.getUseTransaction()) {
                    try {
                        if (needRollback) {
                            transactionRollback();
                        }
                    } catch (JCoException e) {
                        LOG.warn(e, e.toString());
                    }
                    try {
                        JCoContext.end(destination);
                    } catch (JCoException e) {
                        LOG.warn(e, e.toString());
                    }
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    private Uid createUser(Set<Attribute> attributes) throws JCoException, ClassNotFoundException {
        LOG.info("createUser attributes: {0}", attributes);

        JCoFunction function = destination.getRepository().getFunction("BAPI_USER_CREATE1");
        if (function == null)
            throw new RuntimeException("BAPI_USER_CREATE1 not found in SAP.");

        String userName = getStringAttr(attributes, Name.NAME, sapAttributesLength.get(USERNAME));

        function.getImportParameterList().setValue("USERNAME", userName);
        // Alternative logon name for the user. This can be used in some logon scenarios instead of the SAP user name.
//        function.getImportParameterList().getStructure("ALIAS").setValue("USERALIAS",userName);

        // set custom attributes
        setUserCustomAttributes(attributes, function.getImportParameterList(), false);

        // set other attributes
        setGenericAttributes(attributes, function.getImportParameterList(), false);

        // handle password & execute create user
        handlePassword(function, attributes, userName, false, true);

        String savedUserName = function.getImportParameterList().getString("USERNAME");
        LOG.info("Saved UserName: {0}, importParameterList: {1}", savedUserName, function.getImportParameterList().toXML());

        // assign ACTIVITYGROUPS if needed
        assignActivityGroups(attributes, userName);

        // enable or disable user
        enableOrDisableUser(attributes, userName);

        return new Uid(savedUserName);
    }

    private boolean handlePassword(JCoFunction function, Set<Attribute> attributes, String userName, boolean updateUser, boolean updateNeeded) throws JCoException {
        String passwordAttribute = OperationalAttributeInfos.PASSWORD.getName();
        final StringBuilder pwd = new StringBuilder();
        GuardedString password = getAttr(attributes, passwordAttribute, GuardedString.class, null);
        // we need to set password
        if (password != null) {
            // unencrypt password
            password.access(new GuardedString.Accessor() {
                @Override
                public void access(char[] chars) {
                    pwd.append(new String(chars));
                }
            });

            // check password length
            if (configuration.getFailWhenTruncating() && pwd.toString().length() > sapAttributesLength.get(BAPIPWD)) {
                throw new InvalidPasswordException("Attribute " + passwordAttribute + " with value XXXX (secured) is longer then maximum length in SAP " + sapAttributesLength.get(passwordAttribute + ", failWhenTruncating=enabled"));
            }

            // if we set two times the same password, SAP forbid to set the same password for second time - password policy
            // so we first check if new password is not already set to SAP and if yes, we can ignore password update
            if (isPasswordAlreadySet(userName, pwd.toString()))
            {
                // execute create or update user if needed
                if (updateNeeded) {
                    executeFunction(function);
                }
                // password was NOT updated
                return false;
            }

            // we really need to set new password
            if (!configuration.getChangePasswordAtNextLogon()) {
                // if we need unexpired password, we must first set temp password
                // (I need old password, but old password I don't have, so I change old password to tempPwd)
                String tempPwd = generateTempPassword();
                JCoStructure passwordStructure = function.getImportParameterList().getStructure("PASSWORD");
                passwordStructure.setValue(BAPIPWD, tempPwd.toString());
                if (updateUser) {
                    JCoStructure passwordStructureX = function.getImportParameterList().getStructure("PASSWORDX");
                    passwordStructureX.setValue(BAPIPWD, SELECT);
                }
                // execute create or update user
                executeFunction(function);

                // and in next step, temp password we change to needed password
                JCoFunction changePassFunction = destination.getRepository().getFunction("SUSR_USER_CHANGE_PASSWORD_RFC");
                if (changePassFunction == null)
                    throw new RuntimeException("SUSR_USER_CHANGE_PASSWORD_RFC not found in SAP.");

                changePassFunction.getImportParameterList().setValue("BNAME", userName);
                changePassFunction.getImportParameterList().setValue("PASSWORD", tempPwd);
                changePassFunction.getImportParameterList().setValue("NEW_PASSWORD", pwd.toString());

                try {
                    executeFunction(changePassFunction);
                } catch (JCoException e) {
                    LOG.error("can't change password: "+e, e);
                    if (e.getGroup()==126 && "193".equalsIgnoreCase(e.getMessageNumber()))
                        throw new PasswordExpiredException("Choose a password that is different from your last & passwords "+e, e);
                    throw new RuntimeException(e);
                }
                LOG.ok("User "+userName+" don't need to change his password on next login." );
            }
            else {
                // set password - need change it at next logon
                JCoStructure passwordStructure = function.getImportParameterList().getStructure("PASSWORD");
                passwordStructure.setValue(BAPIPWD, pwd.toString());
                if (updateUser) {
                    JCoStructure passwordStructureX = function.getImportParameterList().getStructure("PASSWORDX");
                    passwordStructureX.setValue(BAPIPWD, SELECT);
                }
                // execute create or update user
                executeFunction(function);
                LOG.ok("User "+userName+" need to change his password on next login." );
            }
            // password was updated;
            return true;
        }
        // we don't need to change password
        else {
            // execute create or update user if needed
            if (updateNeeded) {
                executeFunction(function);
            }
            // password was NOT updated;
            return false;
        }
    }

    private void transactionCommit() throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("BAPI_TRANSACTION_COMMIT");
        if (function == null)
            throw new RuntimeException("BAPI_TRANSACTION_COMMIT not found in SAP.");

        function.getImportParameterList().setValue("WAIT", SELECT);

        executeFunction(function);
    }

    private void transactionRollback() throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("BAPI_TRANSACTION_ROLLBACK");
        if (function == null)
            throw new RuntimeException("BAPI_TRANSACTION_ROLLBACK not found in SAP.");

        executeFunction(function);
    }

    private boolean setUserCustomAttributes(Set<Attribute> attributes, JCoParameterList importParameterList, boolean select) {
        boolean updateNeeded = false;

        Long enableDate = getAttr(attributes, OperationalAttributes.ENABLE_DATE_NAME, Long.class, null);
        Long disableDate = getAttr(attributes, OperationalAttributes.DISABLE_DATE_NAME, Long.class, null);
        if (enableDate != null || disableDate != null) {
            JCoStructure logonStructure = importParameterList.getStructure("LOGONDATA");
            if (enableDate != null) {
                logonStructure.setValue(GLTGV, SAP_DF.format(new Date(enableDate)));
                if (select) {
                    JCoStructure logonStructureX = importParameterList.getStructure("LOGONDATAX");
                    logonStructureX.setValue(GLTGV, SELECT);
                    updateNeeded = true;
                }
            }
            if (disableDate != null) {
                logonStructure.setValue(GLTGB, SAP_DF.format(new Date(disableDate)));
                if (select) {
                    JCoStructure logonStructureX = importParameterList.getStructure("LOGONDATAX");
                    logonStructureX.setValue(GLTGB, SELECT);
                    updateNeeded = true;
                }
            }
        }

        return updateNeeded;
    }

    private boolean setGenericAttributes(Set<Attribute> attributes, JCoParameterList importParameterList, boolean select) throws ClassNotFoundException {
        boolean updateNeeded = false;

        for (Attribute attribute : attributes) {
            String name = attribute.getName();
            if (!name.contains(SEPARATOR) || name.startsWith("__")) { // for examle __PASSWORD__
                LOG.ok("attribute managed manually: {0}", attribute);
                continue; // not structure attribute, managed manually
            }
            String[] splittedName = name.split("\\" + SEPARATOR);
            String structureName = splittedName[0];
            String structureNameX = splittedName[0] + "X";
            String attributeName = splittedName[1];
            Integer length = sapAttributesLength.get(name);
            String classs = sapAttributesType.get(name);
            Class type = Class.forName(classs);
            Object value = getAttr(attributes, name, type, length);
            LOG.ok("structureName: {0}, attributeName: {1}, length: {2}, className: {3}, value: {4}", structureName, attributeName, length, classs, value);
            JCoStructure structure = importParameterList.getStructure(structureName);
            structure.setValue(attributeName, value);
            if (select) {
                if ("UCLASSX".equals(structureNameX)) {
                    attributeName = "UCLASS"; // not standard attribute :(
                }
                else if ("ALIASX".equals(structureNameX)) {
                    attributeName = "BAPIALIAS"; // not standard attribute :(
                }
                LOG.ok("structureNameX: {0}, attributeName: {1}, length: {2}, className: {3}, value: {4}", structureNameX, attributeName, length, classs, SELECT);
                JCoStructure structureX = importParameterList.getStructure(structureNameX);
                if (!contains(CREATE_ONLY_ATTRIBUTES, structureName+SEPARATOR+attributeName)) {
                    structureX.setValue(attributeName, SELECT);
                    updateNeeded = true;
                }
                else {
                    LOG.warn("Attribute " + attributeName + " in structure " + structureNameX + " is only Creatable (not Updateable), ignoring changing it to " + value);
                }
            }
        }

        return updateNeeded;
    }


    private boolean contains(String[] datas, String value) {
        for (String data : datas) {
            if (data.equalsIgnoreCase(value)) {
                return true;
            }
        }

        return false;
    }

    private String getStringAttr(Set<Attribute> attributes, String attrName, Integer length) throws InvalidAttributeValueException {
        return getAttr(attributes, attrName, String.class, length);
    }

    private <T> T getAttr(Set<Attribute> attributes, String attrName, Class<T> type, Integer length) throws InvalidAttributeValueException {
        return getAttr(attributes, attrName, type, null, length);
    }

    public <T> T getAttr(Set<Attribute> attributes, String attrName, Class<T> type, T defaultVal, Integer length) throws InvalidAttributeValueException {
        for (Attribute attr : attributes) {
            if (attrName.equals(attr.getName())) {
                List<Object> vals = attr.getValue();
                if (vals == null || vals.isEmpty()) {
                    // set empty value
                    return null;
                }
                if (vals.size() == 1) {
                    Object val = vals.get(0);
                    if (val == null) {
                        // set empty value
                        return null;
                    } else if (type.isAssignableFrom(val.getClass())) {
                        return checkLength((T) val, length, attrName);
                    }
                    throw new InvalidAttributeValueException("Unsupported type " + val.getClass() + " for attribute " + attrName);
                }
                throw new InvalidAttributeValueException("More than one value for attribute " + attrName);
            }
        }
        // set default value when attrName not in changed attributes
        return defaultVal;
    }

    private <T> T checkLength(T val, Integer length, String attrName) {
        if (val == null || length == null) {
            return val;
        } else if (val instanceof String) {
            String stringVal = (String) val;
            if (configuration.getFailWhenTruncating() && stringVal.length() > length) {
                throw new ConnectorException("Attribute " + attrName + " with value " + val + " is longer then maximal length in SAP " + length + ", failWhenTruncating=enabled");
            }
        } else {
            LOG.warn("TODO: implement truncation check for type: " + val.getClass() + ", val: " + val + ", length: " + length + ", attribute: " + attrName);
        }

        return val;
    }


    @Override
    public void delete(ObjectClass objectClass, Uid uid, OperationOptions operationOptions) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            try {
                LOG.info("delete user, Uid: {0}", uid);

                JCoFunction function = destination.getRepository().getFunction("BAPI_USER_DELETE");
                function.getImportParameterList().setValue("USERNAME", uid.getUidValue());
                executeFunction(function);

            } catch (JCoException e) {
                throw new ConnectorIOException(e.getMessage(), e);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    @Override
    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> attributes, OperationOptions operationOptions) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            boolean needRollback = false;
            try {
                if (configuration.getUseTransaction()) {
                    JCoContext.begin(destination);
                }

                Uid retUid = updateUser(uid, attributes);

                if (configuration.getUseTransaction()) {
                    transactionCommit();
                }

                return retUid;

            } catch (ConnectorException ce) {
                if (configuration.getUseTransaction()) {
                    needRollback = true;
                }
                throw ce; // handled in framework
            } catch (Exception e) {
                if (configuration.getUseTransaction()) {
                    needRollback = true;
                }
                // need to recreate
                throw new ConnectorIOException(e.getMessage(), e);
            } finally {
                if (configuration.getUseTransaction()) {
                    try {
                        if (needRollback) {
                            transactionRollback();
                        }
                    } catch (JCoException e) {
                        LOG.warn(e, e.toString());
                    }
                    try {
                        JCoContext.end(destination);
                    } catch (JCoException e) {
                        LOG.warn(e, e.toString());
                    }
                }
            }
        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    private Uid updateUser(Uid uid, Set<Attribute> attributes) throws JCoException, ClassNotFoundException {
        LOG.info("updateUser {0} attributes: {1}", uid, attributes);

        JCoFunction function = destination.getRepository().getFunction("BAPI_USER_CHANGE");
        if (function == null)
            throw new RuntimeException("BAPI_USER_CHANGE not found in SAP.");

        String userName = uid.getUidValue();

        String newUserName = getStringAttr(attributes, Name.NAME, sapAttributesLength.get(USERNAME));

        if (!StringUtil.isEmpty(newUserName) && !userName.equalsIgnoreCase(newUserName)) {
            throw new ConnectorException("SAP don't support RENAME user from '" + userName + "' to '" + newUserName + "'");
        }

        function.getImportParameterList().setValue("USERNAME", userName);
        // Alternative logon name for the user. This can be used in some logon scenarios instead of the SAP user name.
//        function.getImportParameterList().getStructure("ALIAS").setValue("USERALIAS",userName);

        // set custom attributes
        boolean updateNeededCustom = setUserCustomAttributes(attributes, function.getImportParameterList(), true);

        // set other attributes
        boolean updateNeededGeneric = setGenericAttributes(attributes, function.getImportParameterList(), true);

        // handle password & execute update user if needed
        boolean updateNeededPassword =  handlePassword(function, attributes, userName, false, updateNeededCustom || updateNeededGeneric);

        String changedUserName = function.getImportParameterList().getString("USERNAME");
        LOG.info("Changed? {0}, UserName: {1}, importParameterList: {2}", (updateNeededCustom || updateNeededGeneric || updateNeededPassword), changedUserName, function.getImportParameterList().toXML());

        // assign ACTIVITYGROUPS if needed
        assignActivityGroups(attributes, userName);

        // enable or disable user
        enableOrDisableUser(attributes, changedUserName);

        return new Uid(changedUserName);
    }

    private void enableOrDisableUser(Set<Attribute> attributes, String userName) throws JCoException {
        Boolean enable = getAttr(attributes, OperationalAttributes.ENABLE_NAME, Boolean.class, null);
        if (enable != null) {
            String functionName = enable ? "BAPI_USER_UNLOCK" : "BAPI_USER_LOCK";
            JCoFunction functionLock = destination.getRepository().getFunction(functionName);
            if (functionLock == null)
                throw new RuntimeException(functionName + " not found in SAP.");

            functionLock.getImportParameterList().setValue("USERNAME", userName);
            executeFunction(functionLock);
        }
    }

    private void assignActivityGroups(Set<Attribute> attributes, String userName) throws JCoException {
        ActivityGroups activityGroups = null;
        try {
            activityGroups = new ActivityGroups(attributes, ACTIVITYGROUPS);
        } catch (ParseException e) {
            throw new InvalidAttributeValueException("Not parsable ACTIVITYGROUPS in attributes " + attributes + ", " + e, e);
        }
        JCoFunction functionAssign = destination.getRepository().getFunction("BAPI_USER_ACTGROUPS_ASSIGN");
        if (functionAssign == null)
            throw new RuntimeException("BAPI_USER_ACTGROUPS_ASSIGN not found in SAP.");
        functionAssign.getImportParameterList().setValue("USERNAME", userName);
        JCoTable activityGroupsTable = functionAssign.getTableParameterList().getTable("ACTIVITYGROUPS");

        if (activityGroups != null && activityGroups.size() > 0) {
            for (ActivityGroup ag : activityGroups.values) {
                activityGroupsTable.appendRow();
                activityGroupsTable.setValue("AGR_NAME", ag.getName());
                if (ag.getFrom() != null) {
                    activityGroupsTable.setValue("FROM_DAT", ag.getFrom());
                }
                if (ag.getTo() != null) {
                    activityGroupsTable.setValue("TO_DAT", ag.getTo());
                }
            }
        }

        if (activityGroups.modify) {
            executeFunction(functionAssign);
        }
        LOG.info("ACTGROUPS_ASSIGN modify {0}, TPL: {1}", activityGroups.modify, functionAssign.getTableParameterList().toXML());
    }

    @Override
    public void sync(ObjectClass objectClass, SyncToken syncToken, SyncResultsHandler syncResultsHandler, OperationOptions operationOptions) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {    // __ACCOUNT__
            try {
                syncUser(syncToken, syncResultsHandler, operationOptions);
            } catch (Exception e) {
                throw new ConnectorIOException(e.getMessage(), e);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    private void syncUser(SyncToken token, SyncResultsHandler handler, OperationOptions options) throws JCoException, ParseException {
        LOG.info("syncUser, token: {0}, options: {1}", token, options);
        Date fromToken = null;
        if (token != null) {
            Object fromTokenValue = token.getValue();
            if (fromTokenValue instanceof Long) {
                fromToken = new Date((Long) fromTokenValue);
            } else {
                LOG.warn("Synchronization token is not long, ignoring");
            }
        }

        JCoFunction function = destination.getRepository().getFunction("BAPI_USER_GETLIST");
        if (function == null)
            throw new RuntimeException("BAPI_USER_GETLIST not found in SAP.");

        JCoTable exp = function.getTableParameterList().getTable("SELECTION_EXP");
        exp.appendRow();
        // modified users by date, You cannot combine the fields MODDATE and MODTIME for the parameter LAST_MODIFIED.
        exp.setValue("PARAMETER", "LASTMODIFIED");
        exp.setValue("OPTION", "GE");
        exp.setValue("FIELD", "MODDATE");
        exp.setValue("LOW", SAP_DF.format(fromToken));

        executeFunction(function);

        JCoTable userList = function.getTableParameterList().getTable("USERLIST");
        LOG.info("Number of users to read details: " + userList.getNumRows());
        int count = 0;
        int changed = 0;
        if (userList.getNumRows() > 0) {
            do {
                if (++count % 10 == 0) {
                    LOG.ok("syncAccount: processing {0}. of {1} users, changed: {2}", count, userList.getNumRows(), changed);
                }
                String userName = userList.getString("USERNAME");
                JCoFunction functionDetail = destination.getRepository().getFunction("BAPI_USER_GET_DETAIL");
                functionDetail.getImportParameterList().setValue("USERNAME", userName);

                executeFunction(functionDetail);

                JCoStructure lastmodifiedStructure = functionDetail.getExportParameterList().getStructure("LASTMODIFIED");
                String modDate = lastmodifiedStructure.getString("MODDATE");
                String modTime = lastmodifiedStructure.getString("MODTIME");
                // check not only date in filter, but also time and procee only changed after MODDATE and MODTIME
                Date lastModification = DATE_TIME.parse(modDate + " " + modTime);
                if (lastModification.after(fromToken)) {
                    changed++;
                    ConnectorObject connectorObject = convertUserToConnectorObject(functionDetail);

                    SyncDeltaBuilder deltaBuilder = new SyncDeltaBuilder();
                    SyncToken deltaToken = new SyncToken(lastModification.getTime());
                    deltaBuilder.setToken(deltaToken);

                    // all users are updated or created, we can differentiate create from update over
                    // user.getCreateDate().after(fromToken), but it's not necessary to do this
                    SyncDeltaType deltaType = SyncDeltaType.CREATE_OR_UPDATE;

                    deltaBuilder.setObject(connectorObject);
                    deltaBuilder.setUid(new Uid(userName));

                    deltaBuilder.setDeltaType(deltaType);

                    handler.handle(deltaBuilder.build());
                }
            } while (userList.nextRow());
        }

        LOG.info("{0} user(s) changed in SAP from date {1}", changed, fromToken);
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass objectClass) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {    // __ACCOUNT__

            // TODO better implementation?
            Calendar now = new GregorianCalendar();
            now.set(Calendar.MILLISECOND, 0); // we don't have milisecond precision from SAP in LASTMODIFIED
            SyncToken syncToken = new SyncToken(now.getTime().getTime());
            LOG.info("returning SyncToken: {0} ({1})", syncToken, now);
            return syncToken;

        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    @Override
    public Object runScriptOnConnector(ScriptContext scriptContext, OperationOptions operationOptions) {
        String command = scriptContext.getScriptText();
        String[] commandList = command.split("\\s+");
        ProcessBuilder pb = new ProcessBuilder(commandList);
        Map<String, String> env = pb.environment();
        for (Map.Entry<String, Object> argEntry : scriptContext.getScriptArguments().entrySet()) {
            String varName = argEntry.getKey();
            Object varValue = argEntry.getValue();
            if (varValue == null) {
                env.remove(varName);
            } else {
                env.put(varName, varValue.toString());
            }
        }
        Process process;
        try {
            LOG.info("Executing ''{0}''", command);
            process = pb.start();
        } catch (IOException e) {
            LOG.error("Execution of ''{0}'' failed (exec): {1} ({2})", command, e.getMessage(), e.getClass());
            throw new ConnectorIOException(e.getMessage(), e);
        }
        try {
            int exitCode = process.waitFor();
            LOG.info("Execution of ''{0}'' finished, exit code {1}", command, exitCode);
            return exitCode;
        } catch (InterruptedException e) {
            LOG.error("Execution of ''{0}'' failed (waitFor): {1} ({2})", command, e.getMessage(), e.getClass());
            throw new ConnectionBrokenException(e.getMessage(), e);
        }
    }

    private String generateTempPassword (){
        JCoFunction function = null;
        try {
            function = destination.getRepository().getFunction("SUSR_GENERATE_PASSWORD");
            executeFunction(function);
            String pwd = function.getExportParameterList().getString("PASSWORD");
            return pwd;
        } catch (Exception e) {
            LOG.warn("Using hardcoded temp password: " + e);
            return "S&r7tP(%s";
        }
    }

    private boolean isPasswordAlreadySet(String userName, String password) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("SUSR_LOGIN_CHECK_RFC");
        if (function == null)
            throw new RuntimeException("SUSR_LOGIN_CHECK_RFC not found in SAP.");
        function.getImportParameterList().setValue("BNAME", userName);
        function.getImportParameterList().setValue("PASSWORD", password);

        try {
            function.execute(destination);
        } catch (JCoException e) {
            if (e.getGroup()==126 && "152".equalsIgnoreCase(e.getMessageNumber())) {
                // (126) WRONG_PASSWORD: WRONG_PASSWORD Message 152 of class 00 type E
                return false;
            } else if (e.getGroup()==126 && "20".equalsIgnoreCase(e.getMessageNumber())){
                // (126) NO_CHECK_FOR_THIS_USER: NO_CHECK_FOR_THIS_USER Message 200 of class 00 type E
                throw new PermissionDeniedException("Password logon no longer possible - too many failed attempts: "+e, e);
            } else {
                LOG.error(e, "new exception type, how to handle? "+e);
                throw e;
            }
        }

        return true;

    }

}