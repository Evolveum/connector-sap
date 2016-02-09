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
            "BAPI_USER_EXISTENCE_CHECK", "BAPI_TRANSACTION_COMMIT", "BAPI_TRANSACTION_ROLLBACK", "BAPI_USER_DELETE",
            "BAPI_USER_CHANGE", "BAPI_USER_LOCK", "BAPI_USER_UNLOCK", "BAPI_USER_ACTGROUPS_ASSIGN",

            "SUSR_USER_CHANGE_PASSWORD_RFC", "COLL_ACTGROUPS_LOAD_ALL", /*"SUSR_USER_LOCAGR_ACTGROUPS_GET",*/ /* TODO */
            /*"BAPI_USER_CREATE", "BAPI_USER_DISPLAY", "BAPI_USER_EXISTENCE_CHECK", "BAPI_USER_LOCACTGROUPS_ASSIGN",
            "BAPI_USER_LOCACTGROUPS_READ", "BAPI_USER_LOCPROFILES_ASSIGN", "BAPI_USER_LOCPROFILES_READ", "BAPI_USER_PROFILES_ASSIGN"*/
//            "BAPI_PDOTYPES_GET_DETAILEDLIST", "BAPI_ADDRESSORG_GETDETAIL", "SUSR_USER_CHANGE_PASSWORD_RFC"
//            "BAPI_ORGUNITEXT_DATA_GET", "BAPI_OUTEMPLOYEE_GETLIST", "BAPI_EMPLOYEE_GETDATA",
            "RFC_GET_TABLE_ENTRIES", "BAPI_ACTIVITYTYPEGRP_GETLIST"};

    // supported structures reading & writing
    private static final String[] ACCOUNT_PARAMETER_LIST = {"ADDRESS", "DEFAULTS", "UCLASS", "LOGONDATA", "ALIAS", "COMPANY"};
    // supported structures for only for reading
    private static final String[] READ_ONLY_ACCOUNT_PARAMETER_LIST = {"ISLOCKED", "LASTMODIFIED", "REF_USER", "SNC"};

    // LOGONDATA
    private static final String GLTGV = "GLTGV";        //User valid from, AttributeInfo ENABLE_DATE, "LOGONDATA.GLTGV"
    private static final String GLTGB = "GLTGB";        //User valid to, AttributeInfo DISABLE_DATE; "LOGONDATA.GLTGB"

    //ISLOCKED
    private static final String LOCAL_LOCK = "LOCAL_LOCK";     // Local_Lock - Logon generally locked for the local system

    private static final String USERNAME = "USERNAME";

    // PASSWORD
    private static final String BAPIPWD = "BAPIPWD";

    private static final String ACTIVITYGROUPS = "ACTIVITYGROUPS";

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

        LOG.ok("Initialization finished, configuration: {0}", this.configuration.toString());
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
                    destination.createTID();
                }
            }
        } catch (JCoException e) {
            throw new ConnectorIOException(e.getMessage(), e);
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
                    LOG.warn("Not supported class type: " + attrName + "\t" + length + "\t" + className + "\t" + rmd.getRecordTypeName(r) + "\t" + rmd.getTypeAsString(r) + ", ex: " + cnfe);
                }
                if (classs != null && FrameworkUtil.isSupportedAttributeType(classs)) {
                    AttributeInfoBuilder attributeInfoBuilder = new AttributeInfoBuilder(attrName, classs);
                    if (readOnly) {
                        attributeInfoBuilder.setCreateable(false);
                        attributeInfoBuilder.setUpdateable(false);
                    }
                    objClassBuilder.addAttributeInfo(attributeInfoBuilder.build());
                } else if ("java.util.Date".equals(className)) {
                    AttributeInfoBuilder attributeInfoBuilder = new AttributeInfoBuilder(attrName, Long.class);
                    if (readOnly) {
                        attributeInfoBuilder.setCreateable(false);
                        attributeInfoBuilder.setUpdateable(false);
                    }
                    objClassBuilder.addAttributeInfo(attributeInfoBuilder.build());
                    LOG.warn(className + " symulated as java.lang.Long over connector for: " + attrName);
                } else if ("byte[]".equals(className)) {
                    AttributeInfoBuilder attributeInfoBuilder = new AttributeInfoBuilder(attrName, byte[].class);
                    if (readOnly) {
                        attributeInfoBuilder.setCreateable(false);
                        attributeInfoBuilder.setUpdateable(false);
                    }
                    objClassBuilder.addAttributeInfo(attributeInfoBuilder.build());
                } else {
                    AttributeInfoBuilder attributeInfoBuilder = new AttributeInfoBuilder(attrName);
                    if (readOnly) {
                        attributeInfoBuilder.setCreateable(false);
                        attributeInfoBuilder.setUpdateable(false);
                    }
                    objClassBuilder.addAttributeInfo(attributeInfoBuilder.build());
                    LOG.warn("TODO: implement better support for " + className + ", attribute " + attrName + " if you need it, I'm using java.lang.String");
                }
                this.sapAttributesLength.put(attrName, length);
                this.sapAttributesType.put(attrName, className);

                LOG.ok(attrName + "\t" + length + "\t" + className + "\t" + rmd.getRecordTypeName(r) + "\t" + rmd.getTypeAsString(r));
            }
        }
    }

    @Override
    public FilterTranslator<SapFilter> createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {
        return new SapFilterTranslator();
    }

    @Override
    public void executeQuery(ObjectClass objectClass, SapFilter query, ResultsHandler handler, OperationOptions options) {
        LOG.ok("executeQuery: {0}, options: {1}, objectClass: {2}", query, options, objectClass);
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
            JCoFunction function = destination.getRepository().getFunction("RFC_GET_TABLE_ENTRIES");
            if (function == null)
                throw new RuntimeException("RFC_GET_TABLE_ENTRIES not found in SAP.");

            function.getImportParameterList().setValue("TABLE_NAME", tableName);

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

                    handler.handle(builder.build());

                } while (entries.nextRow());
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
                LOG.ok("User as XML, EXPORT: {0}\n TABLE: {1}", function.getExportParameterList().toXML(), function.getTableParameterList().toXML());

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
                            if (finish)
                                break;

                        } while (userList.nextRow());
                    }
                }
            }

        } catch (JCoException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private List<String> executeFunction(JCoFunction function) throws JCoException {
        return executeFunction(function, null);
    }

    private List<String> executeFunction(JCoFunction function, String transactionId) throws JCoException {
        if (transactionId != null) {
            function.execute(destination, transactionId);
        } else {
            function.execute(destination);
        }
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
        if (attrVal != null) {
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
            String transactionId = null;
            try {
                if (configuration.getUseTransaction()) {
                    transactionId = destination.createTID();
                }

                Uid uid = createUser(attributes, transactionId);

                if (configuration.getUseTransaction()) {
                    transactionCommit(transactionId);
                }

                return uid;

            } catch (Exception e) {
                if (configuration.getUseTransaction()) {
                    try {
                        transactionRollback(transactionId);
                    } catch (JCoException e1) {
                        LOG.error(e1, e1.toString());
                    }
                }
                throw new ConnectorIOException(e.getMessage(), e);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    private Uid createUser(Set<Attribute> attributes, String transactionId) throws JCoException, ClassNotFoundException {
        LOG.ok("createUser attributes: {0}, transactionId: {1}", attributes, transactionId);

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

        executeFunction(function, transactionId);

        String savedUserName = function.getImportParameterList().getString("USERNAME");
        LOG.ok("Saved UserName: {0}, importParameterList: {1}", savedUserName, function.getImportParameterList().toXML());

        // assign ACTIVITYGROUPS if needed
        assignActivityGroups(attributes, userName, transactionId);

        // enable or disable user
        enableOrDisableUser(attributes, userName, transactionId);

        if (transactionId != null) {
            destination.confirmTID(transactionId);
        }

        return new Uid(savedUserName);
    }

    private void transactionCommit(String transactionId) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("BAPI_TRANSACTION_COMMIT");
        if (function == null)
            throw new RuntimeException("BAPI_TRANSACTION_COMMIT not found in SAP.");

        function.getImportParameterList().setValue("WAIT", SELECT);

        executeFunction(function, transactionId);
    }

    private void transactionRollback(String transactionId) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("BAPI_TRANSACTION_ROLLBACK");
        if (function == null)
            throw new RuntimeException("BAPI_TRANSACTION_ROLLBACK not found in SAP.");

        executeFunction(function, transactionId);
    }

    private boolean setUserCustomAttributes(Set<Attribute> attributes, JCoParameterList importParameterList, boolean select) {
        boolean updateNeeded = false;

        String passwordAttribute = OperationalAttributeInfos.PASSWORD.getName();

        final StringBuilder sb = new StringBuilder();
        GuardedString password = getAttr(attributes, passwordAttribute, GuardedString.class, null);
        if (password != null) {
            password.access(new GuardedString.Accessor() {
                @Override
                public void access(char[] chars) {
                    sb.append(new String(chars));
                }
            });

//            checkLength(sb.toString(), sapAttributesLength.get(passwordAttribute), passwordAttribute); // DO NOT USE - logging value
            if (configuration.getFailWhenTruncating() && sb.toString().length() > sapAttributesLength.get(BAPIPWD)) {
                throw new ConnectorException("Attribute " + passwordAttribute + " with value XXXX (secured) is longer then maximal length in SAP " + sapAttributesLength.get(passwordAttribute + ", failWhenTruncating=enabled"));
            }

            JCoStructure passwordStructure = importParameterList.getStructure("PASSWORD");
            passwordStructure.setValue(BAPIPWD, sb.toString());
            if (select) {
                JCoStructure passwordStructureX = importParameterList.getStructure("PASSWORDX");
                passwordStructureX.setValue(BAPIPWD, SELECT);
                updateNeeded = true;
            }
        }

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
                LOG.ok("structureNameX: {0}, attributeName: {1}, length: {2}, className: {3}, value: {4}", structureNameX, attributeName, length, classs, SELECT);
                JCoStructure structureX = importParameterList.getStructure(structureNameX);
                structureX.setValue(attributeName, SELECT);
                updateNeeded = true;
            }
        }

        return updateNeeded;
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
                LOG.ok("delete user, Uid: {0}", uid);

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
            String transactionId = null;
            try {
                if (configuration.getUseTransaction()) {
                    transactionId = destination.createTID();
                }
                Uid retUid = updateUser(uid, attributes, transactionId);

                if (configuration.getUseTransaction()) {
                    transactionCommit(transactionId);
                }

                return retUid;
            } catch (Exception e) {
                if (configuration.getUseTransaction()) {
                    try {
                        transactionRollback(transactionId);
                    } catch (JCoException e1) {
                        LOG.error(e1, e1.toString());
                    }
                }
                throw new ConnectorIOException(e.getMessage(), e);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    private Uid updateUser(Uid uid, Set<Attribute> attributes, String transactionId) throws JCoException, ClassNotFoundException {
        LOG.ok("updateUser {0} attributes: {1}, transactionId: {2}", uid, attributes, transactionId);

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

        if (updateNeededCustom || updateNeededGeneric) {
            executeFunction(function, transactionId);
        }

        String changedUserName = function.getImportParameterList().getString("USERNAME");
        LOG.ok("Changed? {0}, UserName: {1}, importParameterList: {2}", (updateNeededCustom || updateNeededGeneric), changedUserName, function.getImportParameterList().toXML());

        // assign ACTIVITYGROUPS if needed
        assignActivityGroups(attributes, userName, transactionId);

        // enable or disable user
        enableOrDisableUser(attributes, changedUserName, transactionId);

        if (configuration.getUseTransaction()) {
            destination.confirmTID(transactionId);
        }

        return new Uid(changedUserName);
    }

    private void enableOrDisableUser(Set<Attribute> attributes, String userName, String transactionId) throws JCoException {
        Boolean enable = getAttr(attributes, OperationalAttributes.ENABLE_NAME, Boolean.class, null);
        if (enable != null) {
            String functionName = enable ? "BAPI_USER_UNLOCK" : "BAPI_USER_LOCK";
            JCoFunction functionLock = destination.getRepository().getFunction(functionName);
            if (functionLock == null)
                throw new RuntimeException(functionName + " not found in SAP.");

            functionLock.getImportParameterList().setValue("USERNAME", userName);
            executeFunction(functionLock, transactionId);
        }
    }

    private void assignActivityGroups(Set<Attribute> attributes, String userName, String transactionId) throws JCoException {
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
            executeFunction(functionAssign, transactionId);
        }
        LOG.ok("ACTGROUPS_ASSIGN modify {0}, TPL: {1}", activityGroups.modify, functionAssign.getTableParameterList().toXML());
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
        LOG.ok("syncUser, token: {0}, options: {1}", token, options);
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

        LOG.ok("{0} user(s) changed in SAP from date {1}", changed, fromToken);
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass objectClass) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {    // __ACCOUNT__

            // TODO better implementation?
            Calendar now = new GregorianCalendar();
            now.set(Calendar.MILLISECOND, 0); // we don't have milisecond precision from SAP in LASTMODIFIED
            SyncToken syncToken = new SyncToken(now.getTime().getTime());
            LOG.ok("returning SyncToken: {0} ({1})", syncToken, now);
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
            LOG.ok("Executing ''{0}''", command);
            process = pb.start();
        } catch (IOException e) {
            LOG.error("Execution of ''{0}'' failed (exec): {1} ({2})", command, e.getMessage(), e.getClass());
            throw new ConnectorIOException(e.getMessage(), e);
        }
        try {
            int exitCode = process.waitFor();
            LOG.ok("Execution of ''{0}'' finished, exit code {1}", command, exitCode);
            return exitCode;
        } catch (InterruptedException e) {
            LOG.error("Execution of ''{0}'' failed (waitFor): {1} ({2})", command, e.getMessage(), e.getClass());
            throw new ConnectionBrokenException(e.getMessage(), e);
        }
    }

}