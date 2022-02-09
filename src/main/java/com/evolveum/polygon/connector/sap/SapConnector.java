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
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.*;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@ConnectorClass(displayNameKey = "sap.connector.display", configurationClass = SapConfiguration.class)
public class SapConnector implements PoolableConnector, TestOp, SchemaOp, SearchOp<SapFilter>, CreateOp, DeleteOp, UpdateOp,
        SyncOp, ScriptOnConnectorOp {

    private static final Log LOG = Log.getLog(SapConnector.class);

    private static final String SEPARATOR = "."; // between structure and his attributes, for example ADDRESS.FIRSTNAME
    // used BAPI functions in connector
    private static final String[] BAPI_FUNCTION_LIST = {"BAPI_USER_GETLIST", "BAPI_USER_GET_DETAIL", "BAPI_USER_CREATE1",
            "BAPI_TRANSACTION_COMMIT", "BAPI_TRANSACTION_ROLLBACK", "BAPI_USER_DELETE",
            "BAPI_USER_CHANGE", "BAPI_USER_LOCK", "BAPI_USER_UNLOCK", "BAPI_USER_ACTGROUPS_ASSIGN",
            "RFC_GET_TABLE_ENTRIES", "SUSR_USER_CHANGE_PASSWORD_RFC", "SUSR_GENERATE_PASSWORD",
            "BAPI_USER_PROFILES_ASSIGN", "BAPI_HELPVALUES_GET",
            "SUSR_LOGIN_CHECK_RFC", "PASSWORD_FORMAL_CHECK",
            "SUSR_GET_ADMIN_USER_LOGIN_INFO"
//            , "SUSR_BAPI_USER_UNLOCK"

            /*"BAPI_ADDRESSORG_GETDETAIL", "BAPI_ORGUNITEXT_DATA_GET", */ /* BAPI to Read Organization Addresses, Get data on organizational unit  */

            /*"BAPI_OUTEMPLOYEE_GETLIST", "BAPI_EMPLOYEE_GETDATA", */ /* List of employees in a payroll area, Find Personnel Numbers for Specified Search Criteria  - not found in SAP :((( */

            /*"BAPI_USER_LOCPROFILES_ASSIGN", "BAPI_USER_LOCPROFILES_READ", "BAPI_USER_LOCPROFILES_DELETE" */ /* CUA landscape*/
            /*"BAPI_USER_LOCACTGROUPS_READ", "BAPI_USER_LOCACTGROUPS_DELETE", "BAPI_USER_LOCACTGROUPS_ASSIGN" */ /* CUA landscape*/

            /*"BAPI_USER_EXISTENCE_CHECK", */ /* handled over Exception and parameter RETURN */
            /*"BAPI_USER_ACTGROUPS_DELETE", */ /* replaced with BAPI_USER_ACTGROUPS_ASSIGN */
            /*"BAPI_USER_PROFILES_DELETE" */ /* replaced with BAPI_USER_PROFILES_ASSIGN, NON CUA landscape */
            /*"BAPI_USER_DISPLAY",*/ /*Don't need, using BAPI_USER_GET_DETAIL */
            /*"SUSR_BAPI_USER_LOCK", "BAPI_USER_CREATE" */ /*DO NOT USE !*/
    };

    // SAP roles, groups, profiles
    private static final String AGR_NAME = "AGR_NAME";
    public static final String ACTIVITYGROUPS = "ACTIVITYGROUPS";
    public static final String ACTIVITYGROUPS__ARG_NAME = ACTIVITYGROUPS + SEPARATOR + AGR_NAME;

    private static final String BAPIPROF = "BAPIPROF";
    public static final String PROFILES = "PROFILES";
    public static final String PROFILES_BAPIPROF = PROFILES + SEPARATOR + BAPIPROF;
    public static final String PROFILE_NAME = "PROFILE"; // also as objectClass name

    private static final String USERGROUP = "USERGROUP";
    public static final String GROUPS = "GROUPS";
    public static final String GROUPS_USERGROUP = GROUPS + SEPARATOR + USERGROUP;

    // see for example http://www.sapdatasheet.org/abap/func/BAPI_USER_GET_DETAIL.html
    // these "Paremeter name"-s we can read and write in Type "Exporting"
    private static final String[] READ_WRITE_PARAMETERS = {"ADDRESS", "DEFAULTS", "UCLASS", "LOGONDATA", "ALIAS", "COMPANY", "REF_USER"};
    // these "Paremeter name"-s we can only read (don't have appropirade parameters in BAPI_USER_CHANGE)
    static final String[] READ_ONLY_PARAMETERS = {"ISLOCKED", "LASTMODIFIED", "SNC", "ADMINDATA", "IDENTITY"};
    // variable version of READ_ONLY_PARAMETERS that could be set through connector configuration
    private String[] readOnlyParams;
    // these attributes in "ADDRESS" parameter name we can't update, only set, because "ADDRESSX" in BAPI_USER_CHANGE don't contains these fields
    private static final String[] CREATE_ONLY_ATTRIBUTES = {"ADDRESS" + SEPARATOR + "COUNTY_CODE", "ADDRESS" + SEPARATOR + "COUNTY",
            "ADDRESS" + SEPARATOR + "TOWNSHIP_CODE", "ADDRESS" + SEPARATOR + "TOWNSHIP", "DEFAULTS" + SEPARATOR + "CATTKENNZ"};


    // supported tables reading & writing in tables parameter type
    // "UCLASSSYS", "EXTIDHEAD", "EXTIDPART", "SYSTEMS" not supported yet
    public static final String[] TABLETYPE_PARAMETER_LIST = {"PARAMETER", PROFILES, ACTIVITYGROUPS,
            "RETURN", "ADDTEL", "ADDFAX", "ADDTTX", "ADDTLX", "ADDSMTP", "ADDRML", "ADDX400", "ADDRFC", "ADDPRT", "ADDSSF",
            "ADDURI", "ADDPAG", "ADDCOMREM", "PARAMETER1", GROUPS/*, "UCLASSSYS", "EXTIDHEAD", "EXTIDPART", "SYSTEMS"*/};

    // these tables on update use "change flag" explicitly as is
    private static final List<String> TABLES_WITH_CHANGE_FLAG = Arrays.asList(GROUPS, "PARAMETER", "PARAMETER1");
    // these communication data tables on update use "change flag" in "ADDCOMX"
    private static final List<String> COMMUNICATION_DATA_TABLES_WITH_CHANGE_FLAG = Arrays.asList("ADDTEL",
            "ADDFAX", "ADDTTX", "ADDTLX", "ADDSMTP", "ADDRML", "ADDX400", "ADDRFC", "ADDPRT", "ADDSSF", "ADDURI", "ADDPAG", "ADDCOMREM");

    // with these tables we can manipulate as XML line with all his attributes (for example ACTIVITYGROUPS)
    // and also as only string keys (for example ACTIVITYGROUPS.AGR_NAME) - used in midPoint associations
    public static final Map<String, String> TABLETYPE_PARAMETER_KEYS = new HashMap<>();

    static {
        TABLETYPE_PARAMETER_KEYS.put(ACTIVITYGROUPS, AGR_NAME);
        TABLETYPE_PARAMETER_KEYS.put(PROFILES, BAPIPROF);
        TABLETYPE_PARAMETER_KEYS.put(GROUPS, USERGROUP);
    }

    // LOGONDATA
    private static final String GLTGV = "GLTGV";        //User valid from, AttributeInfo ENABLE_DATE, "LOGONDATA.GLTGV"
    private static final String GLTGB = "GLTGB";        //User valid to, AttributeInfo DISABLE_DATE; "LOGONDATA.GLTGB"

    //ISLOCKED
    private static final String LOCAL_LOCK = "LOCAL_LOCK";     // Local_Lock - Logon generally locked for the local system
    private static final String WRNG_LOGON = "WRNG_LOGON";     // WRNG_LOGON - Password logon locked by incorrect logon attempts

    public static final String USERNAME = "USERNAME";

    // PASSWORD
    private static final String BAPIPWD = "BAPIPWD";

    //USER_LOGIN_INFO prefix
    private static final String USER_LOGIN_INFO = "USER_LOGIN_INFO";

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
        LOG.info("Initialization start, configuration: {0}", this.configuration.toString());

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

        //properties for destination from config
        Properties props = this.configuration.getDestinationProperties();

        String destinationName = this.configuration.getFinalDestinationName();

        Properties destProps = myProvider.getDestinationProperties(destinationName);
        if (destProps == null || !destProps.equals(props)){
            myProvider.setDestinationProperties(destinationName, props);
        }
        // set read only parameters from gui connector configuration
        readOnlyParams =  this.configuration.getReadOnlyParams();

        if (this.configuration.SNC_MODE_ON.equals(this.configuration.getSncMode())) {
            createDestinationDataFile(destinationName, props);
        }

        // create destination & ping it
        try {
            this.destination = JCoDestinationManager.getDestination(destinationName);
            this.destination.ping();
        } catch (JCoException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }

        // validate & initialize tables
        configuration.validate();
        // read schema
        schema();

        LOG.info("Initialization finished");
    }


    @Override
    public void checkAlive() {
        if (this.destination == null || !this.destination.isValid()) {
            LOG.ok("check alive: FAILED");
            throw new ConnectorException("Connection check failed");
        }
        try {
            this.destination.ping();
        } catch (JCoException e) {
            LOG.ok("connection ping FAILED: "+e);
            throw new ConnectorException("Connection ping failed", e);
        }
        LOG.ok("check alive: OK");
    }


    private void createDestinationDataFile(String destinationName, Properties connectProperties)
    {
        String fileName = destinationName+".jcoDestination";
        try
        {
            File destCfg = new File(fileName);
            destCfg.deleteOnExit(); // only temporary files
            FileOutputStream fos = new FileOutputStream(destCfg,false);
            connectProperties.store(fos, "for tests only !");
            fos.close();
            LOG.ok("destination file was created: "+destCfg.getName()+", "+destCfg.getAbsolutePath());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Unable to create the destination file: "+fileName, e);
        }
    }

    private void deleteDestinationDataFile(String destinationName)
    {
        String fileName = destinationName+".jcoDestination";
        try
        {
            File destCfg = new File(fileName);
            boolean deleted = destCfg.delete();
            if (deleted) {
                LOG.ok("destination file was deleted: " + destCfg.getName() + ", " + destCfg.getAbsolutePath());
            }
        }
        catch (Exception e)
        {
            LOG.warn("Unable to delete the destination file: "+fileName, e);
        }
    }

    @Override
    public void dispose() {
        this.configuration = null;
        if ((this.destination != null) && (JCoContext.isStateful(this.destination))) {
            if (this.configuration.SNC_MODE_ON.equals(this.configuration.getSncMode())) {
                deleteDestinationDataFile(this.destination.getDestinationName());
            }
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

        buildAccountObjectClass(builder);

        buildTableObjectClasses(builder);

        buildProfileObjectClass(builder);

        return builder.build();
    }

    private void buildAccountObjectClass(SchemaBuilder builder) {
        ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();

        try {
            String function = "BAPI_USER_GET_DETAIL";
            getSchemaFromBapiFunction(function, READ_WRITE_PARAMETERS, objClassBuilder, false);
            getSchemaFromBapiFunction(function, readOnlyParams, objClassBuilder, true);
        } catch (Exception e) {
            throw new ConnectorIOException("Error when parse user schema from SAP: " + e, e);
        }
        // __NAME__ and __UID__ is default and was renamed to USERNAME in schema if it's needed
        AttributeInfoBuilder uidAib = new AttributeInfoBuilder(Uid.NAME);
        uidAib.setRequired(true);
        if (this.configuration.getUseNativeNames()) {
            uidAib.setNativeName(USERNAME);
        }
        objClassBuilder.addAttributeInfo(uidAib.build());

        AttributeInfoBuilder nameAib = new AttributeInfoBuilder(Name.NAME);
        if (this.configuration.getUseNativeNames()) {
            nameAib.setNativeName(USERNAME);
        }
        objClassBuilder.addAttributeInfo(nameAib.build());

        sapAttributesType.put(USERNAME, "java.lang.String");
        sapAttributesLength.put(USERNAME, 12);

        objClassBuilder.addAttributeInfo(OperationalAttributeInfos.ENABLE);     // enable / disable - ISLOCKED.LOCAL_LOCK
        objClassBuilder.addAttributeInfo(OperationalAttributeInfos.LOCK_OUT);   // unlock - ISLOCKED.WRNG_LOGON
        objClassBuilder.addAttributeInfo(OperationalAttributeInfos.ENABLE_DATE); // LOGONDATA.GLTGV
        objClassBuilder.addAttributeInfo(OperationalAttributeInfos.DISABLE_DATE); //LOGONDATA.GLTGB

        AttributeInfoBuilder passwordAIB = new AttributeInfoBuilder(OperationalAttributes.PASSWORD_NAME, GuardedString.class);
        passwordAIB.setReadable(false); // write only
        passwordAIB.setReturnedByDefault(false);
        objClassBuilder.addAttributeInfo(passwordAIB.build());
        sapAttributesType.put(BAPIPWD, "java.lang.String");
        sapAttributesLength.put(BAPIPWD, 40);

        // tables
        for (String table : configuration.getTableParameterNames()) {
            AttributeInfoBuilder attributeActivityGroup = new AttributeInfoBuilder(table);
            attributeActivityGroup.setMultiValued(true);
            objClassBuilder.addAttributeInfo(attributeActivityGroup.build());
            // id's from tables
            if (TABLETYPE_PARAMETER_KEYS.containsKey(table)) {
                AttributeInfoBuilder attributeActivityGroupIds = new AttributeInfoBuilder(table + SEPARATOR + TABLETYPE_PARAMETER_KEYS.get(table));
                attributeActivityGroupIds.setMultiValued(true);
                objClassBuilder.addAttributeInfo(attributeActivityGroupIds.build());
            }
        }
        // user_login_infos
        if (this.configuration.getAlsoReadLoginInfo()) {
            boolean readOnly = true;
            objClassBuilder.addAttributeInfo(createAttributeInfo(null, USER_LOGIN_INFO + SEPARATOR + "LAST_LOGON_DATE", Long.class, readOnly));
            objClassBuilder.addAttributeInfo(createAttributeInfo(null, USER_LOGIN_INFO + SEPARATOR + "LOCK_STATUS", String.class, readOnly));
            objClassBuilder.addAttributeInfo(createAttributeInfo(null, USER_LOGIN_INFO + SEPARATOR + "PASSWORD_STATUS", String.class, readOnly));
        }

        builder.defineObjectClass(objClassBuilder.build());
    }

    private void buildTableObjectClasses(SchemaBuilder builder) {
        for (Map.Entry<String, Map<String, Integer>> table : configuration.getTableMetadatas().entrySet()) {
            String tableName = table.getKey();
            Map<String, Integer> columnsMetadata = table.getValue();

            ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
            objClassBuilder.setType(configuration.getTableAliases().get(tableName));

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

    private void buildProfileObjectClass(SchemaBuilder builder) {
        ObjectClassInfoBuilder objClassBuilder = new ObjectClassInfoBuilder();
        objClassBuilder.setType(PROFILE_NAME);

        AttributeInfoBuilder uidAib = new AttributeInfoBuilder(Uid.NAME);
        uidAib.setRequired(true);
        uidAib.setCreateable(false);
        uidAib.setUpdateable(false);
        if (this.configuration.getUseNativeNames()) {
            uidAib.setNativeName(PROFILE_NAME);
        }
        objClassBuilder.addAttributeInfo(uidAib.build());

        AttributeInfoBuilder nameAib = new AttributeInfoBuilder(Name.NAME);
        nameAib.setCreateable(false);
        nameAib.setUpdateable(false);
        if (this.configuration.getUseNativeNames()) {
            nameAib.setNativeName(PROFILE_NAME);
        }
        objClassBuilder.addAttributeInfo(nameAib.build());

        builder.defineObjectClass(objClassBuilder.build());
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
//            LOG.ok("STRUCTURE for " + param + ":");
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
                    LOG.ok(className + " simulated as java.lang.Long over connector for: " + attrName);
                } else if ("byte[]".equals(className)) {
                    objClassBuilder.addAttributeInfo(createAttributeInfo(param, attrName, byte[].class, readOnly));
                } else {
                    objClassBuilder.addAttributeInfo(createAttributeInfo(param, attrName, String.class, readOnly));
                    LOG.warn("TODO: implement better support for " + className + ", attribute " + attrName + " if you need it, I'm using java.lang.String");
                }
                this.sapAttributesLength.put(attrName, length);
                this.sapAttributesType.put(attrName, className);

//                LOG.ok(attrName + "\t" + length + "\t" + className + "\t" + rmd.getRecordTypeName(r) + "\t" + rmd.getTypeAsString(r));
            }
        }
    }


    private AttributeInfo createAttributeInfo(String structure, String attrName, Class classs, boolean readOnly) {
        AttributeInfoBuilder attributeInfoBuilder = new AttributeInfoBuilder(attrName, classs);
        if (readOnly) {
            attributeInfoBuilder.setCreateable(false);
            attributeInfoBuilder.setUpdateable(false);
        } else if (contains(CREATE_ONLY_ATTRIBUTES, structure + SEPARATOR + attrName)) {
            attributeInfoBuilder.setCreateable(true);
            attributeInfoBuilder.setUpdateable(false);
        }

        return attributeInfoBuilder.build();
    }

    @Override
    public FilterTranslator<SapFilter> createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            return new SapAccountFilterTranslator();
        }
        else {
            return new SapBasicFilterTranslator();
        }
    }

    @Override
    public void executeQuery(ObjectClass objectClass, SapFilter query, ResultsHandler handler, OperationOptions options) {
        LOG.info("executeQuery: {0}, options: {1}, objectClass: {2}", query, options, objectClass);

        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {

            executeAccountQuery(query, handler, options);

        } else if (objectClass.is(PROFILE_NAME)) {

            executeProfileQuery(query, handler);

        } else {
            String found = null;
            for (String tableName : configuration.getTableAliases().keySet()) {
                String tableAlias = configuration.getTableAliases().get(tableName);
                if (objectClass.is(tableAlias)) {
                    found = tableName;
                }
            }

            if (found == null) {
                throw new UnsupportedOperationException("Unsupported object class " + objectClass + ", expected: " + configuration.getTableMetadatas());
            }

            executeTableQuery(found, query, handler);
        }

    }

    private void executeProfileQuery(SapFilter query, ResultsHandler handler) {
        try {
            // find all or find by key

            JCoFunction function = destination.getRepository().getFunction("BAPI_HELPVALUES_GET");
            if (function == null)
                throw new RuntimeException("BAPI_HELPVALUES_GET not found in SAP.");

            function.getImportParameterList().setValue("OBJNAME", "USER");
            function.getImportParameterList().setValue("METHOD", "ProfilesAssign");
            function.getImportParameterList().setValue("PARAMETER", "Profiles");
            function.getImportParameterList().setValue("FIELD", BAPIPROF);

            function.execute(destination);

            JCoTable entries = function.getTableParameterList().getTable("VALUES_FOR_FIELD");


            entries.firstRow();
            LOG.ok("Number of entries in input: {0}, filter: {1}", entries.getNumRows(), query != null ? query.getBasicByNameEquals() : "(empty)");
            if (entries.getNumRows() > 0) {
                do {
                    String profile = entries.getString("VALUES");

                    if (query != null && query.getBasicByNameEquals() != null && !profile.equalsIgnoreCase(query.getBasicByNameEquals())) {
                        // TODO: case sensitive or not?
                        // not matched, ignore this
                        continue;
                    }

                    ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
                    builder.setUid(profile);
                    builder.setName(profile);

                    ObjectClass objectClass = new ObjectClass(PROFILE_NAME);
                    builder.setObjectClass(objectClass);

                    ConnectorObject build = builder.build();
                    LOG.ok("ConnectorObject: {0}", build);
                    handler.handle(build);
                } while (entries.nextRow());
            }
        } catch (JCoException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private void executeTableQuery(String tableName, SapFilter query, ResultsHandler handler) {
        try {
            // find all or find by key

            JCoFunction function = destination.getRepository().getFunction("RFC_GET_TABLE_ENTRIES");
            if (function == null)
                throw new RuntimeException("RFC_GET_TABLE_ENTRIES not found in SAP.");

            function.getImportParameterList().setValue("TABLE_NAME", tableName);
            // find by key
            if (query != null && query.getBasicByNameEquals() != null) {
                function.getImportParameterList().setValue("GEN_KEY", query.getBasicByNameEquals());
                LOG.ok("query by Key: " + query.getBasicByNameEquals() + " on table: " + tableName);
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

                    for (Map.Entry<String, Integer> entry : configuration.getTableMetadatas().get(tableName).entrySet()) {
                        String column = entry.getKey();
                        Integer length = entry.getValue();

                        String columnValue = value.substring(index, index + length).trim();
                        // ignore columns, what is selected as :IGNORE
                        if (!configuration.getTableIgnores().get(tableName).contains(column)) {
                            addAttr(builder, column, columnValue);
                        }
                        if (configuration.getTableKeys().get(tableName).contains(column)) {
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
                    if (StringUtil.isEmpty(concatenatedKey.toString())) {
                        LOG.warn("ignoring empty key: " + concatenatedKey);
                        continue;
                    }

                    builder.setUid(concatenatedKey.toString());
                    builder.setName(concatenatedKey.toString());

                    ObjectClass objectClass = new ObjectClass(configuration.getTableAliases().get(tableName));
                    builder.setObjectClass(objectClass);

                    ConnectorObject build = builder.build();
                    LOG.ok("ConnectorObject: {0}", build);
                    handler.handle(build);

                } while (entries.nextRow());
            }
        } catch (JCoException e) {
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private void executeAccountQuery(SapFilter query, ResultsHandler handler, OperationOptions options) {
        try {
            // find by NAME (or UID - same as name)
            if (query != null && query.byNameEquals() != null) {

                JCoFunction function = destination.getRepository().getFunction("BAPI_USER_GET_DETAIL");
                function.getImportParameterList().setValue(USERNAME, query.byNameEquals());
                executeFunction(function);

                JCoFunction userLoginInfoFunc = runUserLoginInfoFunction(query.byNameEquals());

                ConnectorObject connectorObject = convertUserToConnectorObject(function, userLoginInfoFunc);
                handler.handle(connectorObject);

            } // find by name contains
            else if (query != null && query.byNameContains() != null) {
                JCoFunction function = destination.getRepository().getFunction("BAPI_USER_GETLIST");
                JCoTable exp = function.getTableParameterList().getTable("SELECTION_EXP");
                exp.appendRow();
                exp.setValue("PARAMETER", USERNAME);
                exp.setValue("OPTION", "CP");
                exp.setValue("LOW", query.byNameContains());

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
                        functionDetail.getImportParameterList().setValue(USERNAME, userList.getString(USERNAME));

                        executeFunction(functionDetail);

                        JCoFunction userLoginInfoFunc = runUserLoginInfoFunction(userList.getString(USERNAME));

                        ConnectorObject connectorObject = convertUserToConnectorObject(functionDetail, userLoginInfoFunc);
                        boolean finish = !handler.handle(connectorObject);
                        if (finish)
                            break;

                    } while (userList.nextRow());
                }

                // find all or advanced filtering
            } else {
                JCoFunction function = destination.getRepository().getFunction("BAPI_USER_GETLIST");

                final Integer pageSize = options == null ? null : options.getPageSize(); // 0 = return all
                if (pageSize != null && pageSize > 0) {
                    // Paged Search
                    final Integer pagedResultsOffset =
                            null != options.getPagedResultsOffset() ? Math.max(0, options
                                    .getPagedResultsOffset()) : 0;
                    function.getImportParameterList().setValue("MAX_ROWS", pagedResultsOffset + pageSize);
                    prepareFilters(function, query);
                    LOG.ok("SELECTION_EXP: " + function.getTableParameterList().getTable("SELECTION_EXP").toXML());
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
                            functionDetail.getImportParameterList().setValue(USERNAME, userList.getString(USERNAME));

                            executeFunction(functionDetail);

                            JCoFunction userLoginInfoFunc = runUserLoginInfoFunction(userList.getString(USERNAME));

                            ConnectorObject connectorObject = convertUserToConnectorObject(functionDetail, userLoginInfoFunc);
                            if (handler.handle(connectorObject)) {
                                handled++;
                            } else {
                                LOG.ok("finishing read");
                                break;
                            }
                        } while (userList.nextRow());
                    }

                } else {
                    // not paged search
                    prepareFilters(function, query);
                    LOG.ok("SELECTION_EXP: " + function.getTableParameterList().getTable("SELECTION_EXP").toXML());
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
                            functionDetail.getImportParameterList().setValue(USERNAME, userList.getString(USERNAME));

                            executeFunction(functionDetail);

                            JCoFunction userLoginInfoFunc = runUserLoginInfoFunction(userList.getString(USERNAME));

                            ConnectorObject connectorObject = convertUserToConnectorObject(functionDetail, userLoginInfoFunc);
                            boolean finish = !handler.handle(connectorObject);
                            if (finish) {
                                LOG.ok("finishing read");
                                break;
                            }

                        } while (userList.nextRow());
                    }
                }
            }

        } catch (ConnectorException e) {
            // known exceptions - don't change exception type
            throw e;
        } catch (Exception e) {
            // not known exceptions, change it to ConnectorIOException to show error message in midPoint
            throw new ConnectorIOException(e.getMessage(), e);
        }
    }

    private void prepareFilters(JCoFunction function, SapFilter query) {
        if (query == null) {
            return; // empty filter
        }

        JCoTable exp = function.getTableParameterList().getTable("SELECTION_EXP");
        exp.appendRow();

        if (query.getLogicalOperation() != null) {
            exp.setValue("LOGOP", query.getLogicalOperation());
            exp.setValue("ARITY", query.getArity());

            for (SapFilter expr : query.getExpressions()) {
                prepareFilters(function, expr);
            }
        }
        else {
            exp.setValue("PARAMETER", query.getParameter());
            exp.setValue("OPTION", query.getOption());
            if (query.getField() != null) {
                exp.setValue("FIELD", query.getField());
            }
            exp.setValue("LOW", query.getValue());
        }
    }

    private JCoFunction runUserLoginInfoFunction(String userName) throws JCoException {
        if (!this.configuration.getAlsoReadLoginInfo()) {
            return null;
        }

        JCoFunction function = destination.getRepository().getFunction("SUSR_GET_ADMIN_USER_LOGIN_INFO");
        function.getImportParameterList().setValue("USERID", userName);

        executeFunction(function);

        return function;
    }

    private List<String> executeFunction(JCoFunction function) throws JCoException {
        function.execute(destination);

        return parseReturnMessages(function);
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

    private ConnectorObject convertUserToConnectorObject(JCoFunction function, JCoFunction userLoginInfoFunc) throws JCoException, TransformerException, ParserConfigurationException {
        String userName = function.getImportParameterList().getString(USERNAME);

        ConnectorObjectBuilder builder = new ConnectorObjectBuilder();
        builder.setUid(userName);
        builder.setName(userName);

        getDataFromBapiFunction(function, READ_WRITE_PARAMETERS, builder);
        getDataFromBapiFunction(function, readOnlyParams, builder);

        JCoStructure islocked = function.getExportParameterList().getStructure("ISLOCKED");
        Boolean enable = "U".equals(islocked.getString(LOCAL_LOCK)); // U - unlocked, L - locked
        addAttr(builder, OperationalAttributes.ENABLE_NAME, enable);
        // we don't have BAPI method to unlock only this
        Boolean lock_out = "L".equals(islocked.getString("WRNG_LOGON")); // U - unlocked, L - locked
        addAttr(builder, OperationalAttributes.LOCK_OUT_NAME, lock_out);

        JCoStructure logonData = function.getExportParameterList().getStructure("LOGONDATA");
        Date gltgv = logonData.getDate(GLTGV);
        addAttr(builder, OperationalAttributes.ENABLE_DATE_NAME, gltgv == null ? null : gltgv.getTime());
        Date gltgb = logonData.getDate(GLTGB);
        addAttr(builder, OperationalAttributes.DISABLE_DATE_NAME, gltgb == null ? null : gltgb.getTime());

        // tables and his id's
        for (String tableName : configuration.getTableParameterNames()) {
            Table table = new Table(function.getTableParameterList().getTable(tableName));
            builder.addAttribute(AttributeBuilder.build(tableName, table.getXmls()));

            if (TABLETYPE_PARAMETER_KEYS.containsKey(tableName)) {
                String attribute = TABLETYPE_PARAMETER_KEYS.get(tableName);
                builder.addAttribute(AttributeBuilder.build(tableName + SEPARATOR + TABLETYPE_PARAMETER_KEYS.get(tableName), table.getIds(attribute)));
            }
        }

        if (userLoginInfoFunc != null) {
            JCoParameterList epl = userLoginInfoFunc.getExportParameterList();

            Date lastLogonDate = epl.getDate("LAST_LOGON_DATE");
            addAttr(builder, USER_LOGIN_INFO + SEPARATOR + "LAST_LOGON_DATE", lastLogonDate == null ? null : lastLogonDate.getTime());
            addAttr(builder, USER_LOGIN_INFO + SEPARATOR + "LOCK_STATUS", epl.getString("LOCK_STATUS"));
            addAttr(builder, USER_LOGIN_INFO + SEPARATOR + "PASSWORD_STATUS", epl.getString("PASSWORD_STATUS"));
        }

        ConnectorObject connectorObject = builder.build();

        LOG.ok("convertUserToConnectorObject, user: {0}, connectorObject: {1}",
                userName, connectorObject);

        return connectorObject;
    }

    public List<String> parseReturnMessages(JCoFunction function) {

        JCoParameterList tpl = function.getTableParameterList();

        if (tpl == null)
            return null;

        JCoTable returnList = tpl.getTable("RETURN");
        boolean error = false;
        boolean warning = false;
        List<String> ret = new LinkedList<String>();
        List<String> softErrCodes = Arrays.asList(this.configuration.getNonFatalErrorCodes());

        returnList.firstRow();
        if (returnList.getNumRows() > 0) {
            do {
                String message = returnList.getString("MESSAGE");
                String type = returnList.getString("TYPE");
                String number = returnList.getString("NUMBER");

                if ("E".equals(type)) {
                    error = true;
                    if ("224".equals(number)) { // User XXX already exists , alternative is BAPI_USER_EXISTENCE_CHECK function
                        throw new AlreadyExistsException(message + ", RETURN: " + returnList.toXML());
                    }
                    if ("124".equals(number)) { // User XXX does not exist
                        throw new UnknownUidException(message + ", RETURN: " + returnList.toXML());
                    }
                    if ("187".equals(number)) { // Password is not long enough (minimum length: 8 characters)
                        throw handlePasswordException (new InvalidPasswordException(message + ", RETURN: " + returnList.toXML()));
                    }
                    if ("290".equals(number)) { // Please enter an initial password
                        throw handlePasswordException (new InvalidPasswordException(message + ", RETURN: " + returnList.toXML()));
                    }

                    // check if return code is considered as soft schema error by the connector configuration:
                    if (softErrCodes.stream().anyMatch(err -> err.equals(number))) {
                        throw new InvalidAttributeValueException(message + ", RETURN: " + returnList.toXML());
                    }

                } else if ("W".equals(type)) {
                    warning = true;
                }
                ret.add(type + ":" + number + ":" + message);
            } while (returnList.nextRow());
        }

        if (error) {
            throw new ConnectorException(ret + ", XML representation: \n" + tpl.toXML());
        } else if (warning) {
            if (configuration != null && configuration.getFailWhenWarning()) {
                throw new ConnectorException(ret + ", XML representation: \n" + tpl.toXML());
            } else {
                LOG.warn("SAP returns warning, but was ignored (failWhenWarning != true) : " + ret);
            }
        }

        LOG.ok("Return messages: " + ret + " for function: " + function.getName());
        return ret;
    }

    private <T> void addAttr(ConnectorObjectBuilder builder, String attrName, T attrVal) {
        // null value or "" (empty string) we don't return
        boolean isNull = false;
        if (attrVal == null) {
            isNull = true;
        } else if (attrVal instanceof String && StringUtil.isEmpty((String) attrVal)) {
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

        function.getImportParameterList().setValue(USERNAME, userName);
        // Alternative logon name for the user. This can be used in some logon scenarios instead of the SAP user name.
//        function.getImportParameterList().getStructure("ALIAS").setValue("USERALIAS",userName);

        // set custom attributes
        setUserCustomAttributes(attributes, function.getImportParameterList(), false);

        // set other attributes
        setGenericAttributes(attributes, function.getImportParameterList(), false);

        // set table type attributes
        setTableTypeAttributes(attributes, function.getTableParameterList(), function.getImportParameterList(), false);

        // handle password & execute create user
        handlePassword(function, attributes, userName, false, true);

        String savedUserName = function.getImportParameterList().getString(USERNAME);
        LOG.info("Saved UserName: {0}, importParameterList: {1}, importParameterList: {2}", savedUserName, function.getImportParameterList().toXML(), function.getTableParameterList().toXML());

        // assign ACTIVITYGROUPS if needed
        assignActivityGroups(attributes, userName);

        // assign PROFILES if needed
        assignProfiles(attributes, userName);

        // enable/disable/unlock user
        handleActivation(attributes, userName);

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

            // check password max length
            if (configuration.getFailWhenTruncating() && pwd.toString().length() > sapAttributesLength.get(BAPIPWD)) {
                throw handlePasswordException(new InvalidPasswordException("Attribute " + passwordAttribute + " with value XXXX (secured) is longer then maximum length in SAP " + sapAttributesLength.get(passwordAttribute + ", failWhenTruncating=enabled")));
            }

            // validate password in SAP
            validatePassword(pwd.toString());

            // if we set two times the same password, SAP forbid to set the same password for second time - password policy
            // so we first check if new password is not already set to SAP and if yes, we can ignore password update
            if (updateUser && isPasswordAlreadySet(userName, pwd.toString())) {
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
                    LOG.error("can't change password: " + e, e);
                    if (e.getGroup() == 126 && "193".equalsIgnoreCase(e.getMessageNumber())) {
                        throw handlePasswordException(new PasswordExpiredException("Choose a password that is different from your last & passwords for user " + userName + ": " + e, e));
                    } else if (e.getGroup() == 126 && "190".equalsIgnoreCase(e.getMessageNumber())) {
                        LOG.warn("User " + userName + " is locked after too many failed logins, try to unlocking");
                        // try unlock user
                        JCoFunction functionUnlock = destination.getRepository().getFunction("BAPI_USER_UNLOCK");
                        if (functionUnlock == null)
                            throw new RuntimeException("BAPI_USER_UNLOCK not found in SAP.");

                        functionUnlock.getImportParameterList().setValue(USERNAME, userName);

                        try {
                            executeFunction(functionUnlock);
                            // execute change password again
                            executeFunction(changePassFunction);
                        } catch (JCoException eu) {
                            throw new PermissionDeniedException("Password is not yet changeable remotely, please unlock user " + userName + " manually in GUI: " + e, e);
                        }
                    }
                }
                LOG.ok("User " + userName + " don't need to change his password on next login.");
            } else {
                // set password - need change it at next logon
                JCoStructure passwordStructure = function.getImportParameterList().getStructure("PASSWORD");
                passwordStructure.setValue(BAPIPWD, pwd.toString());
                if (updateUser) {
                    JCoStructure passwordStructureX = function.getImportParameterList().getStructure("PASSWORDX");
                    passwordStructureX.setValue(BAPIPWD, SELECT);
                }
                // execute create or update user
                executeFunction(function);
                LOG.ok("User " + userName + " need to change his password on next login.");
            }

            // hack for Information message: Number of failed password login attempts: 1 (see long text)
            isPasswordAlreadySet(userName, pwd.toString());

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
            if (!name.contains(SEPARATOR) || name.startsWith("__") || !sapAttributesType.containsKey(name)) { // for examle __PASSWORD__, or tables
                //LOG.ok("attribute managed elsewhere: {0}", attribute);
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
            //LOG.ok("structureName: {0}, attributeName: {1}, length: {2}, className: {3}, value: {4}", structureName, attributeName, length, classs, value);
            JCoStructure structure = importParameterList.getStructure(structureName);
            structure.setValue(attributeName, value);
            if (select) {
                if ("UCLASSX".equals(structureNameX)) {
                    attributeName = "UCLASS"; // not standard attribute :(
                } else if ("ALIASX".equals(structureNameX)) {
                    attributeName = "BAPIALIAS"; // not standard attribute :(
                }
                LOG.ok("structureNameX: {0}, attributeName: {1}, length: {2}, className: {3}, value: {4}", structureNameX, attributeName, length, classs, SELECT);
                JCoStructure structureX = importParameterList.getStructure(structureNameX);
                if (!contains(CREATE_ONLY_ATTRIBUTES, structureName + SEPARATOR + attributeName)) {
                    structureX.setValue(attributeName, SELECT);
                    updateNeeded = true;
                } else {
                    LOG.warn("Attribute " + attributeName + " in structure " + structureNameX + " is only Creatable (not Updateable), ignoring changing it to " + value);
                }
            }
        }

        return updateNeeded;
    }


    private boolean setTableTypeAttributes(Set<Attribute> attributes, JCoParameterList tableParameterList, JCoParameterList importParameterList, boolean select) throws ClassNotFoundException {
        boolean updateNeeded = false;

        for (Attribute attribute : attributes) {
            String attributeName = attribute.getName();
            boolean notUpdateableTable = true;
            for (String enabledTables : configuration.getTableParameterNames()) {
                // profile and activitygroups are changed over special BAPI methods
                if (attributeName.startsWith(enabledTables) && !attributeName.startsWith(PROFILES) && !attributeName.startsWith(ACTIVITYGROUPS)) {
                    notUpdateableTable = false;
                }
            }
            if (notUpdateableTable) {
                //LOG.ok("not a table attribute : {0}", attribute);
                continue; // not updateable table attribute, managed elsewhere
            }
            // shortlink in Table
            if (attributeName.equals(GROUPS_USERGROUP)) {
                attributeName = GROUPS;
            }

            Table table = null;
            try {
                table = new Table(attributes, attributeName);
            } catch (Exception e) {
                throw new InvalidAttributeValueException("Not parsable " + attributeName + " in attributes " + attributes + ", " + e, e);
            }
            // get table
            JCoTable jCoTable = tableParameterList.getTable(attributeName);
            // put values
            if (table != null && table.size() > 0) {
                //some tables need to set change flag
                JCoStructure structureX = null;
                if (select && TABLES_WITH_CHANGE_FLAG.contains(attributeName)) {
                    String selectAttributeName = attributeName + SELECT;
                    if (attributeName.equals("PARAMETER1")) { // same as PARAMETER
                        selectAttributeName = "PARAMETERX";
                    }
                    structureX = importParameterList.getStructure(selectAttributeName);
                }

                for (Item item : table.getValues()) {
                    jCoTable.appendRow();
                    updateNeeded = true;
                    for (String key : item.getValues().keySet()) {
                        // set values
                        jCoTable.setValue(key, item.getValues().get(key));
                        // set change flags at least one
                        if (structureX != null) {
                            structureX.setValue(key, SELECT);
                        }
                    }
                }
                updateNeeded = true;
            }

            // if we need delete and we need set change fla
            if (table != null && table.isUpdate() && table.size() == 0 && select && TABLES_WITH_CHANGE_FLAG.contains(attributeName)) {
                String selectAttributeName = attributeName + SELECT;
                if (attributeName.equals("PARAMETER1")) { // same as PARAMETER
                    selectAttributeName = "PARAMETERX";
                }

                JCoStructure structureX = importParameterList.getStructure(selectAttributeName);
                JCoFieldIterator fieldIterator = structureX.getFieldIterator();
                while (fieldIterator.hasNextField()) {
                    JCoField field = fieldIterator.nextField();
                    structureX.setValue(field.getName(), SELECT);
                }
                updateNeeded = true;
            }

            // communication data-s has his custom change flag in "ADDCOMX"
            if (table != null && table.isUpdate() && select && COMMUNICATION_DATA_TABLES_WITH_CHANGE_FLAG.contains(attributeName)) {
                // attribute ADDTEL in ADDCOMX is as ADTEL (only single D)
                String shortFieldName = attributeName.replaceFirst("ADD", "AD");
                importParameterList.getStructure("ADDCOMX").setValue(shortFieldName, SELECT);
                updateNeeded = true;
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
                throw new InvalidAttributeValueException("Attribute " + attrName + " with value " + val + " is longer then maximal length in SAP " + length + ", failWhenTruncating=enabled");
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
                function.getImportParameterList().setValue(USERNAME, uid.getUidValue());
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
            throw new PermissionDeniedException("SAP don't support RENAME user from '" + userName + "' to '" + newUserName + "'");
        }

        function.getImportParameterList().setValue(USERNAME, userName);
        // Alternative logon name for the user. This can be used in some logon scenarios instead of the SAP user name.
//        function.getImportParameterList().getStructure("ALIAS").setValue("USERALIAS",userName);

        // set custom attributes
        boolean updateNeededCustom = setUserCustomAttributes(attributes, function.getImportParameterList(), true);

        // set other attributes
        boolean updateNeededGeneric = setGenericAttributes(attributes, function.getImportParameterList(), true);

        // set table type attributes
        boolean updateNeededTable = setTableTypeAttributes(attributes, function.getTableParameterList(), function.getImportParameterList(), true);

        // handle password & execute update user if needed
        boolean updateNeededPassword = handlePassword(function, attributes, userName, true, updateNeededCustom || updateNeededGeneric || updateNeededTable);

        String changedUserName = function.getImportParameterList().getString(USERNAME);
        LOG.info("Changed? {0}, UserName: {1}, importParameterList: {2}, tableParameterList: {3}", (updateNeededCustom || updateNeededGeneric || updateNeededTable || updateNeededPassword), changedUserName, function.getImportParameterList().toXML(), function.getTableParameterList().toXML());

        // assign ACTIVITYGROUPS if needed
        assignActivityGroups(attributes, userName);

        // assign PROFILES if needed
        assignProfiles(attributes, userName);

        // enable/disable/unlock user
        handleActivation(attributes, userName);

        return new Uid(changedUserName);
    }

    private void handleActivation(Set<Attribute> attributes, String userName) throws JCoException {
        Boolean lockOut = getAttr(attributes, OperationalAttributes.LOCK_OUT_NAME, Boolean.class, null);
        Boolean enable = getAttr(attributes, OperationalAttributes.ENABLE_NAME, Boolean.class, null);
        if (lockOut != null) {
            // locking is not supported
            if (lockOut) {
                throw new PermissionDeniedException("LOCK_OUT is " + lockOut + ", supported is only unlock operation (false)");
            }
            // unlocking is supported
            // unlock account after logon locked by incorrect logon attempts
            if (enable != null && enable) {
                // user is unlocked when we run BAPI_USER_UNLOCK to enable it later
                LOG.ok("lockoutStatus is set to: " + lockOut + ", enable: " + enable + ", unlocking account over enable operation");
            } else if (enable != null && !enable) {
                // user will be disabled, but we need to unlock also, we enable it now, and later disable it
                LOG.ok("lockoutStatus is set to: " + lockOut + ", enable: " + enable + ", enabling user to unlock his account and disable it");
                enableOrDisableUser(true, userName);
            } else if (enable == null) {
                // we need to read administrative status, enable account to unlock it and if old status was disabled, disable it
                JCoFunction function = destination.getRepository().getFunction("BAPI_USER_GET_DETAIL");
                function.getImportParameterList().setValue(USERNAME, userName);
                executeFunction(function);

                JCoStructure islocked = function.getExportParameterList().getStructure("ISLOCKED");
                Boolean enabledBefore = "U".equals(islocked.getString(LOCAL_LOCK)); // U - unlocked, L - locked

                LOG.ok("lockoutStatus is set to: " + lockOut + ", enable: " + enable + ", readed administrative status from SAP: " + enabledBefore + ", unlocking acount and setting the same result");
                enableOrDisableUser(true, userName);

                if (!enabledBefore) {
                    enableOrDisableUser(false, userName);
                }
            }
        }

        if (enable != null) {
            enableOrDisableUser(enable, userName);
        }
    }

    private void enableOrDisableUser(boolean enable, String userName) throws JCoException {
        String functionName = enable ? "BAPI_USER_UNLOCK" : "BAPI_USER_LOCK";
        JCoFunction functionLock = destination.getRepository().getFunction(functionName);
        if (functionLock == null)
            throw new RuntimeException(functionName + " not found in SAP.");

        functionLock.getImportParameterList().setValue(USERNAME, userName);
        executeFunction(functionLock);
    }

    private void assignActivityGroups(Set<Attribute> attributes, String userName) throws JCoException {
        Table activityGroups = null;
        try {
            activityGroups = new Table(attributes, ACTIVITYGROUPS);
        } catch (Exception e) {
            throw new InvalidAttributeValueException("Not parsable ACTIVITYGROUPS in attributes " + attributes + ", " + e, e);
        }
        JCoFunction functionAssign = destination.getRepository().getFunction("BAPI_USER_ACTGROUPS_ASSIGN");
        if (functionAssign == null)
            throw new RuntimeException("BAPI_USER_ACTGROUPS_ASSIGN not found in SAP.");

        functionAssign.getImportParameterList().setValue(USERNAME, userName);
        JCoTable activityGroupsTable = functionAssign.getTableParameterList().getTable(ACTIVITYGROUPS);

        if (activityGroups != null && activityGroups.size() > 0) {
            for (Item ag : activityGroups.getValues()) {
                activityGroupsTable.appendRow();
                activityGroupsTable.setValue(AGR_NAME, ag.getByAttribute(AGR_NAME));
                String fromDat = ag.getByAttribute("FROM_DAT");
                if (fromDat != null) {
                    activityGroupsTable.setValue("FROM_DAT", fromDat);
                }
                String toDat = ag.getByAttribute("TO_DAT");
                if (toDat != null) {
                    activityGroupsTable.setValue("TO_DAT", toDat);
                }
                String argText = ag.getByAttribute("AGR_TEXT");
                if (argText != null) {
                    activityGroupsTable.setValue("AGR_TEXT", argText);
                }
                String orgFlag = ag.getByAttribute("ORG_FLAG");
                if (orgFlag != null) {
                    activityGroupsTable.setValue("ORG_FLAG", orgFlag);
                }
            }
        }

        if (activityGroups.isUpdate()) {
            executeFunction(functionAssign);
        }
        LOG.info("ACTGROUPS_ASSIGN modify {0}, TPL: {1}", activityGroups.isUpdate(), functionAssign.getTableParameterList().toXML());
    }

    private void assignProfiles(Set<Attribute> attributes, String userName) throws JCoException {
        Table profiles = null;
        try {
            profiles = new Table(attributes, PROFILES);
        } catch (Exception e) {
            throw new InvalidAttributeValueException("Not parsable PROFILES in attributes " + attributes + ", " + e, e);
        }
        JCoFunction functionAssign = destination.getRepository().getFunction("BAPI_USER_PROFILES_ASSIGN");
        if (functionAssign == null)
            throw new RuntimeException("BAPI_USER_PROFILES_ASSIGN not found in SAP.");

        functionAssign.getImportParameterList().setValue(USERNAME, userName);
        JCoTable profilesTable = functionAssign.getTableParameterList().getTable(PROFILES);

        if (profiles != null && profiles.size() > 0) {
            for (Item ag : profiles.getValues()) {
                profilesTable.appendRow();
                profilesTable.setValue(BAPIPROF, ag.getByAttribute(BAPIPROF));
                String bapipText = ag.getByAttribute("BAPIPTEXT");
                if (bapipText != null) {
                    profilesTable.setValue("BAPIPTEXT", bapipText);
                }
                String bapiType = ag.getByAttribute("BAPITYPE");
                if (bapiType != null) {
                    profilesTable.setValue("BAPITYPE", bapiType);
                }
                String bapiAktps = ag.getByAttribute("BAPIAKTPS");
                if (bapiAktps != null) {
                    profilesTable.setValue("BAPIAKTPS", bapiAktps);
                }
            }
        }

        if (profiles.isUpdate()) {
            executeFunction(functionAssign);
        }
        LOG.info("PROFILES_ASSIGN modify {0}, TPL: {1}", profiles.isUpdate(), functionAssign.getTableParameterList().toXML());
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

    private void syncUser(SyncToken token, SyncResultsHandler handler, OperationOptions options) throws JCoException, ParseException, TransformerException, ParserConfigurationException {
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
                String userName = userList.getString(USERNAME);
                JCoFunction functionDetail = destination.getRepository().getFunction("BAPI_USER_GET_DETAIL");
                functionDetail.getImportParameterList().setValue(USERNAME, userName);

                executeFunction(functionDetail);

                JCoStructure lastmodifiedStructure = functionDetail.getExportParameterList().getStructure("LASTMODIFIED");
                String modDate = lastmodifiedStructure.getString("MODDATE");
                String modTime = lastmodifiedStructure.getString("MODTIME");
                // check not only date in filter, but also time and procee only changed after MODDATE and MODTIME
                Date lastModification = DATE_TIME.parse(modDate + " " + modTime);
                if (lastModification.after(fromToken)) {
                    changed++;

                    JCoFunction userLoginInfoFunc = runUserLoginInfoFunction(userName);

                    ConnectorObject connectorObject = convertUserToConnectorObject(functionDetail, userLoginInfoFunc);

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

    private String generateTempPassword() {
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
            if (e.getGroup() == 126 && "152".equalsIgnoreCase(e.getMessageNumber())) {
                // (126) WRONG_PASSWORD: WRONG_PASSWORD Message 152 of class 00 type E
                return false;
            } else if ((e.getGroup() == 126 && "200".equalsIgnoreCase(e.getMessageNumber())) || e.toString().contains("NO_CHECK_FOR_THIS_USER")) {
                LOG.error("Password logon no longer possible - too many failed attempts: " + e, e);
                return false;
            } else if ((e.getGroup() == 148 && "148".equalsIgnoreCase(e.getMessageNumber())) || e.toString().contains("USER_NOT_ACTIVE")) {
                LOG.warn("User is not active now, but password is correct: " + e, e);
                return true;
            } else if ((e.getGroup() == 126 && "158".equalsIgnoreCase(e.getMessageNumber())) || e.toString().contains("USER_LOCKED")) {
                //(126) USER_LOCKED: USER_LOCKED Message 158 of class 00 type E
                LOG.warn("User is locked, but password is correct: " + e, e);
                return true;
            } else if ((e.getGroup() == 126 && "012".equalsIgnoreCase(e.getMessageNumber())) || e.toString().contains("PASSWORD_EXPIRED")) {
                //(126) PASSWORD_EXPIRED: PASSWORD_EXPIRED Message 012 of class 00 type E
                LOG.warn("Password is expired: " + e, e);
                return true;
            } else {
                LOG.error(e, e.toString());
                throw e;
            }
        }

        return true;

    }

    private void validatePassword(String password) throws JCoException {
        JCoFunction function = destination.getRepository().getFunction("PASSWORD_FORMAL_CHECK");
        if (function == null)
            throw new RuntimeException("PASSWORD_FORMAL_CHECK not found in SAP.");

        function.getImportParameterList().getStructure("PASSWORD").setValue(BAPIPWD, password);

        try {
            function.execute(destination);
        } catch (JCoException e) {
            if (e.getGroup() != 104) {
                if (e.getGroup() != 123) {
                    throw handlePasswordException(new InvalidPasswordException("Password is too simple"));
                }

                LOG.ok("PASSWORD_FORMAL_CHECK is NOT installed");
            }

            LOG.ok("PASSWORD_FORMAL_CHECK is NOT remote enabled");
        }
    }

    private ConnectorException handlePasswordException(InvalidCredentialException e) {
        if (this.configuration.getPwdChangeErrorIsFatal())
            return e;
        else
            return new InvalidAttributeValueException(e); // wrap exception into soft schema exception
    }

}
