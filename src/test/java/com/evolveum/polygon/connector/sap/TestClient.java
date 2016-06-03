package com.evolveum.polygon.connector.sap;

import com.sap.conn.jco.JCoException;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.text.ParseException;
import java.util.*;


/**
 * Created by gpalos on 19. 1. 2016.
 */
public class TestClient {

    static SapConfiguration sapConfiguration;
    static SapConnector sapConnector;

    private static final Log LOG = Log.getLog(TestClient.class);

    static final ObjectClass ACCOUNT_OBJECT_CLASS = new ObjectClass(ObjectClass.ACCOUNT_NAME);
    static final String[] MINIMAL_ACCOUNT_ATTRIBUTE_LIST = {SapConnector.ACTIVITYGROUPS, "LOGONDATA.GLTGB", "ADDRESS.TITLE_P",
            "ADDRESS.FIRSTNAME", "ADDRESS.LASTNAME", "ADDRESS.TITLE_ACA1", "ADDRESS.E_MAIL", "ADDRESS.TEL1_EXT",
            "ADDRESS.TEL1_NUMBR", "ADDRESS.FAX_NUMBER", "ADDRESS.FAX_EXTENS", "ADDRESS.ROOM_NO_P", "ADDRESS.DEPARTMENT",
            "ADDRESS.COMM_TYPE", "ADDRESS.COUNTRY", "ADDRESS.LANGU_P", "ADDRESS.LANGU_ISO", "DEFAULTS.SPLD",
            "UCLASS.LIC_TYPE", "UCLASS.SYSID", "UCLASS.CLIENT", "UCLASS.BNAME_CHARGEABLE", "LOGONDATA.GLTGV"};

    static final String USER_NAME = "Evolveum";

    @BeforeClass
    public static void setUp() throws Exception {
        // load configuration from properties file
        String fileName = "test.properties";
        sapConfiguration = readSapConfigurationFromFile(fileName);

        sapConnector = new SapConnector();
        sapConnector.init(sapConfiguration);
    }

    private static SapConfiguration readSapConfigurationFromFile(String fileName) throws IOException {
        final Properties properties = new Properties();
        InputStream inputStream = TestClient.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream == null) {
            throw new IOException("Sorry, unable to find " + fileName);
        }
        properties.load(inputStream);

        sapConfiguration = new SapConfiguration();
        sapConfiguration.setHost(properties.getProperty("host"));
        if (properties.containsKey("port")) {
            sapConfiguration.setPort(properties.getProperty("port"));
        }
        sapConfiguration.setUser(properties.getProperty("user"));
        sapConfiguration.setPlainPassword(properties.getProperty("password"));
        sapConfiguration.setSystemId("SRS");
        if (properties.containsKey("systemNumber")) {
            sapConfiguration.setSystemNumber(properties.getProperty("systemNumber"));
        }
        sapConfiguration.setClient(properties.getProperty("client"));
        if (properties.containsKey("lang")) {
            sapConfiguration.setLang(properties.getProperty("lang"));
        }
        if (properties.containsKey("failWhenTruncating")) {
            sapConfiguration.setFailWhenTruncating(Boolean.parseBoolean(properties.getProperty("failWhenTruncating")));
        }
        if (properties.containsKey("failWhenWarning")) {
            sapConfiguration.setFailWhenWarning(Boolean.parseBoolean(properties.getProperty("failWhenWarning")));
        }
        if (properties.containsKey("useTransaction")) {
            sapConfiguration.setUseTransaction(Boolean.parseBoolean(properties.getProperty("useTransaction")));
        }
        if (properties.containsKey("testBapiFunctionPermission")) {
            sapConfiguration.setTestBapiFunctionPermission(Boolean.parseBoolean(properties.getProperty("testBapiFunctionPermission")));
        }
        if (properties.containsKey("tables")) {
            String[] tables = properties.getProperty("tables").split(";");
            sapConfiguration.setTables(tables);
        }
        if (properties.containsKey("tableParameterNames")) {
            String[] tableParameterNames = properties.getProperty("tableParameterNames").split(";");
            sapConfiguration.setTableParameterNames(tableParameterNames);
        }


        return sapConfiguration;
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (sapConnector != null) {
            sapConnector.dispose();
        }
    }

    @Test
    public void testConn() throws RemoteException {
        sapConnector.test();
    }

    @Test
    public void testSchema() throws Exception {
        Schema schema = sapConnector.schema();
        LOG.info("generated schema is:\n{0}", schema);
        checkNeededAttributes(schema);
    }

    private static void checkNeededAttributes(Schema schema) throws Exception {
        List<String> found = new LinkedList<String>();
        for (ObjectClassInfo objectClassInfo : schema.getObjectClassInfo()) {
            for (AttributeInfo attributeInfo : objectClassInfo.getAttributeInfo()) {
                found.add(attributeInfo.getName());
            }
        }

        List<String> notFound = new LinkedList<String>();
        for (String attribute : MINIMAL_ACCOUNT_ATTRIBUTE_LIST) {
            if (!found.contains(attribute)) {
                notFound.add(attribute);
            }
        }
        if (notFound.size() > 0) {
            throw new Exception("these required atrributes are not in schema: " + notFound);
        }
    }

    @Test
    public void testFindAllUsers() throws RemoteException {
        SapFilter query = null;
        final int[] count = {0};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                count[0]++;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);
        System.out.println("count[0] = " + count[0]);
        Assert.assertTrue(count[0] > 0, "Find all users return zero users");
    }

    @Test
    public void testFindPagedUsers() throws RemoteException {
        SapFilter query = null;
        final int[] count = {0};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                count[0]++;
                return true; // continue
            }
        };
        int pageSize = 10;
        Map<String, Object> operationOptions = new HashMap<String, Object>();
        operationOptions.put(OperationOptions.OP_PAGE_SIZE, pageSize);
        operationOptions.put(OperationOptions.OP_PAGED_RESULTS_OFFSET, 2);
        OperationOptions options = new OperationOptions(operationOptions);
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        Assert.assertTrue(count[0] == pageSize, "Find " + pageSize + " users return " + count[0] + " users");
    }
    @Test
    public void testCreateMinimal() throws RemoteException {
        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));
        GuardedString password = new GuardedString("Test1234".toCharArray());
        attributes.add(AttributeBuilder.build(OperationalAttributes.PASSWORD_NAME, password));

        OperationOptions operationOptions = null;
        sapConnector.create(ACCOUNT_OBJECT_CLASS, attributes, operationOptions);

        testFindUser();
    }

    @Test(dependsOnMethods = {"testCreateFull"})
    public void testFindUser() throws RemoteException {
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final boolean[] found = {false};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = true;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        Assert.assertTrue(found[0], "User " + USER_NAME + " not found");
    }

    @Test(dependsOnMethods = {"testCreateMinimal"})
    public void testFindContains() throws RemoteException {
        SapFilter query = new SapFilter();
        query.setByNameContains("volv"); // evolveum
        final boolean[] found = {false};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = true;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        Assert.assertTrue(found[0], "User containing " + query.getByNameContains() + " not found");
    }

    @Test(dependsOnMethods = {"testCreateMinimal"})
    public void testDelete() throws RemoteException {
        OperationOptions operationOptions = null;
        sapConnector.delete(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), operationOptions);

        boolean deleted = false;
        try {
            SapFilter query = new SapFilter();
            query.setByName(USER_NAME);
            final boolean[] found = {false};
            ResultsHandler handler = new ResultsHandler() {
                @Override
                public boolean handle(ConnectorObject connectorObject) {
                    found[0] = true;
                    return true; // continue
                }
            };
            OperationOptions options = null;
            sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);
        } catch (UnknownUidException e) {
            deleted = true;
        }

        Assert.assertTrue(deleted, "User " + USER_NAME + " was not deleted");
    }


    @Test(dependsOnMethods = {"testDelete"})
    public void testCreateFull() throws RemoteException {
        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));
        GuardedString password = new GuardedString("Test1234".toCharArray());
        attributes.add(AttributeBuilder.build(OperationalAttributes.PASSWORD_NAME, password));

        String title = "Mr.";
        String firstName = "James";
        String lastName = "Evolveum";
        String titleAca1 = "Dr.";
        String email = "test@evolveum.com";
        String telExt = "123";
        String telNumber = "345";
        String faxNumber = "567";
        String faxExtens = "789";
        String room = "12B";
        String department = "IT";
        String commType = "INT";
        String country = "DE";
        String languP = "D";
        String languIso = "DE";
        attributes.add(AttributeBuilder.build("ADDRESS.TITLE_P", title));
        attributes.add(AttributeBuilder.build("ADDRESS.FIRSTNAME", firstName));
        attributes.add(AttributeBuilder.build("ADDRESS.LASTNAME", lastName));
        attributes.add(AttributeBuilder.build("ADDRESS.TITLE_ACA1", titleAca1));
        attributes.add(AttributeBuilder.build("ADDRESS.E_MAIL", email));
        attributes.add(AttributeBuilder.build("ADDRESS.TEL1_EXT", telExt));
        attributes.add(AttributeBuilder.build("ADDRESS.TEL1_NUMBR", telNumber));
        attributes.add(AttributeBuilder.build("ADDRESS.FAX_NUMBER", faxNumber));
        attributes.add(AttributeBuilder.build("ADDRESS.FAX_EXTENS", faxExtens));
        attributes.add(AttributeBuilder.build("ADDRESS.ROOM_NO_P", room));
        attributes.add(AttributeBuilder.build("ADDRESS.DEPARTMENT", department));
        attributes.add(AttributeBuilder.build("ADDRESS.COMM_TYPE", commType));
        attributes.add(AttributeBuilder.build("ADDRESS.COUNTRY", country));
        attributes.add(AttributeBuilder.build("ADDRESS.LANGU_P", languP));
        attributes.add(AttributeBuilder.build("ADDRESS.LANGU_ISO", languIso));

        String spld = "LOCL";
        attributes.add(AttributeBuilder.build("DEFAULTS.SPLD", spld));

        String licType = "55";
        attributes.add(AttributeBuilder.build("UCLASS.LIC_TYPE", licType));
        attributes.add(AttributeBuilder.build("UCLASS.SYSID", ""));//E:284:Parameter SYSID must not be filled for this user type
        attributes.add(AttributeBuilder.build("UCLASS.CLIENT", ""));//E:284:Parameter CLIENT must not be filled for this user type
        attributes.add(AttributeBuilder.build("UCLASS.BNAME_CHARGEABLE", ""));//E:284:Parameter BNAME_CHARGEABLE must not be filled for this user type

        boolean enable = false;
        attributes.add(AttributeBuilder.build(OperationalAttributes.ENABLE_NAME, enable));

        String activityGroup = "ZBC_ADM_BENUTZERADMINISTRATOR";
        attributes.add(AttributeBuilder.build(SapConnector.ACTIVITYGROUPS__ARG_NAME, activityGroup));

        Date enableDate = new GregorianCalendar(2016, 1, 1).getTime();
        Date disableDate = new GregorianCalendar(2016, 2, 31).getTime();
        attributes.add(AttributeBuilder.build(OperationalAttributes.ENABLE_DATE_NAME, enableDate.getTime()));// LOGONDATA.GLTGV
        attributes.add(AttributeBuilder.build(OperationalAttributes.DISABLE_DATE_NAME, disableDate.getTime()));//LOGONDATA.GLTGB

        // create it
        OperationOptions operationOptions = null;
        sapConnector.create(ACCOUNT_OBJECT_CLASS, attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        Assert.assertTrue(user != null, "Created user " + USER_NAME + " not found");

        Assert.assertEquals(user.getAttributeByName("ADDRESS.TITLE_P").getValue().get(0), title);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.FIRSTNAME").getValue().get(0), firstName);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.LASTNAME").getValue().get(0), lastName);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.TITLE_ACA1").getValue().get(0), titleAca1);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.E_MAIL").getValue().get(0), email);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.TEL1_EXT").getValue().get(0), telExt);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.TEL1_NUMBR").getValue().get(0), telNumber);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.FAX_NUMBER").getValue().get(0), faxNumber);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.FAX_EXTENS").getValue().get(0), faxExtens);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.ROOM_NO_P").getValue().get(0), room);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.DEPARTMENT").getValue().get(0), department);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.COMM_TYPE").getValue().get(0), commType);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.COUNTRY").getValue().get(0), country);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.LANGU_P").getValue().get(0), languP);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.LANGU_ISO").getValue().get(0), languIso);

        Assert.assertEquals(user.getAttributeByName("DEFAULTS.SPLD").getValue().get(0), spld);

        Assert.assertEquals(user.getAttributeByName("UCLASS.LIC_TYPE").getValue().get(0), licType);
        // empty not send
//        Assert.assertEquals(user.getAttributeByName("UCLASS.SYSID").getValue().get(0), "");
//        Assert.assertEquals(user.getAttributeByName("UCLASS.CLIENT").getValue().get(0), "");
//        Assert.assertEquals(user.getAttributeByName("UCLASS.BNAME_CHARGEABLE").getValue().get(0), "");

        // special attributes
        Assert.assertEquals(user.getAttributeByName(OperationalAttributes.ENABLE_NAME).getValue().get(0), enable);

        Assert.assertEquals(user.getAttributeByName(SapConnector.ACTIVITYGROUPS__ARG_NAME).getValue().get(0), activityGroup);

        Assert.assertEquals(user.getAttributeByName(OperationalAttributes.ENABLE_DATE_NAME).getValue().get(0), enableDate.getTime());
        Assert.assertEquals(user.getAttributeByName(OperationalAttributes.DISABLE_DATE_NAME).getValue().get(0), disableDate.getTime());
    }

    @Test(dependsOnMethods = {"testCreateFull"})
    public void testUpdateFull() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));
        GuardedString password = new GuardedString("Test5678".toCharArray());
        attributes.add(AttributeBuilder.build(OperationalAttributes.PASSWORD_NAME, password));

        String title = "Mr.";
        String firstName = "Kate";
        String lastName = "Evolveum midPoint";
        String titleAca1 = "Dr.";
        String email = "testUpdate@evolveum.com";
        String telExt = "1234";
        String telNumber = "3456";
        String faxNumber = "5678";
        String faxExtens = "7890";
        String room = "12C";
        String department = "Sales";
        String commType = "";
        String country = "";
        String languP = "E";
        String languIso = "EN";
        attributes.add(AttributeBuilder.build("ADDRESS.TITLE_P", "Mr."));
        attributes.add(AttributeBuilder.build("ADDRESS.FIRSTNAME", firstName));
        attributes.add(AttributeBuilder.build("ADDRESS.LASTNAME", lastName));
        attributes.add(AttributeBuilder.build("ADDRESS.TITLE_ACA1", titleAca1));
        attributes.add(AttributeBuilder.build("ADDRESS.E_MAIL", email));
        attributes.add(AttributeBuilder.build("ADDRESS.TEL1_EXT", telExt));
        attributes.add(AttributeBuilder.build("ADDRESS.TEL1_NUMBR", telNumber));
        attributes.add(AttributeBuilder.build("ADDRESS.FAX_NUMBER", faxNumber));
        attributes.add(AttributeBuilder.build("ADDRESS.FAX_EXTENS", faxExtens));
        attributes.add(AttributeBuilder.build("ADDRESS.ROOM_NO_P", room));
        attributes.add(AttributeBuilder.build("ADDRESS.DEPARTMENT", department));
        attributes.add(AttributeBuilder.build("ADDRESS.COMM_TYPE", commType)); // OR RML
//        attributes.add(AttributeBuilder.build("ADDRESS.COUNTRY", country)); // not changable?
        attributes.add(AttributeBuilder.build("ADDRESS.LANGU_P", languP));
//        attributes.add(AttributeBuilder.build("ADDRESS.LANGU_ISO", languIso)); // not changable?
        attributes.add(AttributeBuilder.build("ADDRESS.LANGUP_ISO", languIso));

        String spld = "LOCL";
        attributes.add(AttributeBuilder.build("DEFAULTS.SPLD", spld));

        String licType = "11";
        String sysid = "E01";
        String client = "200";
        String bnameChargeable = "BEK.MI";
        attributes.add(AttributeBuilder.build("UCLASS.LIC_TYPE", licType));
        attributes.add(AttributeBuilder.build("UCLASS.SYSID", sysid));
        attributes.add(AttributeBuilder.build("UCLASS.CLIENT", client));
        attributes.add(AttributeBuilder.build("UCLASS.BNAME_CHARGEABLE", bnameChargeable));

        boolean enable = true;
        attributes.add(AttributeBuilder.build(OperationalAttributes.ENABLE_NAME, enable));

        String activityGroup = "ZBC_ADM_ENTWICKLER_ANZEIGE";
        attributes.add(AttributeBuilder.build(SapConnector.ACTIVITYGROUPS__ARG_NAME, activityGroup));

        Date enableDate = new GregorianCalendar(2016, 0, 1).getTime();
        Date disableDate = new GregorianCalendar(2017, 12, 31).getTime();
        attributes.add(AttributeBuilder.build(OperationalAttributes.ENABLE_DATE_NAME, enableDate.getTime()));// LOGONDATA.GLTGV
        attributes.add(AttributeBuilder.build(OperationalAttributes.DISABLE_DATE_NAME, disableDate.getTime()));//LOGONDATA.GLTGB

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        Assert.assertTrue(user != null, "User " + USER_NAME + " not found");

        Assert.assertEquals(user.getAttributeByName("ADDRESS.TITLE_P").getValue().get(0), title);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.FIRSTNAME").getValue().get(0), firstName);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.LASTNAME").getValue().get(0), lastName);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.TITLE_ACA1").getValue().get(0), titleAca1);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.E_MAIL").getValue().get(0), email);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.TEL1_EXT").getValue().get(0), telExt);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.TEL1_NUMBR").getValue().get(0), telNumber);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.FAX_NUMBER").getValue().get(0), faxNumber);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.FAX_EXTENS").getValue().get(0), faxExtens);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.ROOM_NO_P").getValue().get(0), room);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.DEPARTMENT").getValue().get(0), department);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.COMM_TYPE"), null); // empty is null
//        Assert.assertEquals(user.getAttributeByName("ADDRESS.COUNTRY").getValue().get(0), country);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.LANGU_P").getValue().get(0), languP);
//        Assert.assertEquals(user.getAttributeByName("ADDRESS.LANGU_ISO").getValue().get(0), languIso);
        Assert.assertEquals(user.getAttributeByName("ADDRESS.LANGUP_ISO").getValue().get(0), languIso);

        Assert.assertEquals(user.getAttributeByName("DEFAULTS.SPLD").getValue().get(0), spld);

        Assert.assertEquals(user.getAttributeByName("UCLASS.LIC_TYPE").getValue().get(0), licType);
        Assert.assertEquals(user.getAttributeByName("UCLASS.SYSID").getValue().get(0), sysid);
        Assert.assertEquals(user.getAttributeByName("UCLASS.CLIENT").getValue().get(0), client);
        Assert.assertEquals(user.getAttributeByName("UCLASS.BNAME_CHARGEABLE").getValue().get(0), bnameChargeable);

        // special attributes
        Assert.assertEquals(user.getAttributeByName(OperationalAttributes.ENABLE_NAME).getValue().get(0), enable);

        Assert.assertEquals(user.getAttributeByName(SapConnector.ACTIVITYGROUPS__ARG_NAME).getValue().get(0), activityGroup);

        Assert.assertEquals(user.getAttributeByName(OperationalAttributes.ENABLE_DATE_NAME).getValue().get(0), enableDate.getTime());
        Assert.assertEquals(user.getAttributeByName(OperationalAttributes.DISABLE_DATE_NAME).getValue().get(0), disableDate.getTime());
    }

    @Test(dependsOnMethods = {"testEnableUser"})
    public void testDisableUser() throws RemoteException {
        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));
        attributes.add(AttributeBuilder.build(OperationalAttributes.ENABLE_NAME, false));

        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);
        // checked in testCreateFull
    }

    @Test(dependsOnMethods = {"testCreateFull"})
    public void testEnableUser() throws RemoteException {
        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));
        attributes.add(AttributeBuilder.build(OperationalAttributes.ENABLE_NAME, true));

        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);
        // checked in testUpdateFull
    }

    @Test(dependsOnMethods = {"testEnableUser"})
    public void testChangePassword() throws IOException {
        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));
        String newPassword = "Test5678";
        GuardedString password = new GuardedString(newPassword.toCharArray());
        attributes.add(AttributeBuilder.build(OperationalAttributes.PASSWORD_NAME, password));

        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        String fileName = "testChangePass.properties";
        SapConfiguration sapConf = null;
        sapConf = readSapConfigurationFromFile(fileName);
        SapConnector sapConn = new SapConnector();
        try {
            sapConn.init(sapConf);
            sapConn.test();
        } catch (Exception e)
        {
            // authentificated, but don't have privileges
            if (!e.toString().contains("No RFC authorization for function module RFCPING")) {
                throw e;
            }
        }
    }

    @Test(dependsOnMethods = {"testCreateFull"})
    public void testSync() throws IOException {
        SyncToken syncToken = sapConnector.getLatestSyncToken(ACCOUNT_OBJECT_CLASS);
        testChangePassword();
        final boolean[] changeDetected = {false};
        SyncResultsHandler syncResultsHandler = new SyncResultsHandler() {
            @Override
            public boolean handle(SyncDelta syncDelta) {
                System.out.println("syncDelta = " + syncDelta);
                changeDetected[0] = true;
                return true;
            }
        };
        sapConnector.sync(ACCOUNT_OBJECT_CLASS, syncToken, syncResultsHandler, null);

        Assert.assertEquals(changeDetected[0], true);
    }

    @Test
    public void testFindAllActivityGroups() throws RemoteException {
        SapFilter query = null;
        final int[] count = {0};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                System.out.println("connectorObject = " + connectorObject);
                count[0]++;
                return true; // continue
            }
        };
        OperationOptions options = null;
        ObjectClass objectClass = new ObjectClass("ACTIVITYGROUP");
        sapConnector.executeQuery(objectClass, query, handler, options);
        LOG.info("count: "+count[0]);
        Assert.assertTrue(count[0] > 0, "Find all roles return zero lines");
    }


    @Test(dependsOnMethods = {"testCreateFull"})
    public void testAliasAndReadOnlyAttribute() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        String township = "read_only";
        String alias = "ALIAS";
        attributes.add(AttributeBuilder.build("ADDRESS.TOWNSHIP", township));
        attributes.add(AttributeBuilder.build("ALIAS.USERALIAS", alias));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        Assert.assertTrue(user != null, "Created user " + USER_NAME + " not found");

        // TOWNSHIP is not updateable
        String townshipOriginal = "";
        Assert.assertEquals(user.getAttributeByName("ADDRESS.TOWNSHIP"), null);

        // alias is updateable over ALIASX.BAPIALIAS
        Assert.assertEquals(user.getAttributeByName("ALIAS.USERALIAS").getValue().get(0), alias);

    }

    @Test
    public void testParseItemFromXml() throws IOException, SAXException, ParserConfigurationException {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><item><AGR_NAME>ZBC_ADM_BATCH_ADMIN</AGR_NAME><FROM_DAT>2010-03-25</FROM_DAT><TO_DAT>9999-12-31</TO_DAT><AGR_TEXT>Background Processing Administrator</AGR_TEXT><ORG_FLAG/></item>";
        Item item = new Item(xml, true, "ACTIVITYGROUPS");
        System.out.println("item = " + item.getData());
        System.out.println("item.getByAttribute(\"BAPIPROF\") = " + item.getByAttribute("BAPIPROF"));
    }


    @Test(dependsOnMethods = {"testCreateFull"})
    public void testSetSimpleAcitivtyGroups() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        String activityGroup = "ZBC_ADM_ENTWICKLER_ANZEIGE";
        String attribute = SapConnector.ACTIVITYGROUPS__ARG_NAME;
        attributes.add(AttributeBuilder.build(attribute, activityGroup));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().get(0), activityGroup);
    }

    @Test(dependsOnMethods = {"testCreateFull"})
    public void testSetXmlAcivityGroups() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        String activityGroup = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><item><AGR_NAME>ZBC_ADM_BATCH_ADMIN</AGR_NAME><FROM_DAT>2010-03-25</FROM_DAT><TO_DAT>9999-12-31</TO_DAT><AGR_TEXT>Background Processing Administrator</AGR_TEXT><ORG_FLAG/></item>";
        String attribute = SapConnector.ACTIVITYGROUPS;
        attributes.add(AttributeBuilder.build(attribute, activityGroup));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().get(0), activityGroup);
    }


    @Test(dependsOnMethods = {"testSetXmlAcivityGroups"})
    public void testDeleteAcitivtyGroups() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        // empty activity group
        attributes.add(AttributeBuilder.build(SapConnector.ACTIVITYGROUPS));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info("ACTIVITYGROUPS: {0}", user.getAttributeByName(SapConnector.ACTIVITYGROUPS).getValue());
        Assert.assertEquals(user.getAttributeByName(SapConnector.ACTIVITYGROUPS).getValue().size(), 0);
    }

    @Test
    public void testFindAllProfiles() throws RemoteException {
        SapFilter query = null;
        final int[] count = {0};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                System.out.println("connectorObject = " + connectorObject);
                count[0]++;
                return true; // continue
            }
        };
        OperationOptions options = null;
        ObjectClass objectClass = new ObjectClass(SapConnector.PROFILE_NAME);
        sapConnector.executeQuery(objectClass, query, handler, options);
        LOG.info("count: "+count[0]);
        Assert.assertTrue(count[0] > 0, "Find all profiles return zero lines");
    }


    @Test(dependsOnMethods = {"testCreateFull"})
    public void testSetSimpleProfiles() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        String profile = "B_ALE_ALL";
        String attribute = SapConnector.PROFILES_BAPIPROF;
        attributes.add(AttributeBuilder.build(attribute, profile));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().get(0), profile);
    }

    @Test(dependsOnMethods = {"testCreateFull"})
    public void testSetXmlProfiles() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        String profile = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><item><BAPIPROF>B_ALE_GRP_AD</BAPIPROF><BAPIPTEXT>All Authorizations (W/O Object Maint. in Non-Owner System)</BAPIPTEXT><BAPITYPE>S</BAPITYPE><BAPIAKTPS>A</BAPIAKTPS></item>";
        String attribute = SapConnector.PROFILES;
        attributes.add(AttributeBuilder.build(attribute, profile));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().get(0), profile);
    }


    @Test(dependsOnMethods = {"testSetXmlProfiles"})
    public void testDeleteProfiles() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        // empty activity group
        String attribute = SapConnector.PROFILES_BAPIPROF;
        attributes.add(AttributeBuilder.build(attribute));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
//        String defaultProfile = "T-E1010108";
//        Assert.assertEquals(user.getAttributeByName(attribute).getValue().get(0), defaultProfile);
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().size(), 0);
    }

    @Test
    public void testFindAllGroups() throws RemoteException {
        SapFilter query = null;
        final int[] count = {0};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                System.out.println("connectorObject = " + connectorObject);
                count[0]++;
                return true; // continue
            }
        };
        OperationOptions options = null;
        ObjectClass objectClass = new ObjectClass("GROUP");
        sapConnector.executeQuery(objectClass, query, handler, options);
        LOG.info("count: "+count[0]);

        Assert.assertTrue(count[0] > 0, "Find all groups return zero lines");
    }

    @Test(dependsOnMethods = {"testCreateFull"})
    public void testSetSimpleGroups() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        String group = "BLS-AL"; //"SUPER";
        String attribute = SapConnector.GROUPS_USERGROUP;
        attributes.add(AttributeBuilder.build(attribute, group));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().get(0), group);
    }

    @Test(dependsOnMethods = {"testSetSimpleGroups"})
    public void testDeleteGroups() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        // empty group
        String attribute = SapConnector.GROUPS_USERGROUP;
        attributes.add(AttributeBuilder.build(attribute));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().size(), 0);
    }

    @Test(dependsOnMethods = {"testCreateFull"})
    public void testSetXmlParameter() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        String parameter = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><item><PARID>BCS_ADMIN_TREE</PARID><PARVA/><PARTXT>BCS Admin: Width of the Navigation Tree</PARTXT></item>";
        String attribute = "PARAMETER1"; // PARAMETER
        attributes.add(AttributeBuilder.build(attribute, parameter));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().get(0), parameter);
    }

    @Test(dependsOnMethods = {"testSetXmlParameter"})
    public void testDeleteParameter() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        String attribute = "PARAMETER1";
        attributes.add(AttributeBuilder.build(attribute));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().size(), 0);
    }


    @Test(dependsOnMethods = {"testCreateFull"})
    public void testSetXmlAddtel() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        String addtel = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><item><COUNTRY>DE</COUNTRY><COUNTRYISO>DE</COUNTRYISO><STD_NO>X</STD_NO><TELEPHONE>089 1234</TELEPHONE><EXTENSION>6789</EXTENSION><TEL_NO>+498912346789</TEL_NO><CALLER_NO>08912346789</CALLER_NO><STD_RECIP/><R_3_USER>1</R_3_USER><HOME_FLAG>X</HOME_FLAG><CONSNUMBER>001</CONSNUMBER><ERRORFLAG/><FLG_NOUSE/><VALID_FROM/><VALID_TO/></item>";
        String attribute = "ADDTEL";
        attributes.add(AttributeBuilder.build(attribute, addtel));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().get(0), addtel);
    }

    @Test(dependsOnMethods = {"testSetXmlAddtel"})
    public void testDeleteAddtel() throws RemoteException {

        Set<Attribute> attributes = new HashSet<Attribute>();
        attributes.add(AttributeBuilder.build(Name.NAME, USER_NAME));

        String attribute = "ADDTEL";
        attributes.add(AttributeBuilder.build(attribute));

        // update it
        OperationOptions operationOptions = null;
        sapConnector.update(ACCOUNT_OBJECT_CLASS, new Uid(USER_NAME), attributes, operationOptions);

        // read it
        SapFilter query = new SapFilter();
        query.setByName(USER_NAME);
        final ConnectorObject[] found = {null};
        ResultsHandler handler = new ResultsHandler() {
            @Override
            public boolean handle(ConnectorObject connectorObject) {
                found[0] = connectorObject;
                return true; // continue
            }
        };
        OperationOptions options = null;
        sapConnector.executeQuery(ACCOUNT_OBJECT_CLASS, query, handler, options);

        // check attribute values
        ConnectorObject user = found[0];
        LOG.info(attribute+": {0}", user.getAttributeByName(attribute).getValue());
        Assert.assertEquals(user.getAttributeByName(attribute).getValue().size(), 0);
    }

//    @Test
//    public void testResetFailedLoginCount() throws RemoteException, JCoException {
//
//        sapConnector.resetFailedLoginCount("XXX");
//
//    }
}

