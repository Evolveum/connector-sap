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

import com.sap.conn.jco.JCoTable;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by gpalos on 2. 3. 2016.
 *
 * Table represents a JCOTable from SAP.
 * ConnId framework don't support complex data types, we transfer each JCOTable lines to XML representation and send it as String value over ConnId.
 * Each line (Item) is a value in multi value attribute and contains table header names from SAP with same names as represents in SAP and his values:
 * <?xml version="1.0" encoding="UTF-8"?>
 *     <item>
 *          <column1name>column 1 value</column1name>
 *          <column2name>column 2 value</column2name>
 *     </item>
 *
 * Example representation of one line of ACTIVITYGROUPS:
 * <?xml version="1.0" encoding="UTF-8"?>
 *     <item>
 *         <AGR_NAME>name of activity group</AGR_NAME>
 *         <FROM_DAT>2016-06-07</FROM_DAT>
 *         <TO_DAT>9999-12-31</TO_DAT>
 *         <AGR_TEXT>description of activity group</AGR_TEXT>
 *         <ORG_FLAG></ORG_FLAG>
 *     </item>
 *
 * @see Item
 */
public class Table {
    private static final Log LOG = Log.getLog(Table.class);

    /**
     * lines in table represented as Item
     */
    private List<Item> values = new LinkedList<Item>();

    /**
     * need to update
     */
    private boolean update = false;

    public Table(JCoTable agt) throws TransformerException, ParserConfigurationException {
        agt.firstRow();
        if (agt.getNumRows() > 0) {
            do {
                Item item = new Item(agt);

                values.add(item);
            } while (agt.nextRow());
        }
    }

    public Table(Set<Attribute> attributes, String attrName) throws IOException, SAXException, ParserConfigurationException {
        List<Object> items = null;
        for (Attribute attr : attributes) {
            if (attr.getName().startsWith(attrName)) {
                update = true;
                items = attr.getValue();
                if (items != null) {
                    for (int i = 0; i < items.size(); i++) {
                        String item = (String) items.get(i);
                        if (item == null) {
                            throw new InvalidAttributeValueException("Value " + null + " must be not null for attribute " + attrName);
                        }
                        boolean isXml = item.startsWith("<?xml");
                        this.values.add(new Item(item, isXml, attrName));
                    }
                }
            }
        }

        LOG.ok("items in input: {0}, in output: {1}, update: {2} for attribute {3}", items, values, update, attrName);

    }


    public List<String> getXmls() {
        List<String> ret = new LinkedList<String>();
        for (int i = 0; i < values.size(); i++) {
            ret.add(values.get(i).getData());
        }
        return ret;
    }

    public List<String> getIds(String attribute) {
        List<String> ret = new LinkedList<String>();
        for (int i = 0; i < values.size(); i++) {
            ret.add(values.get(i).getByAttribute(attribute));
        }
        return ret;
    }

    @Override
    public String toString() {
        return "Table{" +
                "update=" + update +
                ", values=" + values +
                '}';
    }


    public int size() {
        return this.values.size();
    }

    public List<Item> getValues() {
        return values;
    }

    public boolean isUpdate() {
        return update;
    }
}
