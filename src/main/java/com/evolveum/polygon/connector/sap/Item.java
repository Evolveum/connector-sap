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

import com.sap.conn.jco.JCoField;
import com.sap.conn.jco.JCoTable;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by gpalos on 2. 3. 2016.
 *
 * Item represents one line from SAP JCOTable containing his all JCoField (column name and his value).
 *
 * @see Table
 */
public class Item {

    /**
     * all column names and his value for all JCoField in one JCOTable line
     */
    private Map<String, String> values = new LinkedHashMap<String, String>();

    /**
     * raw XML data
     */
    private String data;
    /**
     * raw datas are XML data with all his columns, or only simlified data containing his keys without XML wrapper elements
     */
    private boolean isXml;

    private static final String ITEM_NAME = "item";

    private static final DocumentBuilder loader;

    static {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            loader = factory.newDocumentBuilder();

        } catch (ParserConfigurationException ex) {
            throw new IllegalStateException("Error creating XML document " + ex.getMessage());
        }
    }


    public Item(String data, boolean isXml, String keyAttributeName) throws IOException, SAXException {
        this.data = data;
        this.isXml = isXml;

        if (this.isXml) {
            // XML data
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(data));
            Document doc = loader.parse(is);
            doc.getDocumentElement().normalize();

            NodeList root = doc.getElementsByTagName(ITEM_NAME);
            if (root.getLength() != 1)
                throw new ConfigurationException("needed " + ITEM_NAME + ", but not found in: " + data);

            NodeList nodeList = root.item(0).getChildNodes();
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                if (node.getNodeType() == Node.ELEMENT_NODE) {
                    values.put(node.getNodeName(), node.getTextContent());
                }
            }
        } else {
            // only key
            values.put(SapConnector.TABLETYPE_PARAMETER_KEYS.get(keyAttributeName), data);
        }
    }

    public Item(JCoTable agt) throws TransformerException {
        Document doc = loader.newDocument();
        Element mainRootElement = doc.createElementNS("", ITEM_NAME);
        doc.appendChild(mainRootElement);

        Iterator<JCoField> iter = agt.iterator();
        while (iter.hasNext()) {
            JCoField field = iter.next();
            Element node = doc.createElement(field.getName());
            node.appendChild(doc.createTextNode(field.getString()));
            mainRootElement.appendChild(node);
            values.put(field.getName(), field.getString());
        }
        Transformer transformer = TransformerFactory.newInstance().newTransformer(); // Transformer is not thread-safe, so it must be called here, not in the static
        // transformer.setOutputProperty(OutputKeys.INDENT, "yes");

        DOMSource source = new DOMSource(doc);
        StreamResult result = new StreamResult(new StringWriter());
        transformer.transform(source, result);

        this.data = result.getWriter().toString();
    }

    public Map<String, String> getValues() {
        return values;
    }

    public String getData() {
        return data;
    }

    public boolean isXml() {
        return isXml;
    }

    @Override
    public String toString() {
        return "Item{" +
                "values='" + values + '\'' +
                ", data=" + data +
                '}';
    }

    public String getByAttribute(String attribute) {
        return values.get(attribute);
    }


}
