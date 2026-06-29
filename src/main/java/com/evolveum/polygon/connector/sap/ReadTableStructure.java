/*
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

import com.sap.conn.jco.JCoTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Field layout of a SAP table as returned by RFC_READ_TABLE / BBP_RFC_READ_TABLE in their
 * {@code FIELDS} table parameter (FIELDNAME, OFFSET, LENGTH, TYPE). Used by the RFC_READ_TABLE
 * code path to slice the fixed-width {@code DATA} work area and to keep the column order for the
 * delimiter-separated {@code ET_DATA} output.
 */
class ReadTableStructure {

    static class Field {
        private final String name;
        private final int offset;
        private final int length;
        private final String type;

        Field(String name, int offset, int length, String type) {
            this.name = name;
            this.offset = offset;
            this.length = length;
            this.type = type;
        }

        String getName() {
            return name;
        }

        int getOffset() {
            return offset;
        }

        int getLength() {
            return length;
        }

        String getType() {
            return type;
        }
    }

    private final List<Field> fields = new ArrayList<>();
    private final Map<String, Field> byName = new LinkedHashMap<>();

    ReadTableStructure(JCoTable fieldsTable) {
        if (fieldsTable.getNumRows() > 0) {
            fieldsTable.firstRow();
            do {
                Field field = new Field(
                        fieldsTable.getString("FIELDNAME"),
                        fieldsTable.getInt("OFFSET"),
                        fieldsTable.getInt("LENGTH"),
                        fieldsTable.getString("TYPE"));
                fields.add(field);
                byName.put(field.getName(), field);
            } while (fieldsTable.nextRow());
        }
    }

    List<Field> getFields() {
        return fields;
    }

    Field getField(String name) {
        return byName.get(name);
    }
}
