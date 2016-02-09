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

import org.identityconnectors.common.StringUtil;

import java.text.ParseException;
import java.util.Date;

/**
 * Created by gpalos on 21. 1. 2016.
 */
public class ActivityGroup {


    private String value;

    private String name;

    private Date from;

    private Date to;

    private static final String DELIMITER = "|";

    public ActivityGroup(String name, Date from, Date to) {
        this.name = name;
        this.from = from;
        this.to = to;

        generateValue();
    }

    public ActivityGroup(String value) throws ParseException {
        this.value = value;
        String[] ags = value.split("\\" + DELIMITER);
        name = ags[0];
        if (ags.length > 1 && !StringUtil.isEmpty(ags[1])) {
            from = SapConnector.SAP_DF.parse(ags[1]);
        }
        if (ags.length > 2 && !StringUtil.isEmpty(ags[2])) {
            to = SapConnector.SAP_DF.parse(ags[2]);
        }
    }

    private void generateValue() {
        this.value = name + DELIMITER + (from == null ? "" : SapConnector.SAP_DF.format(from)) + DELIMITER + (to == null ? "" : SapConnector.SAP_DF.format(to));
    }

    public String getValue(boolean withDates) {
        if (withDates) {
            return value;
        } else
            return name;
    }

    public String getName() {
        return name;
    }

    public String getFrom() {
        if (from == null) {
            return null;
        }
        return SapConnector.SAP_DF.format(from);
    }

    public String getTo() {
        if (to == null) {
            return null;
        }
        return SapConnector.SAP_DF.format(to);
    }

    @Override
    public String toString() {
        return "ActivityGroup{" +
                "value='" + value + '\'' +
                ", name='" + name + '\'' +
                ", from=" + from +
                ", to=" + to +
                '}';
    }
}
