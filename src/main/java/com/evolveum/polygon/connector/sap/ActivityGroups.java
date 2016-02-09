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

import java.text.ParseException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by gpalos on 21. 1. 2016.
 */
public class ActivityGroups {
    private static final Log LOG = Log.getLog(ActivityGroups.class);

    List<ActivityGroup> values = new LinkedList<ActivityGroup>();

    // if ActivityGroups not null, modify
    boolean modify = false;

    public ActivityGroups(JCoTable agt) {
        agt.firstRow();
        if (agt.getNumRows() > 0) {
            do {
                String name = (String) agt.getValue("AGR_NAME");
                Date from = (Date) agt.getValue("FROM_DAT");
                Date to = (Date) agt.getValue("TO_DAT");

                ActivityGroup ag = new ActivityGroup(name, from, to);

                values.add(ag);
            } while (agt.nextRow());
        }
    }

    public ActivityGroups(Set<Attribute> attributes, String attrName) throws ParseException {
        List<Object> activityGroups = null;
        for (Attribute attr : attributes) {
            if (attrName.equals(attr.getName())) {
                modify = true;
                activityGroups = attr.getValue();
                if (activityGroups != null) {
                    for (int i = 0; i < activityGroups.size(); i++) {
                        String activityGroup = (String) activityGroups.get(i);
                        if (activityGroup == null) {
                            throw new InvalidAttributeValueException("Value " + null + " must be not null for attribute " + attrName);
                        }

                        ActivityGroup ag = new ActivityGroup(activityGroup);
                        this.values.add(ag);
                    }
                }
            }
        }

        LOG.ok("activityGroups in input: {0}, in output: {1}, modify: {2} ", activityGroups, values, modify);

    }


    public List<String> getValues(boolean withDates) {
        List<String> ret = new LinkedList<String>();
        for (int i = 0; i < values.size(); i++) {
            ret.add(values.get(i).getValue(withDates));
        }
        return ret;
    }

    @Override
    public String toString() {
        return "ActivityGroups{" +
                "modify=" + modify +
                ", values=" + values +
                '}';
    }


    public int size() {
        return this.values.size();
    }
}
