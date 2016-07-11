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

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.*;

import java.util.List;

/**
 * Created by gpalos on 21. 1. 2016.
 */
public class SapAccountFilterTranslator extends AbstractFilterTranslator<SapFilter> {
    private static final Log LOG = Log.getLog(SapAccountFilterTranslator.class);

    String[] supportedAttributes = {"USERNAME",
            "LOGONDATA.GLTGV", "LOGONDATA.GLTGB", "LOGONDATA.USTYP", "LOGONDATA.CLASS", "LOGONDATA.ACCNT", "LOGONDATA.TZONE", "LOGONDATA.CODVN", "LOGONDATA.UFLAG",
            "DEFAULTS.SPLD", "DEFAULTS.SPLG", "DEFAULTS.SPDB", "DEFAULTS.SPDA", "DEFAULTS.DATFM", "DEFAULTS.DCPFM", "DEFAULTS.LANGU", "DEFAULTS.KOSTL", "DEFAULTS.START_MENU", "DEFAULTS.TIMEFM",
            "REF_USER.REF_USER",
            "ALIAS.USERALIAS",
            "PROFILES.BAPIPROF",
            "LOCPROFILES.SUBSYSTEM", "LOCPROFILES.PROFILE",
            "ACTIVITYGROUPS.AGR_NAME", "ACTIVITYGROUPS.FROM_DAT", "ACTIVITYGROUPS.TO_DAT",
            "LOCACTGROUPS.SUBSYSTEM", "LOCACTGROUPS.AGR_NAME", "LOCACTGROUPS.FROM_DAT", "LOCACTGROUPS.TO_DAT",
            "ADDRESS.FIRSTNAME", "ADDRESS.LASTNAME", "ADDRESS.DEPARTMENT", "ADDRESS.INHOUSE_ML", "ADDRESS.FUNCTION", "ADDRESS.BUILDING_P", "ADDRESS.BUILDING", "ADDRESS.ROOM_NO_P",
            "ADDRESS.TEL1_EXT", "ADDRESS.TEL1_NUMBR", "ADDRESS.FAX_EXTENS", "ADDRESS.FAX_NUMBER", "ADDRESS.E_MAIL",
            "COMPANY.COMPANY",
            "LASTMODIFIED.MODDATE", "LASTMODIFIED.MODTIME",
            "ISLOCKED.LOCAL_LOCK", "ISLOCKED.GLOB_LOCK", "ISLOCKED.WRNG_LOGON", "ISLOCKED.NO_USER_PW",
            "SYSTEM.SUBSYSTEM"};

    @Override
    protected SapFilter createEqualsExpression(EqualsFilter filter, boolean not) {
        LOG.ok("createEqualsExpression, filter: {0}, not: {1}", filter, not);

        Attribute attr = filter.getAttribute();
        LOG.ok("attr.getId:  {0}, attr.getValue: {1}", attr.getName(), attr.getValue());
        // filter by NAME is the same as by UID
        if (!not) {
            if (Name.NAME.equals(attr.getName()) || Uid.NAME.equals(attr.getName())) {
                if (attr.getValue() != null && attr.getValue().get(0) != null) {
                    SapFilter lf = new SapFilter(SapFilter.OPERATOR_EQUAL, SapConnector.USERNAME, String.valueOf(attr.getValue().get(0)));
                    return lf;
                }
            }
        }

        if (notSupportedAttribute(attr.getName())) {
            return null;            // not supported attribute
        }

        if (notSupportedValue(attr.getValue())) {
            return null;            // not supported value/s
        }

        String operator = not ? SapFilter.OPERATOR_NOT_EQUAL : SapFilter.OPERATOR_EQUAL;
        return new SapFilter(operator, attr.getName(), (String) attr.getValue().get(0));
    }

    @Override
    protected SapFilter createContainsExpression(ContainsFilter filter, boolean not) {
        LOG.ok("createContainsExpression, filter: {0}, not: {1}", filter, not);

        Attribute attr = filter.getAttribute();
        LOG.ok("attr.getId:  {0}, attr.getValue: {1}", attr.getName(), attr.getValue());
        if (!not) {
            if (Name.NAME.equals(attr.getName())) {
                if (attr.getValue() != null && attr.getValue().get(0) != null) {
                    String like = SapFilter.ANY_NUMBER_OF_CHARACTERS + (String) attr.getValue().get(0) + SapFilter.ANY_NUMBER_OF_CHARACTERS;
                    SapFilter lf = new SapFilter(SapFilter.OPERATOR_CONTAINS_PATTERN, SapConnector.USERNAME, like);
                    return lf;
                }
            }
        }

        if (notSupportedAttribute(attr.getName())) {
            return null;            // not supported attribute
        }

        if (notSupportedValue(attr.getValue())) {
            return null;            // not supported value/s
        }

        String operator = not ? SapFilter.OPERATOR_NOT_CONTAINS_PATTERN : SapFilter.OPERATOR_CONTAINS_PATTERN;
        String like = SapFilter.ANY_NUMBER_OF_CHARACTERS + (String) attr.getValue().get(0) + SapFilter.ANY_NUMBER_OF_CHARACTERS;
        return new SapFilter(operator, attr.getName(), like);
    }

    @Override
    protected SapFilter createStartsWithExpression(StartsWithFilter filter, boolean not) {
        LOG.ok("createStartsWithExpression, filter: {0}, not: {1}", filter, not);

        Attribute attr = filter.getAttribute();
        LOG.ok("attr.getId:  {0}, attr.getValue: {1}", attr.getName(), attr.getValue());
        if (notSupportedAttribute(attr.getName())) {
            return null;            // not supported attribute
        }

        if (notSupportedValue(attr.getValue())) {
            return null;            // not supported value/s
        }

        String operator = not ? SapFilter.OPERATOR_NOT_CONTAINS_PATTERN : SapFilter.OPERATOR_CONTAINS_PATTERN;
        String like = (String) attr.getValue().get(0) + SapFilter.ANY_NUMBER_OF_CHARACTERS;
        return new SapFilter(operator, attr.getName(), like);
    }

    @Override
    protected SapFilter createEndsWithExpression(EndsWithFilter filter, boolean not) {
        LOG.ok("createEndsWithExpression, filter: {0}, not: {1}", filter, not);

        Attribute attr = filter.getAttribute();
        LOG.ok("attr.getId:  {0}, attr.getValue: {1}", attr.getName(), attr.getValue());
        if (notSupportedAttribute(attr.getName())) {
            return null;            // not supported attribute
        }

        if (notSupportedValue(attr.getValue())) {
            return null;            // not supported value/s
        }

        String operator = not ? SapFilter.OPERATOR_NOT_CONTAINS_PATTERN : SapFilter.OPERATOR_CONTAINS_PATTERN;
        String like = SapFilter.ANY_NUMBER_OF_CHARACTERS + (String) attr.getValue().get(0);
        return new SapFilter(operator, attr.getName(), like);
    }

    @Override
    protected SapFilter createGreaterThanExpression(GreaterThanFilter filter, boolean not) {
        LOG.ok("createGreaterThanExpression, filter: {0}, not: {1}", filter, not);
        if (not) {
            LOG.ok("not supported native SAP NOT GreaterThanExpression filter, filtering is performed over connector framework (slower)");
            return null;            // not supported
        }

        Attribute attr = filter.getAttribute();
        LOG.ok("attr.getId:  {0}, attr.getValue: {1}", attr.getName(), attr.getValue());
        if (notSupportedAttribute(attr.getName())) {
            return null;            // not supported attribute
        }

        if (notSupportedValue(attr.getValue())) {
            return null;            // not supported value/s
        }

        String value = (String) attr.getValue().get(0);
        return new SapFilter(SapFilter.OPERATOR_GREATER_THAN, attr.getName(), value);
    }

    @Override
    protected SapFilter createGreaterThanOrEqualExpression(GreaterThanOrEqualFilter filter, boolean not) {
        LOG.ok("createGreaterThanOrEqualExpression, filter: {0}, not: {1}", filter, not);
        if (not) {
            LOG.ok("not supported native SAP NOT GreaterThanOrEqualExpression filter, filtering is performed over connector framework (slower)");
            return null;            // not supported
        }

        Attribute attr = filter.getAttribute();
        LOG.ok("attr.getId:  {0}, attr.getValue: {1}", attr.getName(), attr.getValue());
        if (notSupportedAttribute(attr.getName())) {
            return null;            // not supported attribute
        }

        if (notSupportedValue(attr.getValue())) {
            return null;            // not supported value/s
        }

        String value = (String) attr.getValue().get(0);
        return new SapFilter(SapFilter.OPERATOR_GREATER_EQUAL, attr.getName(), value);
    }

    @Override
    protected SapFilter createLessThanExpression(LessThanFilter filter, boolean not) {
        LOG.ok("createLessThanExpression, filter: {0}, not: {1}", filter, not);
        if (not) {
            LOG.ok("not supported native SAP NOT LessThanExpression filter, filtering is performed over connector framework (slower)");
            return null;            // not supported
        }

        Attribute attr = filter.getAttribute();
        LOG.ok("attr.getId:  {0}, attr.getValue: {1}", attr.getName(), attr.getValue());
        if (notSupportedAttribute(attr.getName())) {
            return null;            // not supported attribute
        }

        if (notSupportedValue(attr.getValue())) {
            return null;            // not supported value/s
        }

        String value = (String) attr.getValue().get(0);
        return new SapFilter(SapFilter.OPERATOR_LESS_THAN, attr.getName(), value);
    }

    @Override
    protected SapFilter createLessThanOrEqualExpression(LessThanOrEqualFilter filter, boolean not) {
        LOG.ok("createLessThanOrEqualExpression, filter: {0}, not: {1}", filter, not);
        if (not) {
            LOG.ok("not supported native SAP NOT LessThanOrEqualExpression filter, filtering is performed over connector framework (slower)");
            return null;            // not supported
        }

        Attribute attr = filter.getAttribute();
        LOG.ok("attr.getId:  {0}, attr.getValue: {1}", attr.getName(), attr.getValue());
        if (notSupportedAttribute(attr.getName())) {
            return null;            // not supported attribute
        }

        if (notSupportedValue(attr.getValue())) {
            return null;            // not supported value/s
        }

        String value = (String) attr.getValue().get(0);
        return new SapFilter(SapFilter.OPERATOR_LESS_EQUAL, attr.getName(), value);
    }

    @Override
    protected SapFilter createAndExpression(SapFilter leftExpression, SapFilter rightExpression) {
        LOG.ok("createAndExpression, leftExpression: {0}, rightExpression: {1}", leftExpression, rightExpression);

        // SAP support only one logical operation, otherwise error occured:
        // "Logical operation and arity are only supported in the first line"
        // If we have combination of AND and OR, ignore it

        if (leftExpression.getLogicalOperation() != null && SapFilter.LOGICAL_OR.equals(leftExpression.getLogicalOperation())) {
            LOG.ok("combining OR and AND operation is not supported in SAP, filtering is performed over connector framework (slower)");
            return null; // combining OR and AND operation is not supported in SAP
        } else if (rightExpression.getLogicalOperation() != null && SapFilter.LOGICAL_OR.equals(rightExpression.getLogicalOperation())) {
            LOG.ok("combining OR and AND operation is not supported in SAP, filtering is performed over connector framework (slower)");
            return null; // combining OR and AND operation is not supported in SAP
        } else if (leftExpression.getLogicalOperation() != null && SapFilter.LOGICAL_AND.equals(leftExpression.getLogicalOperation())) {
            return leftExpression.handleNextExpression(rightExpression);
        } else if (rightExpression.getLogicalOperation() != null && SapFilter.LOGICAL_AND.equals(rightExpression.getLogicalOperation())) {
            return rightExpression.handleNextExpression(leftExpression);
        }
        // this is a first logical operation, create it
        SapFilter filter = new SapFilter(SapFilter.LOGICAL_AND, leftExpression);
        return filter.handleNextExpression(rightExpression);
    }

    @Override
    protected SapFilter createOrExpression(SapFilter leftExpression, SapFilter rightExpression) {
        LOG.ok("createOrExpression, leftExpression: {0}, rightExpression: {1}", leftExpression, rightExpression);

        // SAP support only one logical operation, otherwise error occured:
        // "Logical operation and arity are only supported in the first line"
        // If we have combination of AND and OR, ignore it

        if (leftExpression.getLogicalOperation() != null && SapFilter.LOGICAL_AND.equals(leftExpression.getLogicalOperation())) {
            LOG.ok("combining OR and AND operation is not supported in SAP, filtering is performed over connector framework (slower)");
            return null; // combining OR and AND operation is not supported in SAP
        } else if (rightExpression.getLogicalOperation() != null && SapFilter.LOGICAL_AND.equals(rightExpression.getLogicalOperation())) {
            LOG.ok("combining OR and AND operation is not supported in SAP, filtering is performed over connector framework (slower)");
            return null; // combining OR and AND operation is not supported in SAP
        } else if (leftExpression.getLogicalOperation() != null && SapFilter.LOGICAL_OR.equals(leftExpression.getLogicalOperation())) {
            return leftExpression.handleNextExpression(rightExpression);
        } else if (rightExpression.getLogicalOperation() != null && SapFilter.LOGICAL_OR.equals(rightExpression.getLogicalOperation())) {
            return rightExpression.handleNextExpression(leftExpression);
        }
        // this is a first logical operation, create it
        SapFilter filter = new SapFilter(SapFilter.LOGICAL_OR, leftExpression);
        return filter.handleNextExpression(rightExpression);
    }

    private boolean notSupportedAttribute(String attrName) {
        boolean supported = false;
        for (String supportedAttribute : supportedAttributes) {
            if (supportedAttribute.equals(attrName)) {
                supported = true;
            }
        }
        if (!supported) {
            LOG.ok("not supported native SAP attribute filter: " + attrName + ", filtering is performed over connector framework (slower)");
        }

        return !supported;
    }

    private boolean notSupportedValue(List<Object> values) {
        if (values == null || values.size() != 1 || values.get(0) == null) {
            LOG.ok("not supported native SAP attribute value(s) filter: " + values + ", filtering is performed over connector framework (slower)");
            return true;
        }
        return false;
    }


}
