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
import org.identityconnectors.framework.common.objects.filter.Filter;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by gpalos on 21. 1. 2016.
 *
 */
public class SapFilter {
    private static final Log LOG = Log.getLog(SapFilter.class);
    /* The operators provided by SAP are:
        https://www.consolut.com/en/s/sap-ides-access/d/s/doc/H-BAPIOPTION */
    public static final String OPERATOR_EQUAL = "EQ";
    public static final String OPERATOR_NOT_EQUAL = "NE";
//    public static final String OPERATOR_BETWEEN = "BT";
//    public static final String OPERATOR_NOT_BETWEEN = "NB";
    public static final String OPERATOR_LESS_THAN = "LT";
    public static final String OPERATOR_LESS_EQUAL = "LE";
    public static final String OPERATOR_GREATER_THAN = "GT";
    public static final String OPERATOR_GREATER_EQUAL = "GE";
    public static final String OPERATOR_CONTAINS_PATTERN = "CP";
    public static final String OPERATOR_NOT_CONTAINS_PATTERN = "NP";

//    public static final String ONE_CHARACTER = "+";
    public static final String ANY_NUMBER_OF_CHARACTERS = "*";

    public static final String LOGICAL_AND = "AND";
    public static final String LOGICAL_OR = "OR";


    // SAP basic filter parameter for all object classes expected account
    private String basicByNameEquals;

    // SAP filter parameters for account object class

    /**
     * PARAMETER, for example ADDRESS
     */
    private String parameter;
    /**
     * FIELD, for example FIRSTNAME
     */
    private String field;
    /**
     *  OPTION - operators, for examle EQ - Equal
     */
    private String option;
    /**
     * Permitted LOW Values
     */
    private String value;

    /**
     * logical operation, AND, OR,
     * if not null, leftExpression and rightExpression must filled
     */
    private String logicalOperation;

    /**
     * SAP support only one logical operation, otherwise error occured:
     * "Logical operation and arity are only supported in the first line"
     * If we have more operations, ignore it
     */
    private List<SapFilter> expressions;

    /**
     * arity - the number of subsequent rows
     */
    private int arity;

    private Filter inMemoryFilter;

    public SapFilter(String sapOperator, String attribute, String value) {
        this.setOption(sapOperator);

        String[] params = attribute.split("\\.");
        this.setParameter(params[0]);
        if (params.length==2) {
            this.setField(params[1]);
        }

        this.setValue(value);
    }

    public SapFilter() {

    }

    public SapFilter(String basicByNameEquals) {
        this.basicByNameEquals = basicByNameEquals;
    }

    public SapFilter(Filter inMemoryFilter) {
        this.inMemoryFilter = inMemoryFilter;
    }

    public SapFilter(String logicalOperation, SapFilter leftExpression) {
        this.logicalOperation = logicalOperation;
        this.expressions = new LinkedList<>();
        this.expressions.add(leftExpression);
        this.arity = 1;
    }


    /**
     * Handle next expression with SAP restriction.
     * If you use the AND operator, you cannot specify more than one identical field for the same parameter. The result list would then be empty.
     *
     * @param nextExpression
     * @return null (not supported filter) if AND logical operation is occured and nextExpression contains the same parameter and field what we already have in expression
     */
    public SapFilter handleNextExpression(SapFilter nextExpression){
        if (this.logicalOperation != null && LOGICAL_AND.equals(this.logicalOperation))
        {
            for (SapFilter expr : this.expressions) {
                if (expr.getParameter().equals(nextExpression.getParameter())) {
                    if (expr.getField() == null && nextExpression.getField() == null) {
                        LOG.ok("combining more than one identical field "+expr.getParameter()+" for the same parameter is not supported in SAP, filtering is performed over connector framework (slower)");
                        return null; // not supported
                    } else if (expr.getField().equals(nextExpression.getField())) {
                        LOG.ok("combining more than one identical field "+expr.getParameter()+"."+expr.getField()+" for the same parameter is not supported in SAP, filtering is performed over connector framework (slower)");
                        return null; // not supported
                    }
                }
            }
        }

        this.expressions.add(nextExpression);
        this.arity++;

        return this;
    }

    public String byNameEquals() {
        if (option != null && OPERATOR_EQUAL.equals(option)
                && parameter!=null && SapConnector.USERNAME.equals(parameter) && logicalOperation == null) {
            return value;
        }

        return null; // searching not byt name equals
    }

    public void setByUsernameEquals(String username) {
        this.option = OPERATOR_EQUAL;
        this.parameter = SapConnector.USERNAME;
        this.value = username;
    }

    public String byNameContains() {
        if (option != null && OPERATOR_CONTAINS_PATTERN.equals(option)
                && parameter!=null && SapConnector.USERNAME.equals(parameter) && logicalOperation == null) {
            return value;
        }

        return null; // searching not byt name equals
    }

    public String getBasicByNameEquals() {
        return basicByNameEquals;
    }

    public void setBasicByNameEquals(String basicByNameEquals) {
        this.basicByNameEquals = basicByNameEquals;
    }

    public String getParameter() {
        return parameter;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOption() {
        return option;
    }

    public void setOption(String option) {
        this.option = option;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getLogicalOperation() {
        return logicalOperation;
    }

    public void setLogicalOperation(String logicalOperation) {
        this.logicalOperation = logicalOperation;
    }

    public List<SapFilter> getExpressions() {
        return expressions;
    }

    public void setExpressions(List<SapFilter> expressions) {
        this.expressions = expressions;
    }

    public int getArity() {
        return arity;
    }

    public void setArity(int arity) {
        this.arity = arity;
    }

    public Filter getInMemoryFilter() {
        return inMemoryFilter;
    }

    @Override
    public String toString() {
        return "SapFilter{" +
                "byNameEquals='" + byNameEquals() + '\'' +
                ", byNameContains='" + byNameContains() + '\'' +
                ", parameter='" + parameter + '\'' +
                ", field='" + field + '\'' +
                ", option='" + option + '\'' +
                ", value='" + value + '\'' +
                ", logicalOperation='" + logicalOperation + '\'' +
                ", expressions=" + expressions +
                ", arity=" + arity +
                '}';
    }
}
