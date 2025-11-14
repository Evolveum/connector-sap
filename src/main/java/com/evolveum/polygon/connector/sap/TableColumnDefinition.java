package com.evolveum.polygon.connector.sap;

import org.identityconnectors.framework.common.exceptions.ConfigurationException;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TableColumnDefinition {
    private String columnName;
    private int offset;
    private int length;
    private Mode mode = Mode.OUTPUT;
    private String filterConstant;

    public enum Mode {
        /**
         * Don't include this column in the result.
         */
        IGNORE,

        /**
         * Don't include this column in the result.
         * The value of this column has to be equal to the same column in the root table.
         */
        MATCH,

        /**
         * Include this column in the result. This is the default setting if nothing is specified.
         */
        OUTPUT
    }

    public String getColumnName() {
        return columnName;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }

    public Mode getMode() {
        return mode;
    }

    public String getFilterConstant() {
        return filterConstant;
    }

    private static final Pattern PATTERN_FILTER_CONST = Pattern.compile("^(\\d+)\\(\"(.*)\"\\)$");

    private static final String FORMAT = "<columnName>:<size>[(\"<filterValue>\")][:<syncMode>]";

    public static TableColumnDefinition parseConfig(int currentOffset, String config) {
        String[] allParts = config.split(":");

        TableColumnDefinition res = new TableColumnDefinition();
        res.offset = currentOffset;

        if (allParts.length < 2 || allParts.length > 3) {
            throw new ConfigurationException("Please specify a column name in the requires format " + FORMAT +
                                             " (example: 'MANDT:3:IGNORE'), got: " + config);
        }

        res.columnName = allParts[0];

        Matcher matcher = PATTERN_FILTER_CONST.matcher(allParts[1]);
        if (matcher.find()) {
            try {
                res.length = Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                throw new ConfigurationException("Please specify a column width as integer, got: '" + matcher.group(1) +
                                                 "' in column definition: '" + config + "'");
            }
            res.filterConstant = matcher.group(2);
        } else {
            try {
                res.length = Integer.parseInt(allParts[1]);
            } catch (NumberFormatException e) {
                throw new ConfigurationException(
                        "Please specify a column width as integer, got: '" + allParts[1] + "' in column definition: '" +
                        config + "'");
            }
        }

        if (allParts.length > 2) {
            try {
                res.mode = Mode.valueOf(allParts[2]);
            } catch (IllegalArgumentException e) {
                throw new ConfigurationException(
                        "Please use one of these column modes: " + Arrays.toString(SubTableMetadata.Format.values()) +
                        ", got: " + allParts[2], e);
            }
        }

        return res;
    }
}
