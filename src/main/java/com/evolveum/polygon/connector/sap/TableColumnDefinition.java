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

    /** A ("value") filter constant, wherever it appears in a lenient (RFC_READ_TABLE) column definition. */
    private static final Pattern PATTERN_FILTER_VALUE = Pattern.compile("\\(\"([^\"]*)\"\\)");

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
                        "Please use one of these column modes: " + Arrays.toString(Mode.values()) +
                        ", got: " + allParts[2], e);
            }
        }

        return res;
    }

    /**
     * Lenient parser for the RFC_READ_TABLE path. Data is read by field name, not by offset, so the
     * fixed-width {@code :<size>} is optional and ignored - a legacy {@code MANDT:3:IGNORE} and a
     * simplified {@code MANDT:IGNORE} parse the same. A {@code ("value")} filter and a
     * {@code :MATCH}/{@code :IGNORE}/{@code :OUTPUT} mode may appear in any position; any remaining
     * purely numeric token is treated as an (ignored) size. {@code offset}/{@code length} stay unused.
     */
    public static TableColumnDefinition parseLenientConfig(String config) {
        TableColumnDefinition res = new TableColumnDefinition();
        String work = config.trim();

        Matcher filterMatcher = PATTERN_FILTER_VALUE.matcher(work);
        if (filterMatcher.find()) {
            res.filterConstant = filterMatcher.group(1);
            work = (work.substring(0, filterMatcher.start()) + work.substring(filterMatcher.end())).trim();
        }

        String[] parts = work.split(":");
        res.columnName = parts[0].trim();
        if (res.columnName.isEmpty()) {
            throw new ConfigurationException("Please specify a sub-table column name, got: '" + config + "'");
        }
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.isEmpty() || part.matches("\\d+")) {
                continue; // an ignored fixed-width size
            }
            try {
                res.mode = Mode.valueOf(part);
            } catch (IllegalArgumentException e) {
                throw new ConfigurationException("Unknown sub-table column option '" + part + "' in '" + config
                        + "'; expected a size, a (\"filter\") or one of " + Arrays.toString(Mode.values()), e);
            }
        }

        return res;
    }
}
