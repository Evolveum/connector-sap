package com.evolveum.polygon.connector.sap;

import org.identityconnectors.framework.common.exceptions.ConfigurationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SubTableMetadata {
    private String rootTableName;
    private String tableName;
    private String virtualColumnName;
    private Format format = SubTableMetadata.Format.XML;
    private final List<TableColumnDefinition> columns = new ArrayList<>();

    private static final Pattern PATTERN_FOR = Pattern.compile(" for ([^ ]+)");
    private static final Pattern PATTERN_FORMAT = Pattern.compile(" format ([^ ]+)");
    private static final Pattern PATTERN_AS = Pattern.compile(" as ([^ ]+)");
    private static final Pattern PATTERN_NAME = Pattern.compile("^([^ ]+)");

    public enum Format {
        /**
         * XML format with each column as separate XML tag.
         */
        XML,

        /**
         * All columns are concatenated with TAB as separator.
         * TAB characters are usually not used by SAP, so the values don't need escaping.
         */
        TSV
    }

    public String getRootTableName() {
        return rootTableName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getVirtualColumnName() {
        return virtualColumnName;
    }

    public List<TableColumnDefinition> getColumns() {
        return columns;
    }

    public Format getFormat() {
        return format;
    }

    private int getTableWidth() {
        int width = 0;
        for (TableColumnDefinition c : columns) {
            width += c.getLength();
        }
        return width;
    }

    public static SubTableMetadata parseConfig(String config) {
        SubTableMetadata metadata = new SubTableMetadata();

        String[] definitionParts = config.split("=");
        if (definitionParts.length != 2) {
            throw new ConfigurationException(
                    "Please use correct sub-table definition, for example: 'AGR_TEXTS for AGR_DEFINE format TSV as ShortDescription=MANDT:3:IGNORE,AGR_NAME:30:MATCH,SPRAS:1(\"E\"):IGNORE,LINE:5(\"00000\"):IGNORE,TEXT:80', got: " +
                    config);
        }

        metadata.parseTableDefinition(definitionParts[0]);

        String[] allColumnsDef = definitionParts[1].split(",");
        if (allColumnsDef.length == 0) {
            throw new ConfigurationException(
                    "Please specify at least one column definition for the sub-table after the '=' character, for example: '...=AGR_NAME:30', got: " +
                    config);
        }

        for (String columnDefinition : allColumnsDef) {
            metadata.columns.add(TableColumnDefinition.parseConfig(metadata.getTableWidth(), columnDefinition));
        }

        return metadata;
    }

    private void parseTableDefinition(String definitionPart) {
        Matcher nameMatcher = PATTERN_NAME.matcher(definitionPart);
        if (nameMatcher.find()) {
            tableName = nameMatcher.group(1);
        } else {
            throw new ConfigurationException(
                    "Please specify a sub-table name before the '=' character (example: 'AGR_TEXTS for ARG_DEFINE=...'), got: " +
                    definitionPart);
        }

        Matcher rootNameMatcher = PATTERN_FOR.matcher(definitionPart);
        if (rootNameMatcher.find()) {
            rootTableName = rootNameMatcher.group(1);
        } else {
            throw new ConfigurationException(
                    "Please specify a root table name, on which this sub-table depends (example: 'AGR_TEXTS for ARG_DEFINE=...'), got: " +
                    definitionPart);
        }

        Matcher typeMatcher = PATTERN_FORMAT.matcher(definitionPart);
        if (typeMatcher.find()) {
            try {
                format = Format.valueOf(typeMatcher.group(1));
            } catch (IllegalArgumentException e) {
                throw new ConfigurationException(
                        "Please use one of these formats: " + Arrays.toString(Format.values()) + ", got: " +
                        typeMatcher.group(1), e);
            }
        }

        Matcher aliasMatcher = PATTERN_AS.matcher(definitionPart);
        if (aliasMatcher.find()) {
            virtualColumnName = aliasMatcher.group(1);
        } else {
            virtualColumnName = tableName;
        }
    }
}
