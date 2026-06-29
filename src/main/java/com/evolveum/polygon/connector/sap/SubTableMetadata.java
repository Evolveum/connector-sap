package com.evolveum.polygon.connector.sap;

import org.identityconnectors.framework.common.exceptions.ConfigurationException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SubTableMetadata {
    private String rootAlias;
    private String tableName;
    private String virtualColumnName;
    private Format format = SubTableMetadata.Format.XML;
    private final List<TableColumnDefinition> columns = new ArrayList<>();
    private String where;
    private Pattern rootReferencePattern;

    private static final Pattern PATTERN_FOR = Pattern.compile(" for ([^ ]+)");
    private static final Pattern PATTERN_FORMAT = Pattern.compile(" format ([^ ]+)");
    private static final Pattern PATTERN_AS = Pattern.compile(" as ([^ ]+)");
    private static final Pattern PATTERN_NAME = Pattern.compile("^([^ ]+)");
    /** the optional trailing " WHERE <clause>" (RFC_READ_TABLE mode only) */
    private static final Pattern PATTERN_WHERE = Pattern.compile("(?i)\\s+WHERE\\s+");

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

    /** The object class (alias) of the root objects this sub-table is attached to (the {@code for <alias>} part). */
    public String getRootAlias() {
        return rootAlias;
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

    /** Optional extra WHERE clause for the RFC_READ_TABLE sub-query (null in legacy mode). */
    public String getWhere() {
        return where;
    }

    /**
     * Root field names referenced in the WHERE as {@code <rootAlias>.<field>}. These have to be read on
     * the root row so {@link #resolveWhere} can substitute their values.
     */
    public Set<String> getRootFieldReferences() {
        Set<String> fields = new LinkedHashSet<>();
        if (where != null) {
            Matcher matcher = rootReferencePattern().matcher(where);
            while (matcher.find()) {
                fields.add(matcher.group(1));
            }
        }
        return fields;
    }

    /**
     * The WHERE with each {@code <rootAlias>.<field>} reference replaced by the (quoted, escaped) value of
     * that field in the current root row - so the sub-query can join on fields named differently in the
     * two tables (e.g. {@code WHERE BEGDA = ROLES.VALIDFROM}). Returns null when there is no WHERE.
     */
    public String resolveWhere(Map<String, String> rootValues) {
        if (where == null) {
            return null;
        }
        Matcher matcher = rootReferencePattern().matcher(where);
        StringBuffer resolved = new StringBuffer();
        while (matcher.find()) {
            String value = rootValues.getOrDefault(matcher.group(1), "");
            matcher.appendReplacement(resolved, Matcher.quoteReplacement("'" + value.replace("'", "''") + "'"));
        }
        matcher.appendTail(resolved);
        return resolved.toString();
    }

    private Pattern rootReferencePattern() {
        if (rootReferencePattern == null) {
            // <rootAlias>.<field>, not preceded by another identifier char (so it is a standalone token)
            rootReferencePattern = Pattern.compile(
                    "(?<![A-Za-z0-9_/])" + Pattern.quote(rootAlias) + "\\.([A-Za-z0-9_/]+)");
        }
        return rootReferencePattern;
    }

    private int getTableWidth() {
        int width = 0;
        for (TableColumnDefinition c : columns) {
            width += c.getLength();
        }
        return width;
    }

    public static SubTableMetadata parseConfig(String config, boolean readTableMode) {
        SubTableMetadata metadata = new SubTableMetadata();
        String def = config.trim();

        // RFC_READ_TABLE: split off an optional trailing WHERE first, so a '=' inside the clause is safe
        if (readTableMode) {
            Matcher whereMatcher = PATTERN_WHERE.matcher(def);
            if (whereMatcher.find()) {
                metadata.where = def.substring(whereMatcher.end()).trim();
                def = def.substring(0, whereMatcher.start()).trim();
            }
        }

        String[] definitionParts = def.split("=", 2);
        if (definitionParts.length != 2) {
            throw new ConfigurationException(
                    "Please use correct sub-table definition, for example: 'AGR_TEXTS for ACTIVITYGROUP format TSV as ShortDescription=MANDT:3:IGNORE,AGR_NAME:30:MATCH,SPRAS:1(\"E\"):IGNORE,LINE:5(\"00000\"):IGNORE,TEXT:80', got: " +
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
            // RFC_READ_TABLE ignores the fixed-width :<size>; the legacy path needs it for the offsets
            metadata.columns.add(readTableMode
                    ? TableColumnDefinition.parseLenientConfig(columnDefinition)
                    : TableColumnDefinition.parseConfig(metadata.getTableWidth(), columnDefinition));
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
            rootAlias = rootNameMatcher.group(1);
        } else {
            throw new ConfigurationException(
                    "Please specify the root object class (the 'tables' alias) this sub-table depends on " +
                    "(example: 'AGR_TEXTS for ACTIVITYGROUP=...'), got: " + definitionPart);
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
