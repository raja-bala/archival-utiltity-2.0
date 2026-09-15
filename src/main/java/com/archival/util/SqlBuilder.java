package com.archival.util;

import com.archival.model.TableConfig;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds every SQL statement the framework needs, purely from
 * {@link TableConfig} metadata. This is the only place that assembles
 * table-specific SQL text; every other component works against the
 * resulting strings/parameters and never string-concatenates a table or
 * column name itself.
 */
public final class SqlBuilder {

    private SqlBuilder() {
    }

    public static String selectForExport(TableConfig config) {
        String columns = String.join(", ", config.columnNames());
        return "SELECT " + columns +
                " FROM " + config.getTableName() +
                " WHERE " + windowPredicate(config) +
                " ORDER BY " + config.getPrimaryKeyColumn() + " ASC";
    }

    public static String countInWindow(TableConfig config) {
        return "SELECT COUNT(*) FROM " + config.getTableName() +
                " WHERE " + windowPredicate(config);
    }

    /**
     * Deletes up to {@code batchSize} rows in the window via a derived-table
     * subquery, which works on both MariaDB/MySQL (where you cannot directly
     * select from the table you are deleting from) and H2/most other engines.
     * Callers loop this until zero rows are affected.
     */
    public static String deleteBatch(TableConfig config) {
        return "DELETE FROM " + config.getTableName() +
                " WHERE " + config.getPrimaryKeyColumn() + " IN (" +
                "  SELECT " + config.getPrimaryKeyColumn() + " FROM (" +
                "    SELECT " + config.getPrimaryKeyColumn() + " FROM " + config.getTableName() +
                "    WHERE " + windowPredicate(config) +
                "    ORDER BY " + config.getPrimaryKeyColumn() + " ASC" +
                "    LIMIT ?" +
                "  ) archival_delete_batch" +
                ")";
    }

    /**
     * Parameterized INSERT used by the restore job to re-insert previously
     * exported/deleted rows back into the source table, in the same column
     * order as {@link TableConfig#getColumns()} (and therefore the same
     * order {@code ParquetRecordConverter}/{@code RestoreTasklet} bind
     * values in).
     */
    public static String insertRow(TableConfig config) {
        String columns = String.join(", ", config.columnNames());
        String placeholders = config.getColumns().stream().map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + config.getTableName() + " (" + columns + ") VALUES (" + placeholders + ")";
    }

    private static String windowPredicate(TableConfig config) {
        StringBuilder sb = new StringBuilder();
        sb.append(config.getFyeColumn()).append(" >= ? AND ")
          .append(config.getFyeColumn()).append(" < ?");
        if (config.getAdditionalWhereClause() != null && !config.getAdditionalWhereClause().isBlank()) {
            sb.append(" AND (").append(config.getAdditionalWhereClause()).append(")");
        }
        return sb.toString();
    }

    public static List<String> quotedColumnList(TableConfig config) {
        return config.getColumns().stream().map(c -> c.getName()).collect(Collectors.toList());
    }
}
