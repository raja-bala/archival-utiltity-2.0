package com.archival.introspection;

import com.archival.model.ColumnDefinition;
import com.archival.model.ParquetColumnType;
import com.archival.model.SqlColumnType;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Discovers a table's column list and types directly from the database via
 * JDBC metadata, instead of requiring a hand-written config file.
 * <p>
 * This is what lets the framework support any table - across any number of
 * applications - purely by naming it on the command line: every column in
 * the table is exported, using the type mapping in {@link #mapSqlType}.
 * The two things introspection cannot discover (which column is the
 * primary key, and any extra filter predicate) are supplied as job
 * parameters instead - see {@code ArchivalApplicationRunner}.
 */
@Component
public class SchemaIntrospector {

    private final DataSource dataSource;

    public SchemaIntrospector(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<ColumnDefinition> introspectColumns(String tableName) {
        List<ColumnDefinition> columns = new ArrayList<>();
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            // JDBC guarantees getColumns() results are ordered by ORDINAL_POSITION within a table,
            // so no extra sorting is needed here.
            try (ResultSet rs = metaData.getColumns(connection.getCatalog(), null, tableName, null)) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    int jdbcType = rs.getInt("DATA_TYPE");
                    int columnSize = rs.getInt("COLUMN_SIZE");
                    int decimalDigits = rs.getInt("DECIMAL_DIGITS");
                    int nullableCode = rs.getInt("NULLABLE");

                    SqlColumnType sqlType = mapSqlType(jdbcType, columnName);
                    ParquetColumnType parquetType = mapParquetType(sqlType);

                    ColumnDefinition column = new ColumnDefinition();
                    column.setName(columnName);
                    column.setSqlType(sqlType);
                    column.setParquetType(parquetType);
                    column.setNullable(nullableCode != DatabaseMetaData.columnNoNulls);
                    if (sqlType == SqlColumnType.DECIMAL) {
                        column.setPrecision(columnSize);
                        column.setScale(Math.max(decimalDigits, 0));
                    }
                    columns.add(column);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to introspect columns for table '" + tableName + "'", e);
        }

        if (columns.isEmpty()) {
            throw new IllegalStateException("Table '" + tableName
                    + "' was not found (or has no columns visible to this JDBC connection). "
                    + "Check --tableName and that the configured database user can see it.");
        }
        return columns;
    }

    private SqlColumnType mapSqlType(int jdbcType, String columnName) {
        return switch (jdbcType) {
            case Types.CHAR, Types.NCHAR -> SqlColumnType.CHAR;
            case Types.VARCHAR, Types.NVARCHAR, Types.LONGVARCHAR, Types.LONGNVARCHAR -> SqlColumnType.VARCHAR;
            case Types.CLOB, Types.NCLOB -> SqlColumnType.TEXT;
            case Types.TINYINT -> SqlColumnType.TINYINT;
            case Types.SMALLINT -> SqlColumnType.SMALLINT;
            case Types.INTEGER -> SqlColumnType.INTEGER;
            case Types.BIGINT -> SqlColumnType.BIGINT;
            case Types.DECIMAL, Types.NUMERIC -> SqlColumnType.DECIMAL;
            case Types.DOUBLE -> SqlColumnType.DOUBLE;
            case Types.FLOAT, Types.REAL -> SqlColumnType.FLOAT;
            case Types.DATE -> SqlColumnType.DATE;
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> SqlColumnType.TIMESTAMP;
            case Types.BOOLEAN, Types.BIT -> SqlColumnType.BOOLEAN;
            case Types.BLOB, Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY -> SqlColumnType.BLOB;
            default -> throw new IllegalStateException(
                    "Column '" + columnName + "' has JDBC type code " + jdbcType
                            + " which this framework does not know how to map to Parquet. "
                            + "Extend SchemaIntrospector.mapSqlType to support it.");
        };
    }

    private ParquetColumnType mapParquetType(SqlColumnType sqlType) {
        return switch (sqlType) {
            case VARCHAR, CHAR, TEXT -> ParquetColumnType.STRING;
            case TINYINT, SMALLINT, INTEGER -> ParquetColumnType.INT;
            case BIGINT -> ParquetColumnType.LONG;
            case DECIMAL -> ParquetColumnType.DECIMAL;
            case DOUBLE -> ParquetColumnType.DOUBLE;
            case FLOAT -> ParquetColumnType.FLOAT;
            case DATE -> ParquetColumnType.DATE;
            case DATETIME, TIMESTAMP -> ParquetColumnType.TIMESTAMP;
            case BOOLEAN -> ParquetColumnType.BOOLEAN;
            case BLOB -> ParquetColumnType.BYTES;
        };
    }
}
