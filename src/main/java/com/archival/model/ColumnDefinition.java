package com.archival.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Description of a single column, discovered at runtime via JDBC metadata
 * (see {@code com.archival.introspection.SchemaIntrospector}) rather than
 * read from any config file. This is the only place column-level knowledge
 * exists; the framework never hard-codes a table's column names in Java code.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColumnDefinition {

    /** Physical column name in the source MariaDB table. */
    private String name;

    /** JDBC-level source type, used to extract the value with the right accessor. */
    private SqlColumnType sqlType;

    /** Target Avro/Parquet logical type. */
    private ParquetColumnType parquetType;

    private boolean nullable = true;

    /** Only used when parquetType == DECIMAL. */
    private Integer precision;

    /** Only used when parquetType == DECIMAL. */
    private Integer scale;

    public int precisionOrDefault() {
        return precision == null ? 18 : precision;
    }

    public int scaleOrDefault() {
        return scale == null ? 2 : scale;
    }
}
