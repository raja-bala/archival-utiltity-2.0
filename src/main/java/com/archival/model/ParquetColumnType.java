package com.archival.model;

/**
 * Target Avro/Parquet logical type for a column, derived automatically from
 * its {@link SqlColumnType} by {@code com.archival.introspection.SchemaIntrospector}.
 * Drives dynamic Avro schema generation in {@code ParquetSchemaFactory}.
 */
public enum ParquetColumnType {
    STRING,
    INT,
    LONG,
    DOUBLE,
    FLOAT,
    BOOLEAN,
    DATE,
    TIMESTAMP,
    DECIMAL,
    BYTES
}
