package com.archival.model;

/**
 * Source (MariaDB) column type, discovered at runtime via JDBC metadata
 * (see {@code com.archival.introspection.SchemaIntrospector}).
 * Deliberately small/generic: only the JDBC extraction behavior needed by the
 * generic reader depends on this value. Table-specific meaning is never
 * hard-coded into framework code.
 */
public enum SqlColumnType {
    VARCHAR,
    CHAR,
    TEXT,
    INTEGER,
    BIGINT,
    SMALLINT,
    TINYINT,
    DECIMAL,
    DOUBLE,
    FLOAT,
    DATE,
    DATETIME,
    TIMESTAMP,
    BOOLEAN,
    BLOB
}
