package com.archival;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * Generic Spring Batch archival/export utility.
 * <p>
 * Reads aged records (older than a configurable retention period, based on
 * their {@code fye} date) out of MariaDB in bounded 4-month windows, exports
 * them to partitioned Apache Parquet files, validates the output, and only
 * then deletes the corresponding rows from the source table.
 * <p>
 * The framework itself is 100% table-agnostic and needs no per-table config
 * file: the table name (and, if not {@code id}, the primary key column) is
 * simply passed on the command line via {@code --tableName} (see
 * {@link com.archival.job.ArchivalApplicationRunner}), and every other
 * column is discovered automatically via JDBC metadata (see
 * {@link com.archival.introspection.SchemaIntrospector}). This lets the
 * same jar be reused, unmodified, across many source tables and
 * applications.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class ArchivalApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ArchivalApplication.class);
        app.setWebApplicationType(org.springframework.boot.WebApplicationType.NONE);
        app.run(args);
    }
}
