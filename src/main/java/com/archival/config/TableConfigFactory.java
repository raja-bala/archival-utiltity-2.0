package com.archival.config;

import com.archival.introspection.SchemaIntrospector;
import com.archival.model.PartitionGranularity;
import com.archival.model.TableConfig;
import org.springframework.stereotype.Component;

/**
 * Assembles a {@link TableConfig} at runtime from command-line arguments and
 * (for a full run) JDBC schema introspection - there is no per-table config
 * file to load. See {@link TableConfig} for the full rationale.
 */
@Component
public class TableConfigFactory {

    private final SchemaIntrospector schemaIntrospector;
    private final ArchivalProperties properties;

    public TableConfigFactory(SchemaIntrospector schemaIntrospector, ArchivalProperties properties) {
        this.schemaIntrospector = schemaIntrospector;
        this.properties = properties;
    }

    /**
     * A lightweight {@link TableConfig} with no column list populated yet -
     * enough for {@code DateWindowCalculator} to resolve a window (it only
     * needs the table name, primary key, fye column and optional filter),
     * without paying for a schema introspection query before we even know
     * whether there is an eligible window to process.
     */
    public TableConfig buildForWindowResolution(String tableName, String primaryKeyColumn, String additionalWhereClause) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Missing required argument --tableName=<table>");
        }
        TableConfig config = new TableConfig();
        config.setTableName(tableName);
        config.setPrimaryKeyColumn(resolvePrimaryKeyColumn(primaryKeyColumn));
        config.setFyeColumn(TableConfig.FYE_COLUMN);
        config.setAdditionalWhereClause(additionalWhereClause);
        return config;
    }

    /**
     * The full {@link TableConfig}, with every column in the table
     * discovered via JDBC metadata - used once a job is actually about to
     * run.
     */
    public TableConfig buildFull(String tableName, String primaryKeyColumn, String additionalWhereClause,
                                  PartitionGranularity partitionGranularity) {
        TableConfig config = buildForWindowResolution(tableName, primaryKeyColumn, additionalWhereClause);
        config.setPartitionGranularity(partitionGranularity != null ? partitionGranularity : properties.getDefaultPartitionGranularity());
        config.setOutputBaseDir(properties.getOutputBaseDir());
        config.setFetchSize(properties.getDefaultFetchSize());
        config.setChunkSize(properties.getDefaultChunkSize());
        config.setDeleteBatchSize(properties.getDefaultDeleteBatchSize());
        config.setMaxRowsPerFile(properties.getDefaultMaxRowsPerFile());
        config.setColumns(schemaIntrospector.introspectColumns(tableName));
        return config;
    }

    private String resolvePrimaryKeyColumn(String primaryKeyColumn) {
        return (primaryKeyColumn == null || primaryKeyColumn.isBlank())
                ? properties.getDefaultPrimaryKeyColumn()
                : primaryKeyColumn;
    }
}
