package com.archival.config;

import com.archival.model.PartitionGranularity;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Global, table-agnostic defaults for the archival framework.
 * <p>
 * There is deliberately no per-table configuration file: every table this
 * framework processes is expected to follow two fixed conventions - a
 * primary key column (named {@code id} by default, overridable per run via
 * {@code --primaryKeyColumn}) and an {@code fye} date/datetime column used
 * for the retention window. Everything else about a table (its full column
 * list and their types) is discovered at runtime via JDBC metadata - see
 * {@code com.archival.introspection.SchemaIntrospector}. This is what lets
 * the same jar, with zero config files and zero code changes, process any
 * table just by naming it on the command line.
 */
@ConfigurationProperties(prefix = "archival")
public class ArchivalProperties {

    /** Default retention window: rows are only eligible once fye is older than this many years. */
    private int retentionYears = 5;

    /** Default size, in months, of the fye window processed by a single job execution. */
    private int windowMonths = 4;

    /** Default base directory Parquet files are written under (a per-table subfolder is added automatically). */
    private String outputBaseDir = "/data/archival/output";

    /** Default primary key column name, used when --primaryKeyColumn is not supplied on the command line. */
    private String defaultPrimaryKeyColumn = "id";

    /** Default row-grouping strategy for Parquet output, used when --partitionGranularity is not supplied. */
    private PartitionGranularity defaultPartitionGranularity = PartitionGranularity.MONTH;

    /** JDBC fetch size for the streaming export cursor. */
    private int defaultFetchSize = 1000;

    /** Spring Batch chunk size for the export step. */
    private int defaultChunkSize = 500;

    /** Row batch size used when issuing chunked DELETE statements. */
    private int defaultDeleteBatchSize = 500;

    /** Target rows per Parquet part file before rolling over (0 = unlimited). */
    private long defaultMaxRowsPerFile = 0;

    /** Name of the framework's own audit/tracking table. */
    private String auditTable = "archival_job_audit";

    /** Master safety switch: when false, the delete step is skipped entirely regardless of reconciliation result. */
    private boolean deleteEnabled = true;

    /** When true, export and reconcile run normally but the delete step only logs what it would delete. */
    private boolean dryRunDefault = false;

    public int getRetentionYears() {
        return retentionYears;
    }

    public void setRetentionYears(int retentionYears) {
        this.retentionYears = retentionYears;
    }

    public int getWindowMonths() {
        return windowMonths;
    }

    public void setWindowMonths(int windowMonths) {
        this.windowMonths = windowMonths;
    }

    public String getOutputBaseDir() {
        return outputBaseDir;
    }

    public void setOutputBaseDir(String outputBaseDir) {
        this.outputBaseDir = outputBaseDir;
    }

    public String getDefaultPrimaryKeyColumn() {
        return defaultPrimaryKeyColumn;
    }

    public void setDefaultPrimaryKeyColumn(String defaultPrimaryKeyColumn) {
        this.defaultPrimaryKeyColumn = defaultPrimaryKeyColumn;
    }

    public PartitionGranularity getDefaultPartitionGranularity() {
        return defaultPartitionGranularity;
    }

    public void setDefaultPartitionGranularity(PartitionGranularity defaultPartitionGranularity) {
        this.defaultPartitionGranularity = defaultPartitionGranularity;
    }

    public int getDefaultFetchSize() {
        return defaultFetchSize;
    }

    public void setDefaultFetchSize(int defaultFetchSize) {
        this.defaultFetchSize = defaultFetchSize;
    }

    public int getDefaultChunkSize() {
        return defaultChunkSize;
    }

    public void setDefaultChunkSize(int defaultChunkSize) {
        this.defaultChunkSize = defaultChunkSize;
    }

    public int getDefaultDeleteBatchSize() {
        return defaultDeleteBatchSize;
    }

    public void setDefaultDeleteBatchSize(int defaultDeleteBatchSize) {
        this.defaultDeleteBatchSize = defaultDeleteBatchSize;
    }

    public long getDefaultMaxRowsPerFile() {
        return defaultMaxRowsPerFile;
    }

    public void setDefaultMaxRowsPerFile(long defaultMaxRowsPerFile) {
        this.defaultMaxRowsPerFile = defaultMaxRowsPerFile;
    }

    public String getAuditTable() {
        return auditTable;
    }

    public void setAuditTable(String auditTable) {
        this.auditTable = auditTable;
    }

    public boolean isDeleteEnabled() {
        return deleteEnabled;
    }

    public void setDeleteEnabled(boolean deleteEnabled) {
        this.deleteEnabled = deleteEnabled;
    }

    public boolean isDryRunDefault() {
        return dryRunDefault;
    }

    public void setDryRunDefault(boolean dryRunDefault) {
        this.dryRunDefault = dryRunDefault;
    }
}
