package com.archival.job;

import com.archival.config.ArchivalProperties;
import com.archival.config.TableConfigFactory;
import com.archival.model.TableConfig;
import com.archival.util.DateWindowCalculator;
import com.archival.util.ResolvedWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;

/**
 * The command-line entry point into a single archival run.
 * <p>
 * Supported job parameters (all passed as standard Spring Boot
 * {@code --name=value} arguments):
 * <ul>
 *     <li><b>--tableName</b> (required) - the physical MariaDB table to
 *         archive from, e.g. {@code --tableName=customer_orders}. Every
 *         column in the table is discovered automatically via JDBC
 *         metadata; the table must have a {@code fye} date/datetime column.</li>
 *     <li><b>--primaryKeyColumn</b> (optional, default {@code id}) - only
 *         needed when the table's primary key column is not literally
 *         named {@code id}.</li>
 *     <li><b>--additionalWhereClause</b> (optional) - an extra SQL
 *         predicate ANDed into every generated query, e.g.
 *         {@code --additionalWhereClause="status = 'CLOSED'"}.</li>
 *     <li><b>--partitionGranularity</b> (optional, default {@code MONTH}) -
 *         how exported rows are grouped into separate Parquet files
 *         ({@code EXACT_DATE}, {@code MONTH}, {@code YEAR}, or {@code NONE}).</li>
 *     <li><b>--windowStart</b> (optional) - ISO date (yyyy-MM-dd) to start
 *         the 4-month window from. If omitted, resolved automatically from
 *         the audit table's watermark, or the oldest {@code fye} in the
 *         source table on the very first run.</li>
 *     <li><b>--retentionYears</b> (optional, default from application.yml,
 *         normally 5) - only data older than this is eligible.</li>
 *     <li><b>--windowMonths</b> (optional, default from application.yml,
 *         normally 4) - width of the window processed by this execution.</li>
 *     <li><b>--dryRun</b> (optional, default false) - export and reconcile
 *         normally, but skip the physical delete.</li>
 * </ul>
 * Example:
 * <pre>
 *   java -jar archival-utility.jar \
 *       --tableName=customer_orders \
 *       --primaryKeyColumn=order_id \
 *       --retentionYears=5 \
 *       --windowMonths=4
 * </pre>
 * <p>
 * <b>Restore mode</b> - pass <b>--operation=restore</b> to instead re-insert
 * a previously exported-and-deleted window's data back into its source
 * table from Parquet file(s) you name explicitly (see
 * {@code com.archival.restore.RestoreTasklet}). Restore deliberately takes
 * no window or path as input at all - you name the actual Parquet file(s)
 * you have in hand, and the framework validates each one, by file name,
 * against what {@code archival_job_audit} recorded as that table's real
 * export output before restoring anything, and recovers which window they
 * belong to from that same lookup:
 * <ul>
 *     <li><b>--operation=restore</b> (required to enter restore mode).</li>
 *     <li><b>--tableName</b> (required) - same table that was archived.</li>
 *     <li><b>--primaryKeyColumn</b> (optional, default {@code id}) - same
 *         value used when that window was archived.</li>
 *     <li><b>--parquetFile</b> (required, repeatable) - one or more Parquet
 *         files to restore from. Each is validated by file name against
 *         {@code archival_job_audit.output_files} for this table; a file
 *         whose name was never recorded there is refused. All files given
 *         in one run must belong to the same archived window.</li>
 * </ul>
 * Example:
 * <pre>
 *   java -jar archival-utility.jar \
 *       --operation=restore \
 *       --tableName=customer_orders \
 *       --primaryKeyColumn=order_id \
 *       --parquetFile=/data/archival/output/customer_orders/2015-01/customer_orders_fye-2015-01_run-....parquet \
 *       --parquetFile=/data/archival/output/customer_orders/2015-02/customer_orders_fye-2015-02_run-....parquet
 * </pre>
 * This class deliberately contains the only table-agnostic "orchestration"
 * logic that touches the audit table before a job even starts (computing
 * the window). There is no per-table config file to load - everything the
 * framework needs beyond these arguments is discovered from the database
 * itself by {@link TableConfigFactory}.
 */
@Component
@ConditionalOnProperty(prefix = "archival.runner", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ArchivalApplicationRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ArchivalApplicationRunner.class);

    private final TableConfigFactory tableConfigFactory;
    private final DateWindowCalculator dateWindowCalculator;
    private final JobLauncher jobLauncher;
    private final Job archivalJob;
    private final Job archivalRestoreJob;
    private final ArchivalProperties properties;
    private final ConfigurableApplicationContext applicationContext;

    public ArchivalApplicationRunner(TableConfigFactory tableConfigFactory, DateWindowCalculator dateWindowCalculator,
                                      JobLauncher jobLauncher, Job archivalJob,
                                      @Qualifier("archivalRestoreJob") Job archivalRestoreJob,
                                      ArchivalProperties properties,
                                      ConfigurableApplicationContext applicationContext) {
        this.tableConfigFactory = tableConfigFactory;
        this.dateWindowCalculator = dateWindowCalculator;
        this.jobLauncher = jobLauncher;
        this.archivalJob = archivalJob;
        this.archivalRestoreJob = archivalRestoreJob;
        this.properties = properties;
        this.applicationContext = applicationContext;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        int exitCode;
        try {
            exitCode = doRun(args);
        } catch (Exception e) {
            log.error("Archival run failed with an unexpected error", e);
            exitCode = 1;
        }
        final int finalExitCode = exitCode;
        int code = SpringApplication.exit(applicationContext, () -> finalExitCode);
        if (code != 0) {
            System.exit(code);
        }
    }

    private int doRun(ApplicationArguments args) throws Exception {
        String operation = singleOption(args, "operation").orElse("archive");
        return switch (operation.toLowerCase(java.util.Locale.ROOT)) {
            case "archive" -> doArchive(args);
            case "restore" -> doRestore(args);
            default -> throw new IllegalArgumentException(
                    "Unknown --operation='" + operation + "' (expected 'archive' or 'restore')");
        };
    }

    private int doArchive(ApplicationArguments args) throws Exception {
        String tableName = singleOption(args, "tableName")
                .orElseThrow(() -> new IllegalArgumentException("Missing required argument --tableName=<table>"));
        String primaryKeyColumn = singleOption(args, "primaryKeyColumn").orElse(null);
        String additionalWhereClause = singleOption(args, "additionalWhereClause").orElse(null);
        String partitionGranularity = singleOption(args, "partitionGranularity").orElse(null);

        LocalDate explicitWindowStart = singleOption(args, "windowStart").map(LocalDate::parse).orElse(null);
        int retentionYears = singleOption(args, "retentionYears").map(Integer::parseInt).orElse(properties.getRetentionYears());
        int windowMonths = singleOption(args, "windowMonths").map(Integer::parseInt).orElse(properties.getWindowMonths());
        boolean dryRun = singleOption(args, "dryRun").map(Boolean::parseBoolean).orElse(properties.isDryRunDefault());

        // Lightweight only - no column introspection yet, since we don't know
        // whether there is an eligible window to process at all.
        TableConfig tableConfig = tableConfigFactory.buildForWindowResolution(tableName, primaryKeyColumn, additionalWhereClause);
        ResolvedWindow window = dateWindowCalculator.resolve(tableConfig, explicitWindowStart, retentionYears, windowMonths);

        if (!window.eligible()) {
            log.info("Nothing eligible to archive for '{}' right now (retention cutoff {}). Exiting without launching a job.",
                    tableName, window.retentionCutoff());
            return 0;
        }

        log.info("Launching archival job for table '{}': window [{}, {}), retentionCutoff={}, dryRun={}",
                tableName, window.windowStart(), window.windowEnd(), window.retentionCutoff(), dryRun);

        JobParametersBuilder builder = new JobParametersBuilder()
                .addString("tableName", tableName)
                .addString("primaryKeyColumn", tableConfig.getPrimaryKeyColumn(), false)
                .addString("windowStart", window.windowStart().toString())
                .addString("windowEnd", window.windowEnd().toString())
                .addString("dryRun", String.valueOf(dryRun), false)
                .addLong("retentionYears", (long) retentionYears, false)
                .addLong("windowMonths", (long) windowMonths, false)
                .addString("runToken", Long.toString(System.nanoTime()), false);
        if (additionalWhereClause != null && !additionalWhereClause.isBlank()) {
            builder.addString("additionalWhereClause", additionalWhereClause, false);
        }
        if (partitionGranularity != null && !partitionGranularity.isBlank()) {
            builder.addString("partitionGranularity", partitionGranularity, false);
        }
        JobParameters jobParameters = builder.toJobParameters();

        JobExecution execution;
        try {
            execution = jobLauncher.run(archivalJob, jobParameters);
        } catch (JobInstanceAlreadyCompleteException e) {
            log.info("Window [{}, {}) for '{}' was already completed in a previous run - nothing to do.",
                    window.windowStart(), window.windowEnd(), tableName);
            return 0;
        }

        log.info("Archival job finished with status {} for '{}' window [{}, {})",
                execution.getStatus(), tableName, window.windowStart(), window.windowEnd());
        return execution.getStatus() == BatchStatus.COMPLETED ? 0 : 1;
    }

    private int doRestore(ApplicationArguments args) throws Exception {
        String tableName = singleOption(args, "tableName")
                .orElseThrow(() -> new IllegalArgumentException("Missing required argument --tableName=<table>"));
        String primaryKeyColumnArg = singleOption(args, "primaryKeyColumn").orElse(null);
        String additionalWhereClause = singleOption(args, "additionalWhereClause").orElse(null);
        String partitionGranularity = singleOption(args, "partitionGranularity").orElse(null);

        List<String> parquetFiles = args.getOptionValues("parquetFile");
        if (parquetFiles == null || parquetFiles.isEmpty()) {
            throw new IllegalArgumentException(
                    "--operation=restore requires at least one --parquetFile=<path> - the exact Parquet file(s) to "
                            + "restore from. There is no --windowStart/--windowEnd or base-path input for restore: "
                            + "each given file is validated by its file name against archival_job_audit.output_files "
                            + "for this table, which is also how the window to restore is determined.");
        }

        // Cheap resolution only (no schema introspection yet) just to apply the
        // same "id"-default convention used when the window was archived.
        TableConfig tableConfig = tableConfigFactory.buildForWindowResolution(tableName, primaryKeyColumnArg, additionalWhereClause);

        log.info("Launching restore job for table '{}' from {} given file(s): {}", tableName, parquetFiles.size(), parquetFiles);

        JobParametersBuilder builder = new JobParametersBuilder()
                .addString("tableName", tableName)
                .addString("primaryKeyColumn", tableConfig.getPrimaryKeyColumn(), false)
                .addString("restoreFiles", String.join(",", parquetFiles))
                .addString("runToken", Long.toString(System.nanoTime()), false);
        if (additionalWhereClause != null && !additionalWhereClause.isBlank()) {
            builder.addString("additionalWhereClause", additionalWhereClause, false);
        }
        if (partitionGranularity != null && !partitionGranularity.isBlank()) {
            builder.addString("partitionGranularity", partitionGranularity, false);
        }
        JobParameters jobParameters = builder.toJobParameters();

        JobExecution execution;
        try {
            execution = jobLauncher.run(archivalRestoreJob, jobParameters);
        } catch (JobInstanceAlreadyCompleteException e) {
            log.info("Restore of {} for '{}' was already completed in a previous run - nothing to do.",
                    parquetFiles, tableName);
            return 0;
        }

        log.info("Restore job finished with status {} for '{}' from {} given file(s)",
                execution.getStatus(), tableName, parquetFiles.size());
        return execution.getStatus() == BatchStatus.COMPLETED ? 0 : 1;
    }

    private java.util.Optional<String> singleOption(ApplicationArguments args, String name) {
        List<String> values = args.getOptionValues(name);
        if (values == null || values.isEmpty()) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(values.get(0));
    }
}
