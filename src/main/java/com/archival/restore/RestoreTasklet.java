package com.archival.restore;

import com.archival.io.LocalInputFile;
import com.archival.model.ColumnDefinition;
import com.archival.model.TableConfig;
import com.archival.tracking.ArchivalAuditRecord;
import com.archival.tracking.ArchivalAuditService;
import com.archival.tracking.ArchivalRunStatus;
import com.archival.util.SqlBuilder;
import com.archival.writer.ParquetRecordConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Restores a previously exported-and-deleted window of data back into its
 * original source table, reading the exact Parquet file(s) the caller names
 * and decoding each row back into native Java values via
 * {@link ParquetRecordConverter#decode(ColumnDefinition, Object)} - the
 * exact inverse of the encoding used on the way out.
 * <p>
 * This is a separate, standalone job/step from the main archival flow (see
 * {@code archivalRestoreJob} in {@code ArchivalJobConfig}) - it never runs as
 * part of the normal export/reconcile/delete pipeline.
 * <p>
 * Deliberately, the caller never supplies a window ({@code windowStart}/
 * {@code windowEnd}) or a base directory/path to restore "from" - only the
 * concrete Parquet file(s) to restore. Each file is validated purely by its
 * file name against the {@code archival_job_audit} table's recorded
 * {@code output_files} for this table ({@link ArchivalAuditService#findByOutputFileName}):
 * if a file's name was never recorded as an archival export's output for
 * this table, restore refuses outright, before touching the database. This
 * also recovers which window a file belongs to, so the caller does not need
 * to separately know or track {@code windowStart}/{@code windowEnd} - and it
 * means a file can be restored from wherever it currently lives (e.g.
 * retrieved from cold storage to a new path) rather than only from the exact
 * path it was originally written to.
 * <p>
 * Safety measures, mirroring {@code SafeDeleteTasklet}'s approach for the
 * opposite direction:
 * <ul>
 *     <li>every given file name must be found in the audit trail's recorded
 *         output for this table, or the whole restore is refused - a file
 *         that was not actually produced by a real archival export (or
 *         whose name was mistyped) is never silently accepted;</li>
 *     <li>every given file must resolve to the <em>same</em> archived
 *         window - a restore attempt mixing files from two different
 *         windows is refused;</li>
 *     <li>idempotent: a window already {@code RESTORED} is skipped, not
 *         re-inserted (which would duplicate rows);</li>
 *     <li>refuses to run unless the resolved window's audit status is
 *         {@code DELETED} or {@code RESTORE_FAILED} (i.e. it must have
 *         actually been archived-and-removed, or a previous restore attempt
 *         must have failed partway);</li>
 *     <li>refuses to run if the source table already has any rows in the
 *         window (would otherwise duplicate data);</li>
 *     <li>the total row count read out of the given files is compared
 *         against the window's previously recorded exported/reconciled
 *         count before any row is inserted - so restoring only some of a
 *         window's files (an incomplete set) is caught and refused, not
 *         silently accepted as a partial restore;</li>
 *     <li>all inserts happen inside a single transaction, and the actual
 *         number of rows inserted is verified against that same expected
 *         count afterward.</li>
 * </ul>
 * The audit table's {@code restored_record_count} column and the
 * {@code RESTORE_STARTED}/{@code RESTORED}/{@code RESTORE_SKIPPED}/
 * {@code RESTORE_FAILED} statuses give this operation the same durable,
 * queryable audit trail the archival side already has.
 */
public class RestoreTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(RestoreTasklet.class);

    private final ArchivalAuditService auditService;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final TableConfig tableConfig;
    private final List<String> restoreFiles;

    public RestoreTasklet(ArchivalAuditService auditService, JdbcTemplate jdbcTemplate,
                           TransactionTemplate transactionTemplate, TableConfig tableConfig,
                           List<String> restoreFiles) {
        this.auditService = auditService;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.tableConfig = tableConfig;
        this.restoreFiles = restoreFiles;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        String tableName = tableConfig.getTableName();

        if (restoreFiles == null || restoreFiles.isEmpty()) {
            throw new IllegalArgumentException("No Parquet file(s) given to restore from for table '" + tableName + "'");
        }

        ArchivalAuditRecord audit = resolveAndValidateAuditRecord(tableName);
        LocalDate windowStart = audit.getWindowStart();
        LocalDate windowEnd = audit.getWindowEnd();

        if (audit.getStatus() == ArchivalRunStatus.RESTORED) {
            log.info("Window [{}, {}) for '{}' was already restored in a previous run - skipping (idempotent).",
                    windowStart, windowEnd, tableName);
            return RepeatStatus.FINISHED;
        }

        if (audit.getStatus() != ArchivalRunStatus.DELETED && audit.getStatus() != ArchivalRunStatus.RESTORE_FAILED) {
            String msg = "Refusing to restore: window [" + windowStart + ", " + windowEnd + ") is not in DELETED "
                    + "(or RESTORE_FAILED) status (was " + audit.getStatus() + "). Only a window that was "
                    + "successfully exported, reconciled, and deleted can be restored.";
            log.error("{} table='{}'", msg, tableName);
            auditService.markRestoreFailed(tableName, windowStart, windowEnd, msg);
            throw new IllegalStateException(msg);
        }

        long expectedCount = Optional.ofNullable(audit.getReconciledRecordCount())
                .orElse(Optional.ofNullable(audit.getExportedRecordCount()).orElse(0L));

        long existingCount = currentWindowCount(windowStart, windowEnd);
        if (existingCount > 0) {
            String msg = "Refusing to restore: source table '" + tableName + "' already has " + existingCount
                    + " row(s) for window [" + windowStart + ", " + windowEnd + "). Restoring would create "
                    + "duplicates; clear these rows first if they are unexpected.";
            log.error(msg);
            auditService.markRestoreFailed(tableName, windowStart, windowEnd, msg);
            throw new IllegalStateException(msg);
        }

        auditService.markRestoreStarted(tableName, windowStart, windowEnd);

        try {
            List<Object[]> rows = new ArrayList<>();
            for (String file : restoreFiles) {
                readParquetRowsInto(file, rows);
            }

            if (expectedCount > 0 && rows.size() != expectedCount) {
                String msg = "Parquet row count (" + rows.size() + ") from the given file(s) does not match the "
                        + "window's recorded exported/reconciled count (" + expectedCount + "). This usually means "
                        + "not every file for this window was given - refusing to restore a partial data set.";
                log.error("{} table='{}' window=[{}, {})", msg, tableName, windowStart, windowEnd);
                auditService.markRestoreFailed(tableName, windowStart, windowEnd, msg);
                throw new IllegalStateException(msg);
            }

            String insertSql = SqlBuilder.insertRow(tableConfig);
            int[] argTypes = argTypes(tableConfig);

            int[] updateCounts = transactionTemplate.execute(status ->
                    jdbcTemplate.batchUpdate(insertSql, rows, argTypes));

            long totalInserted = 0;
            for (int count : updateCounts) {
                // Some JDBC drivers report SUCCESS_NO_INFO (-2) for batched inserts instead of an exact count.
                totalInserted += count >= 0 ? count : 1;
            }

            if (totalInserted != rows.size()) {
                log.warn("Batch insert reported {} affected rows but {} rows were submitted for '{}' window [{}, {}).",
                        totalInserted, rows.size(), tableName, windowStart, windowEnd);
            }

            long finalCount = currentWindowCount(windowStart, windowEnd);
            if (finalCount != rows.size()) {
                // The insert transaction above has already committed at this point (or it would
                // have thrown and rolled back on its own) - this check is defense in depth against
                // something else writing into the window concurrently, not an expected failure
                // mode. Do NOT simply re-run after this: the rows from this attempt are already in
                // the table, so a naive retry would be blocked by (and should be blocked by) the
                // "table already has rows for this window" guard above. Investigate the actual
                // row count for this window before deciding how to proceed.
                String msg = "Post-restore verification failed: source table now has " + finalCount
                        + " row(s) for the window but " + rows.size() + " were restored from Parquet. "
                        + "The insert already committed - do not blindly retry; inspect the table first.";
                log.error("{} table='{}' window=[{}, {})", msg, tableName, windowStart, windowEnd);
                auditService.markRestoreFailed(tableName, windowStart, windowEnd, msg);
                throw new IllegalStateException(msg);
            }

            auditService.markRestored(tableName, windowStart, windowEnd, rows.size());
            log.info("Restore completed for '{}' window [{}, {}): {} row(s) re-inserted from {} given Parquet file(s).",
                    tableName, windowStart, windowEnd, rows.size(), restoreFiles.size());
            return RepeatStatus.FINISHED;
        } catch (IOException e) {
            String msg = "I/O error reading Parquet files during restore: " + e.getMessage();
            log.error("{} table='{}' window=[{}, {})", msg, tableName, windowStart, windowEnd, e);
            auditService.markRestoreFailed(tableName, windowStart, windowEnd, msg);
            throw new IllegalStateException(msg, e);
        }
    }

    /**
     * Validates every entry in {@link #restoreFiles} by file name against
     * {@code archival_job_audit.output_files} for this table, and confirms
     * they all resolve to the same archived window. Returns that window's
     * audit record. Refuses (throws, without touching the database) if any
     * file is not recognized as an archived output for this table, or if
     * the given files span more than one window.
     */
    private ArchivalAuditRecord resolveAndValidateAuditRecord(String tableName) {
        ArchivalAuditRecord resolved = null;
        for (String filePath : restoreFiles) {
            String fileName = Paths.get(filePath).getFileName().toString();
            ArchivalAuditRecord match = auditService.findByOutputFileName(tableName, fileName)
                    .orElseThrow(() -> new IllegalStateException(
                            "File '" + fileName + "' is not recorded as an archived output file for table '"
                                    + tableName + "' in the audit trail - refusing to restore from an unverified file. "
                                    + "Double-check the file name, or confirm this file actually came from an "
                                    + "archival export of this table."));
            if (resolved == null) {
                resolved = match;
            } else if (!match.getWindowStart().equals(resolved.getWindowStart())
                    || !match.getWindowEnd().equals(resolved.getWindowEnd())) {
                throw new IllegalStateException("Given files belong to different archived windows for table '"
                        + tableName + "' (window [" + resolved.getWindowStart() + ", " + resolved.getWindowEnd()
                        + ") vs [" + match.getWindowStart() + ", " + match.getWindowEnd() + ")) - restore one "
                        + "window's files at a time.");
            }
        }
        return resolved;
    }

    private long currentWindowCount(LocalDate windowStart, LocalDate windowEnd) {
        return jdbcTemplate.queryForObject(SqlBuilder.countInWindow(tableConfig), Long.class,
                Date.valueOf(windowStart), Date.valueOf(windowEnd));
    }

    private void readParquetRowsInto(String filePath, List<Object[]> out) throws IOException {
        Path path = Paths.get(filePath);
        if (!path.toFile().exists()) {
            throw new IOException("Expected Parquet file does not exist: " + filePath);
        }
        List<ColumnDefinition> columns = tableConfig.getColumns();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(path)).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                Object[] row = new Object[columns.size()];
                for (int i = 0; i < columns.size(); i++) {
                    ColumnDefinition column = columns.get(i);
                    row[i] = toJdbcValue(ParquetRecordConverter.decode(column, record.get(column.getName())));
                }
                out.add(row);
            }
        }
    }

    /**
     * Converts {@code ParquetRecordConverter.decode}'s output into the
     * concrete {@code java.sql.*} types the rest of this codebase already
     * relies on for JDBC parameter binding (e.g. {@code SqlBuilder}'s
     * window-predicate queries bind {@code java.sql.Date}, not
     * {@code LocalDate}, for the same reason) rather than trusting every
     * JDBC driver to accept a raw {@code java.time} value via
     * {@code setObject}.
     */
    private Object toJdbcValue(Object decoded) {
        if (decoded instanceof LocalDate localDate) {
            return Date.valueOf(localDate);
        }
        if (decoded instanceof LocalDateTime localDateTime) {
            return Timestamp.valueOf(localDateTime);
        }
        return decoded;
    }

    private int[] argTypes(TableConfig config) {
        List<ColumnDefinition> columns = config.getColumns();
        int[] types = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            types[i] = switch (columns.get(i).getParquetType()) {
                case STRING -> Types.VARCHAR;
                case INT -> Types.INTEGER;
                case LONG -> Types.BIGINT;
                case DOUBLE -> Types.DOUBLE;
                case FLOAT -> Types.FLOAT;
                case BOOLEAN -> Types.BOOLEAN;
                case BYTES -> Types.VARBINARY;
                case DATE -> Types.DATE;
                case TIMESTAMP -> Types.TIMESTAMP;
                case DECIMAL -> Types.DECIMAL;
            };
        }
        return types;
    }
}
