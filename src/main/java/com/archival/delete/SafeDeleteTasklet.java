package com.archival.delete;

import com.archival.model.TableConfig;
import com.archival.tracking.ArchivalAuditRecord;
import com.archival.tracking.ArchivalAuditService;
import com.archival.tracking.ArchivalRunStatus;
import com.archival.util.SqlBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Optional;

/**
 * Deletes archived rows from the source table - and only those rows -
 * strictly after the same window has been marked {@code RECONCILED}
 * (requirement: "Delete records from MariaDB only when they have been
 * successfully exported and reconciled").
 * <p>
 * Defense in depth, on top of the job-flow already refusing to reach this
 * step unless the reconciliation step completed successfully:
 * <ul>
 *     <li>re-checks the audit table's status is {@code RECONCILED} (not
 *         already {@code DELETED}, not anything else);</li>
 *     <li>re-counts the source table for the window immediately before
 *         deleting and aborts if it no longer matches the reconciled count
 *         (guards against rows landing in the window between reconciliation
 *         and delete);</li>
 *     <li>deletes in small batches via a bounded loop rather than one
 *         unbounded statement, to avoid long-held locks on large tables;</li>
 *     <li>honors a global kill switch ({@code archival.delete-enabled=false})
 *         and a per-run {@code --dryRun=true} flag, both of which skip the
 *         actual delete while still recording that reconciliation passed.</li>
 * </ul>
 */
public class SafeDeleteTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(SafeDeleteTasklet.class);
    private static final int MAX_ITERATIONS_SAFETY_MULTIPLIER = 4;

    private final ArchivalAuditService auditService;
    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;
    private final TableConfig tableConfig;
    private final LocalDate windowStart;
    private final LocalDate windowEnd;
    private final boolean deleteEnabled;
    private final boolean dryRun;

    public SafeDeleteTasklet(ArchivalAuditService auditService, JdbcTemplate jdbcTemplate,
                              TransactionTemplate transactionTemplate, TableConfig tableConfig,
                              LocalDate windowStart, LocalDate windowEnd, boolean deleteEnabled, boolean dryRun) {
        this.auditService = auditService;
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
        this.tableConfig = tableConfig;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.deleteEnabled = deleteEnabled;
        this.dryRun = dryRun;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        String tableName = tableConfig.getTableName();
        ArchivalAuditRecord audit = auditService.find(tableName, windowStart, windowEnd)
                .orElseThrow(() -> new IllegalStateException(
                        "No audit record for '" + tableName + "' window [" + windowStart + ", " + windowEnd + ")"));

        if (audit.getStatus() == ArchivalRunStatus.DELETED) {
            log.info("Window [{}, {}) for '{}' was already deleted in a previous run - skipping (idempotent).",
                    windowStart, windowEnd, tableName);
            return RepeatStatus.FINISHED;
        }

        if (audit.getStatus() != ArchivalRunStatus.RECONCILED) {
            String msg = "Refusing to delete: window is not in RECONCILED status (was " + audit.getStatus() + ")";
            log.error("{} for '{}' window [{}, {})", msg, tableName, windowStart, windowEnd);
            auditService.markDeleteFailed(tableName, windowStart, windowEnd, msg);
            throw new IllegalStateException(msg);
        }

        if (!deleteEnabled) {
            log.warn("archival.delete-enabled=false: skipping physical delete for '{}' window [{}, {}). Data remains in source table.",
                    tableName, windowStart, windowEnd);
            auditService.markDeleteSkipped(tableName, windowStart, windowEnd, "delete disabled globally (archival.delete-enabled=false)");
            return RepeatStatus.FINISHED;
        }

        if (dryRun) {
            long wouldDelete = currentWindowCount();
            log.info("[DRY RUN] Would delete {} rows from '{}' for window [{}, {}). No rows were actually deleted.",
                    wouldDelete, tableName, windowStart, windowEnd);
            auditService.markDeleteSkipped(tableName, windowStart, windowEnd, "dry-run (--dryRun=true)");
            return RepeatStatus.FINISHED;
        }

        long reconciledCount = Optional.ofNullable(audit.getReconciledRecordCount()).orElse(0L);
        long currentCount = currentWindowCount();
        if (currentCount != reconciledCount) {
            String msg = "Source row count for window (" + currentCount + ") no longer matches reconciled count ("
                    + reconciledCount + "); data changed after reconciliation. Aborting delete for safety.";
            log.error("{} table='{}' window=[{}, {})", msg, tableName, windowStart, windowEnd);
            auditService.markDeleteFailed(tableName, windowStart, windowEnd, msg);
            throw new IllegalStateException(msg);
        }

        long totalDeleted = deleteInBatches(reconciledCount);

        if (totalDeleted != reconciledCount) {
            log.warn("Deleted {} rows but expected {} for '{}' window [{}, {}) - see audit table for details.",
                    totalDeleted, reconciledCount, tableName, windowStart, windowEnd);
        }
        auditService.markDeleted(tableName, windowStart, windowEnd, totalDeleted);
        log.info("Delete step completed for '{}' window [{}, {}): {} rows removed from '{}'.",
                tableName, windowStart, windowEnd, totalDeleted, tableName);
        return RepeatStatus.FINISHED;
    }

    private long currentWindowCount() {
        return jdbcTemplate.queryForObject(SqlBuilder.countInWindow(tableConfig), Long.class,
                Date.valueOf(windowStart), Date.valueOf(windowEnd));
    }

    private long deleteInBatches(long expectedTotal) {
        String deleteSql = SqlBuilder.deleteBatch(tableConfig);
        int batchSize = Math.max(1, tableConfig.getDeleteBatchSize());
        long maxIterations = Math.max(10, (expectedTotal / batchSize + 1) * MAX_ITERATIONS_SAFETY_MULTIPLIER);

        long totalDeleted = 0;
        long iterations = 0;
        int affected;
        do {
            final int fBatchSize = batchSize;
            affected = transactionTemplate.execute(status ->
                    jdbcTemplate.update(deleteSql, Date.valueOf(windowStart), Date.valueOf(windowEnd), fBatchSize));
            totalDeleted += affected;
            iterations++;
            if (iterations > maxIterations) {
                log.error("Delete loop for '{}' window [{}, {}) exceeded safety limit of {} iterations; stopping early.",
                        tableConfig.getTableName(), windowStart, windowEnd, maxIterations);
                break;
            }
        } while (affected > 0);
        return totalDeleted;
    }
}
