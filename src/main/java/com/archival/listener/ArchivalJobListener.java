package com.archival.listener;

import com.archival.model.TableConfig;
import com.archival.tracking.ArchivalAuditService;
import com.archival.tracking.ArchivalRunStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;

import java.time.LocalDate;

/**
 * Job-level bookkeeping: opens the audit row for this window when the job
 * starts, and provides a final fallback (in case some unexpected error
 * happened outside of any step's own listener) so a run never leaves an
 * ambiguous audit state.
 */
public class ArchivalJobListener implements JobExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(ArchivalJobListener.class);

    private final ArchivalAuditService auditService;
    private final TableConfig tableConfig;
    private final LocalDate windowStart;
    private final LocalDate windowEnd;

    public ArchivalJobListener(ArchivalAuditService auditService, TableConfig tableConfig,
                                LocalDate windowStart, LocalDate windowEnd) {
        this.auditService = auditService;
        this.tableConfig = tableConfig;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Starting archival job '{}' (executionId={}) for table '{}', window [{}, {})",
                jobExecution.getJobInstance().getJobName(), jobExecution.getId(),
                tableConfig.getTableName(), windowStart, windowEnd);
        auditService.recordStarted(tableConfig.getTableName(), windowStart, windowEnd, jobExecution.getId());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() != BatchStatus.COMPLETED) {
            auditService.find(tableConfig.getTableName(), windowStart, windowEnd).ifPresent(record -> {
                if (record.getStatus() == ArchivalRunStatus.STARTED) {
                    // No step-level listener got a chance to record a more specific failure reason.
                    auditService.markExportFailed(tableConfig.getTableName(), windowStart, windowEnd,
                            "Job ended with status " + jobExecution.getStatus() + " before any step recorded a result");
                }
            });
            log.error("Archival job for '{}' window [{}, {}) ended with status {}",
                    tableConfig.getTableName(), windowStart, windowEnd, jobExecution.getStatus());
        } else {
            log.info("Archival job for '{}' window [{}, {}) completed successfully.",
                    tableConfig.getTableName(), windowStart, windowEnd);
        }
    }
}
