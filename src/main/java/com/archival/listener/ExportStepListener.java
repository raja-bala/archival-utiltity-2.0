package com.archival.listener;

import com.archival.tracking.ArchivalAuditService;
import com.archival.writer.ParquetItemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import java.time.LocalDate;

/**
 * Bridges the export chunk step to the audit table: records how many rows
 * were written and to which Parquet files (on success), or the failure
 * reason (on failure), keyed by (tableName, windowStart, windowEnd).
 */
public class ExportStepListener implements StepExecutionListener {

    private static final Logger log = LoggerFactory.getLogger(ExportStepListener.class);

    private final ArchivalAuditService auditService;
    private final ParquetItemWriter writer;
    private final String tableName;
    private final LocalDate windowStart;
    private final LocalDate windowEnd;

    public ExportStepListener(ArchivalAuditService auditService, ParquetItemWriter writer,
                               String tableName, LocalDate windowStart, LocalDate windowEnd) {
        this.auditService = auditService;
        this.writer = writer;
        this.tableName = tableName;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        if (stepExecution.getStatus() == BatchStatus.COMPLETED) {
            long count = writer.getTotalWritten();
            log.info("Export step completed for '{}' window [{}, {}): {} rows written to {} file(s)",
                    tableName, windowStart, windowEnd, count, writer.getProducedFiles().size());
            auditService.markExported(tableName, windowStart, windowEnd, count, writer.getProducedFiles());
        } else {
            String reason = stepExecution.getExitStatus().getExitDescription();
            log.error("Export step failed for '{}' window [{}, {}): {}", tableName, windowStart, windowEnd, reason);
            auditService.markExportFailed(tableName, windowStart, windowEnd, reason);
        }
        return stepExecution.getExitStatus();
    }
}
