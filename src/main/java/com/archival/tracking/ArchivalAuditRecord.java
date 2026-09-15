package com.archival.tracking;

import java.time.LocalDate;
import java.time.LocalDateTime;

/** Read model for one row of the archival_job_audit table. */
public class ArchivalAuditRecord {
    private Long id;
    private Long jobExecutionId;
    private String tableName;
    private LocalDate windowStart;
    private LocalDate windowEnd;
    private ArchivalRunStatus status;
    private Long exportedRecordCount;
    private Long reconciledRecordCount;
    private Long deletedRecordCount;
    private Long restoredRecordCount;
    private String outputFiles;
    private String errorMessage;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public Long getJobExecutionId() { return jobExecutionId; }
    public void setJobExecutionId(Long jobExecutionId) { this.jobExecutionId = jobExecutionId; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public LocalDate getWindowStart() { return windowStart; }
    public void setWindowStart(LocalDate windowStart) { this.windowStart = windowStart; }
    public LocalDate getWindowEnd() { return windowEnd; }
    public void setWindowEnd(LocalDate windowEnd) { this.windowEnd = windowEnd; }
    public ArchivalRunStatus getStatus() { return status; }
    public void setStatus(ArchivalRunStatus status) { this.status = status; }
    public Long getExportedRecordCount() { return exportedRecordCount; }
    public void setExportedRecordCount(Long exportedRecordCount) { this.exportedRecordCount = exportedRecordCount; }
    public Long getReconciledRecordCount() { return reconciledRecordCount; }
    public void setReconciledRecordCount(Long reconciledRecordCount) { this.reconciledRecordCount = reconciledRecordCount; }
    public Long getDeletedRecordCount() { return deletedRecordCount; }
    public void setDeletedRecordCount(Long deletedRecordCount) { this.deletedRecordCount = deletedRecordCount; }
    public Long getRestoredRecordCount() { return restoredRecordCount; }
    public void setRestoredRecordCount(Long restoredRecordCount) { this.restoredRecordCount = restoredRecordCount; }
    public String getOutputFiles() { return outputFiles; }
    public void setOutputFiles(String outputFiles) { this.outputFiles = outputFiles; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
    public LocalDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(LocalDateTime updatedAt) { this.updatedAt = updatedAt; }
}
