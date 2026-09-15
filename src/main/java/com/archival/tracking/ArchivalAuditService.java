package com.archival.tracking;

import com.archival.config.ArchivalProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Durable, cross-run tracking of archival job progress, keyed by
 * (tableName, windowStart, windowEnd). This is the framework's single
 * source of truth for:
 * <ul>
 *     <li>the watermark used to auto-compute the next 4-month window;</li>
 *     <li>the safety gate that prevents the delete step from ever running
 *         against a window that was not successfully exported and reconciled;</li>
 *     <li>idempotency across retries/restarts of the same window.</li>
 * </ul>
 */
@Repository
public class ArchivalAuditService {

    private static final Logger log = LoggerFactory.getLogger(ArchivalAuditService.class);

    private final JdbcTemplate jdbcTemplate;
    private final String auditTable;

    public ArchivalAuditService(JdbcTemplate jdbcTemplate, ArchivalProperties properties) {
        this.jdbcTemplate = jdbcTemplate;
        this.auditTable = properties.getAuditTable();
    }

    public Optional<ArchivalAuditRecord> find(String tableName, LocalDate windowStart, LocalDate windowEnd) {
        String sql = "SELECT * FROM " + auditTable + " WHERE table_name = ? AND window_start = ? AND window_end = ?";
        List<ArchivalAuditRecord> results = jdbcTemplate.query(sql, this::mapRow, tableName, windowStart, windowEnd);
        return results.stream().findFirst();
    }

    /**
     * Finds the audit record for {@code tableName} whose recorded
     * {@code output_files} list contains an entry with exactly this file
     * name - matched by the base file name only, not the full path, since a
     * Parquet file may have been moved since it was originally exported
     * (e.g. retrieved from cold storage to a new location before being
     * restored). This is what lets the restore job validate a file it's
     * about to read really was produced by a real archival export for this
     * table, and recover which window it belongs to, without the caller
     * needing to separately track or supply {@code windowStart}/
     * {@code windowEnd}.
     */
    public Optional<ArchivalAuditRecord> findByOutputFileName(String tableName, String fileName) {
        // A LIKE prefilter narrows the rows read back from the database; the exact match (by base
        // file name, comparing every comma-separated entry) happens in Java below so a filename
        // that happens to be a substring of another table's or window's file name is never
        // mistaken for a real match.
        List<ArchivalAuditRecord> candidates = jdbcTemplate.query(
                "SELECT * FROM " + auditTable + " WHERE table_name = ? AND output_files LIKE ?",
                this::mapRow, tableName, "%" + fileName + "%");
        return candidates.stream()
                .filter(r -> outputFilesContainName(r.getOutputFiles(), fileName))
                .findFirst();
    }

    private boolean outputFilesContainName(String outputFilesCsv, String fileName) {
        if (outputFilesCsv == null || outputFilesCsv.isBlank()) {
            return false;
        }
        for (String entry : outputFilesCsv.split(",")) {
            String baseName = Paths.get(entry.trim()).getFileName().toString();
            if (baseName.equals(fileName)) {
                return true;
            }
        }
        return false;
    }

    /** Latest window_end among fully-completed (DELETED) runs for this table; empty if none yet. */
    public Optional<LocalDate> findWatermark(String tableName) {
        String sql = "SELECT MAX(window_end) FROM " + auditTable + " WHERE table_name = ? AND status = ?";
        LocalDate max = jdbcTemplate.queryForObject(sql, LocalDate.class, tableName, ArchivalRunStatus.DELETED.name());
        return Optional.ofNullable(max);
    }

    public void recordStarted(String tableName, LocalDate windowStart, LocalDate windowEnd, Long jobExecutionId) {
        Optional<ArchivalAuditRecord> existing = find(tableName, windowStart, windowEnd);
        if (existing.isPresent()) {
            jdbcTemplate.update(
                    "UPDATE " + auditTable + " SET job_execution_id = ?, status = ?, error_message = NULL, updated_at = ? " +
                            "WHERE table_name = ? AND window_start = ? AND window_end = ?",
                    jobExecutionId, ArchivalRunStatus.STARTED.name(), Timestamp.valueOf(LocalDateTime.now()),
                    tableName, windowStart, windowEnd);
        } else {
            jdbcTemplate.update(
                    "INSERT INTO " + auditTable +
                            " (job_execution_id, table_name, window_start, window_end, status, created_at, updated_at) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    jobExecutionId, tableName, windowStart, windowEnd, ArchivalRunStatus.STARTED.name(),
                    Timestamp.valueOf(LocalDateTime.now()), Timestamp.valueOf(LocalDateTime.now()));
        }
    }

    public void markExported(String tableName, LocalDate windowStart, LocalDate windowEnd, long exportedCount, List<String> outputFiles) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.EXPORTED, u -> {
            u.exportedRecordCount = exportedCount;
            u.outputFiles = String.join(",", outputFiles);
        });
    }

    public void markExportFailed(String tableName, LocalDate windowStart, LocalDate windowEnd, String error) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.EXPORT_FAILED, u -> u.errorMessage = truncate(error));
    }

    public void markReconciled(String tableName, LocalDate windowStart, LocalDate windowEnd, long reconciledCount) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.RECONCILED, u -> u.reconciledRecordCount = reconciledCount);
    }

    public void markReconcileFailed(String tableName, LocalDate windowStart, LocalDate windowEnd, String error) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.RECONCILE_FAILED, u -> u.errorMessage = truncate(error));
    }

    public void markDeleted(String tableName, LocalDate windowStart, LocalDate windowEnd, long deletedCount) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.DELETED, u -> u.deletedRecordCount = deletedCount);
    }

    public void markDeleteSkipped(String tableName, LocalDate windowStart, LocalDate windowEnd, String reason) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.DELETE_SKIPPED, u -> u.errorMessage = truncate(reason));
    }

    public void markDeleteFailed(String tableName, LocalDate windowStart, LocalDate windowEnd, String error) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.DELETE_FAILED, u -> u.errorMessage = truncate(error));
    }

    /** Marks the beginning of a restore attempt, before any rows are re-inserted. */
    public void markRestoreStarted(String tableName, LocalDate windowStart, LocalDate windowEnd) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.RESTORE_STARTED, u -> { });
    }

    public void markRestored(String tableName, LocalDate windowStart, LocalDate windowEnd, long restoredCount) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.RESTORED, u -> u.restoredRecordCount = restoredCount);
    }

    public void markRestoreSkipped(String tableName, LocalDate windowStart, LocalDate windowEnd, String reason) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.RESTORE_SKIPPED, u -> u.errorMessage = truncate(reason));
    }

    public void markRestoreFailed(String tableName, LocalDate windowStart, LocalDate windowEnd, String error) {
        update(tableName, windowStart, windowEnd, ArchivalRunStatus.RESTORE_FAILED, u -> u.errorMessage = truncate(error));
    }

    public boolean isAlreadyRestored(String tableName, LocalDate windowStart, LocalDate windowEnd) {
        return find(tableName, windowStart, windowEnd)
                .map(r -> r.getStatus() == ArchivalRunStatus.RESTORED)
                .orElse(false);
    }

    /** Safety check used by the delete tasklet: only ever true once export + reconciliation both succeeded. */
    public boolean isReconciled(String tableName, LocalDate windowStart, LocalDate windowEnd) {
        return find(tableName, windowStart, windowEnd)
                .map(r -> r.getStatus() == ArchivalRunStatus.RECONCILED)
                .orElse(false);
    }

    public boolean isAlreadyDeleted(String tableName, LocalDate windowStart, LocalDate windowEnd) {
        return find(tableName, windowStart, windowEnd)
                .map(r -> r.getStatus() == ArchivalRunStatus.DELETED)
                .orElse(false);
    }

    private interface Mutator {
        void apply(Fields f);
    }

    private static class Fields {
        Long exportedRecordCount;
        Long reconciledRecordCount;
        Long deletedRecordCount;
        Long restoredRecordCount;
        String outputFiles;
        String errorMessage;
    }

    private void update(String tableName, LocalDate windowStart, LocalDate windowEnd, ArchivalRunStatus status, Mutator mutator) {
        Fields f = new Fields();
        mutator.apply(f);
        int rows = jdbcTemplate.update(
                "UPDATE " + auditTable + " SET status = ?, " +
                        "exported_record_count = COALESCE(?, exported_record_count), " +
                        "reconciled_record_count = COALESCE(?, reconciled_record_count), " +
                        "deleted_record_count = COALESCE(?, deleted_record_count), " +
                        "restored_record_count = COALESCE(?, restored_record_count), " +
                        "output_files = COALESCE(?, output_files), " +
                        "error_message = ?, " +
                        "updated_at = ? " +
                        "WHERE table_name = ? AND window_start = ? AND window_end = ?",
                ps -> {
                    ps.setString(1, status.name());
                    setNullableLong(ps, 2, f.exportedRecordCount);
                    setNullableLong(ps, 3, f.reconciledRecordCount);
                    setNullableLong(ps, 4, f.deletedRecordCount);
                    setNullableLong(ps, 5, f.restoredRecordCount);
                    ps.setString(6, f.outputFiles);
                    ps.setString(7, f.errorMessage);
                    ps.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
                    ps.setString(9, tableName);
                    ps.setObject(10, windowStart);
                    ps.setObject(11, windowEnd);
                });
        if (rows == 0) {
            log.warn("Audit update for status {} matched no row for {}/{}/{} - was recordStarted() called?",
                    status, tableName, windowStart, windowEnd);
        }
    }

    private void setNullableLong(java.sql.PreparedStatement ps, int index, Long value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.BIGINT);
        } else {
            ps.setLong(index, value);
        }
    }

    private String truncate(String s) {
        if (s == null) return null;
        return s.length() > 1900 ? s.substring(0, 1900) : s;
    }

    private ArchivalAuditRecord mapRow(ResultSet rs, int rowNum) throws SQLException {
        ArchivalAuditRecord r = new ArchivalAuditRecord();
        r.setId(rs.getLong("id"));
        r.setJobExecutionId(rs.getObject("job_execution_id") == null ? null : rs.getLong("job_execution_id"));
        r.setTableName(rs.getString("table_name"));
        r.setWindowStart(rs.getObject("window_start", LocalDate.class));
        r.setWindowEnd(rs.getObject("window_end", LocalDate.class));
        r.setStatus(ArchivalRunStatus.valueOf(rs.getString("status")));
        r.setExportedRecordCount(rs.getObject("exported_record_count") == null ? null : rs.getLong("exported_record_count"));
        r.setReconciledRecordCount(rs.getObject("reconciled_record_count") == null ? null : rs.getLong("reconciled_record_count"));
        r.setDeletedRecordCount(rs.getObject("deleted_record_count") == null ? null : rs.getLong("deleted_record_count"));
        r.setRestoredRecordCount(rs.getObject("restored_record_count") == null ? null : rs.getLong("restored_record_count"));
        r.setOutputFiles(rs.getString("output_files"));
        r.setErrorMessage(rs.getString("error_message"));
        Timestamp created = rs.getTimestamp("created_at");
        r.setCreatedAt(created == null ? null : created.toLocalDateTime());
        Timestamp updated = rs.getTimestamp("updated_at");
        r.setUpdatedAt(updated == null ? null : updated.toLocalDateTime());
        return r;
    }
}
