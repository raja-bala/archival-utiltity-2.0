-- Framework tracking table. One row per (table_name, window_start, window_end).
-- Idempotent by design (IF NOT EXISTS) so it is safe to run on every app startup
-- against MariaDB or H2.
CREATE TABLE IF NOT EXISTS archival_job_audit (
    id                       BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_execution_id         BIGINT,
    table_name               VARCHAR(200)   NOT NULL,
    window_start             DATE           NOT NULL,
    window_end               DATE           NOT NULL,
    status                   VARCHAR(30)    NOT NULL,
    exported_record_count    BIGINT,
    reconciled_record_count  BIGINT,
    deleted_record_count     BIGINT,
    restored_record_count    BIGINT,
    output_files             TEXT,
    error_message            VARCHAR(2000),
    created_at               TIMESTAMP,
    updated_at               TIMESTAMP,
    CONSTRAINT uq_archival_job_audit_window UNIQUE (table_name, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS ix_archival_job_audit_table_status
    ON archival_job_audit (table_name, status);

-- Defensive add for databases that already had this table created by an
-- earlier version of the framework, before the restore job existed. Most
-- engines (MariaDB 10.5+, H2) support "ADD COLUMN IF NOT EXISTS"; on an
-- engine/version that doesn't, this statement is safe to remove or run
-- manually once, since the CREATE TABLE above already covers fresh installs.
ALTER TABLE archival_job_audit ADD COLUMN IF NOT EXISTS restored_record_count BIGINT;
