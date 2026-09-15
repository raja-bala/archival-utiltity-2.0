package com.archival.tracking;

/** Lifecycle states for one (tableName, windowStart, windowEnd) archival run, persisted in the audit table. */
public enum ArchivalRunStatus {
    STARTED,
    EXPORTED,
    EXPORT_FAILED,
    RECONCILED,
    RECONCILE_FAILED,
    DELETED,
    DELETE_SKIPPED,
    DELETE_FAILED,
    RESTORE_STARTED,
    RESTORED,
    RESTORE_SKIPPED,
    RESTORE_FAILED
}
