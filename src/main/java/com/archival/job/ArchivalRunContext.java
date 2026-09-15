package com.archival.job;

import com.archival.model.TableConfig;

import java.time.LocalDate;

/**
 * Immutable snapshot of everything a single job execution needs, resolved
 * once per run from job parameters. Declared {@code @JobScope} in
 * {@link ArchivalJobConfig} so every step-scoped reader/writer/tasklet in
 * the run shares the exact same table config and window without each
 * re-parsing job parameters independently.
 */
public class ArchivalRunContext {

    private final TableConfig tableConfig;
    private final LocalDate windowStart;
    private final LocalDate windowEnd;
    private final boolean dryRun;
    private final String runToken;

    public ArchivalRunContext(TableConfig tableConfig, LocalDate windowStart, LocalDate windowEnd, boolean dryRun, String runToken) {
        this.tableConfig = tableConfig;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.dryRun = dryRun;
        this.runToken = runToken;
    }

    public TableConfig tableConfig() {
        return tableConfig;
    }

    public LocalDate windowStart() {
        return windowStart;
    }

    public LocalDate windowEnd() {
        return windowEnd;
    }

    public boolean dryRun() {
        return dryRun;
    }

    public String runToken() {
        return runToken;
    }
}
