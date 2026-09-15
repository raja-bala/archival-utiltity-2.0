package com.archival.util;

import java.time.LocalDate;

/**
 * The concrete [windowStart, windowEnd) fye range a single job execution will process,
 * plus the retention cutoff it was computed against.
 */
public record ResolvedWindow(LocalDate windowStart, LocalDate windowEnd, LocalDate retentionCutoff, boolean eligible) {

    public static ResolvedWindow none(LocalDate windowStart, LocalDate retentionCutoff) {
        return new ResolvedWindow(windowStart, windowStart, retentionCutoff, false);
    }
}
