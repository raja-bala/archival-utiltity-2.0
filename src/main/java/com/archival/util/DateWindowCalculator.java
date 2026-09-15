package com.archival.util;

import com.archival.model.TableConfig;
import com.archival.tracking.ArchivalAuditService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Optional;

/**
 * Computes the single 4-month {@code fye} window a job execution should
 * process (requirement: "Process only a 4-month fye window per job
 * execution" and "Identify records whose fye date is more than 5 years old").
 * <p>
 * Resolution order for the window start:
 * <ol>
 *     <li>an explicit {@code --windowStart} job parameter, if supplied;</li>
 *     <li>otherwise the watermark recorded in the audit table (window_end of
 *         the last fully-DELETED run for this table);</li>
 *     <li>otherwise {@code MIN(fyeColumn)} from the source table itself, i.e.
 *         start from the oldest data on the very first run.</li>
 * </ol>
 * The window end is always {@code min(windowStart + windowMonths, retentionCutoff)}
 * where {@code retentionCutoff = today - retentionYears}, so a run never
 * touches data younger than the retention threshold even if windowMonths
 * would otherwise overshoot it.
 */
@Component
public class DateWindowCalculator {

    private static final Logger log = LoggerFactory.getLogger(DateWindowCalculator.class);

    private final JdbcTemplate jdbcTemplate;
    private final ArchivalAuditService auditService;

    public DateWindowCalculator(JdbcTemplate jdbcTemplate, ArchivalAuditService auditService) {
        this.jdbcTemplate = jdbcTemplate;
        this.auditService = auditService;
    }

    public ResolvedWindow resolve(TableConfig tableConfig, LocalDate explicitWindowStart, int retentionYears, int windowMonths) {
        LocalDate retentionCutoff = LocalDate.now().minusYears(retentionYears);

        LocalDate windowStart = explicitWindowStart != null
                ? explicitWindowStart
                : auditService.findWatermark(tableConfig.getTableName())
                        .orElseGet(() -> findOldestFye(tableConfig).orElse(null));

        if (windowStart == null) {
            log.info("Table '{}': source table has no rows; nothing to archive.",
                    tableConfig.getTableName());
            return ResolvedWindow.none(retentionCutoff, retentionCutoff);
        }

        if (!windowStart.isBefore(retentionCutoff)) {
            log.info("Table '{}': window start {} has already caught up to the {}-year retention cutoff {}; nothing eligible yet.",
                    tableConfig.getTableName(), windowStart, retentionYears, retentionCutoff);
            return ResolvedWindow.none(windowStart, retentionCutoff);
        }

        LocalDate naiveEnd = windowStart.plusMonths(windowMonths);
        LocalDate windowEnd = naiveEnd.isAfter(retentionCutoff) ? retentionCutoff : naiveEnd;

        return new ResolvedWindow(windowStart, windowEnd, retentionCutoff, true);
    }

    private Optional<LocalDate> findOldestFye(TableConfig tableConfig) {
        String where = (tableConfig.getAdditionalWhereClause() == null || tableConfig.getAdditionalWhereClause().isBlank())
                ? ""
                : " WHERE " + tableConfig.getAdditionalWhereClause();
        String sql = "SELECT MIN(" + tableConfig.getFyeColumn() + ") FROM " + tableConfig.getTableName() + where;
        LocalDate min = jdbcTemplate.queryForObject(sql, LocalDate.class);
        return Optional.ofNullable(min);
    }
}
