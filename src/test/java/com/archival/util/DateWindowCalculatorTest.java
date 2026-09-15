package com.archival.util;

import com.archival.model.TableConfig;
import com.archival.tracking.ArchivalAuditService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DateWindowCalculatorTest {

    private JdbcTemplate jdbcTemplate;
    private ArchivalAuditService auditService;
    private DateWindowCalculator calculator;
    private TableConfig tableConfig;

    @BeforeEach
    void setUp() {
        jdbcTemplate = mock(JdbcTemplate.class);
        auditService = mock(ArchivalAuditService.class);
        calculator = new DateWindowCalculator(jdbcTemplate, auditService);

        tableConfig = new TableConfig();
        tableConfig.setTableName("test_table");
        tableConfig.setFyeColumn("fye");
    }

    @Test
    void explicitWindowStart_naiveEndWithinCutoff_isUsedAsIs() {
        LocalDate start = LocalDate.now().minusYears(8);

        ResolvedWindow window = calculator.resolve(tableConfig, start, 5, 4);

        assertThat(window.eligible()).isTrue();
        assertThat(window.windowStart()).isEqualTo(start);
        assertThat(window.windowEnd()).isEqualTo(start.plusMonths(4));
    }

    @Test
    void explicitWindowStart_naiveEndBeyondCutoff_isClippedToCutoff() {
        LocalDate cutoff = LocalDate.now().minusYears(5);
        LocalDate start = cutoff.minusMonths(2); // start + 4 months would overshoot the cutoff

        ResolvedWindow window = calculator.resolve(tableConfig, start, 5, 4);

        assertThat(window.eligible()).isTrue();
        assertThat(window.windowStart()).isEqualTo(start);
        assertThat(window.windowEnd()).isEqualTo(cutoff);
        assertThat(window.windowEnd()).isBefore(start.plusMonths(4).plusDays(1));
    }

    @Test
    void explicitWindowStart_atOrAfterCutoff_isNotEligible() {
        LocalDate cutoff = LocalDate.now().minusYears(5);

        ResolvedWindow window = calculator.resolve(tableConfig, cutoff, 5, 4);

        assertThat(window.eligible()).isFalse();
    }

    @Test
    void noExplicitStart_usesWatermarkFromAuditTable() {
        LocalDate watermark = LocalDate.now().minusYears(9);
        when(auditService.findWatermark("test_table")).thenReturn(Optional.of(watermark));

        ResolvedWindow window = calculator.resolve(tableConfig, null, 5, 4);

        assertThat(window.eligible()).isTrue();
        assertThat(window.windowStart()).isEqualTo(watermark);
    }

    @Test
    void noExplicitStart_noWatermark_fallsBackToOldestFyeInSourceTable() {
        LocalDate oldest = LocalDate.now().minusYears(10);
        when(auditService.findWatermark("test_table")).thenReturn(Optional.empty());
        when(jdbcTemplate.queryForObject(anyString(), (Class<LocalDate>) any())).thenReturn(oldest);

        ResolvedWindow window = calculator.resolve(tableConfig, null, 5, 4);

        assertThat(window.eligible()).isTrue();
        assertThat(window.windowStart()).isEqualTo(oldest);
    }

    @Test
    void noExplicitStart_noWatermark_emptySourceTable_isNotEligible() {
        when(auditService.findWatermark("test_table")).thenReturn(Optional.empty());
        when(jdbcTemplate.queryForObject(anyString(), (Class<LocalDate>) any())).thenReturn(null);

        ResolvedWindow window = calculator.resolve(tableConfig, null, 5, 4);

        assertThat(window.eligible()).isFalse();
    }
}
