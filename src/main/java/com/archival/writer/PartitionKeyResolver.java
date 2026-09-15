package com.archival.writer;

import com.archival.model.PartitionGranularity;
import com.archival.model.TableConfig;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Derives the output-file grouping key for a row from its {@code fye} value,
 * per requirement "Group/split output by fye where appropriate".
 */
public final class PartitionKeyResolver {

    private static final DateTimeFormatter MONTH = DateTimeFormatter.ofPattern("yyyy-MM");
    private static final DateTimeFormatter YEAR = DateTimeFormatter.ofPattern("yyyy");
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private PartitionKeyResolver() {
    }

    public static String resolve(TableConfig config, Map<String, Object> row) {
        if (config.getPartitionGranularity() == PartitionGranularity.NONE) {
            return "all";
        }
        Object fyeValue = row.get(config.getFyeColumn());
        LocalDate fye = toLocalDate(fyeValue);
        if (fye == null) {
            return "unknown";
        }
        return switch (config.getPartitionGranularity()) {
            case EXACT_DATE -> DAY.format(fye);
            case MONTH -> MONTH.format(fye);
            case YEAR -> YEAR.format(fye);
            case NONE -> "all";
        };
    }

    private static LocalDate toLocalDate(Object value) {
        if (value == null) return null;
        if (value instanceof LocalDate ld) return ld;
        if (value instanceof LocalDateTime ldt) return ldt.toLocalDate();
        throw new IllegalArgumentException("fye column value is not a date/datetime: " + value.getClass());
    }
}
