package com.archival.reconciliation;

import com.archival.io.LocalInputFile;
import com.archival.model.ColumnDefinition;
import com.archival.model.TableConfig;
import com.archival.reader.GenericRowMapper;
import com.archival.tracking.ArchivalAuditRecord;
import com.archival.tracking.ArchivalAuditService;
import com.archival.tracking.ArchivalRunStatus;
import com.archival.util.SqlBuilder;
import com.archival.writer.ParquetRecordConverter;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Validates and reconciles the Parquet files produced by the export step
 * before anything is allowed to be deleted from the source table
 * (requirement: "Validate/reconcile the generated Parquet files").
 * <p>
 * This is a full CONTENT-level reconciliation, not merely a row-count check:
 * <ol>
 *     <li>every Parquet file recorded by the export step must exist and be a
 *         readable, well-formed Parquet file with the expected column count;</li>
 *     <li>the number of rows decoded from Parquet must equal the row count
 *         the export step reported writing, and the source table must still
 *         report that same row count for the window (guards against
 *         concurrent writes to the window during/after the export);</li>
 *     <li>every row currently in the source table's window is then matched,
 *         by primary key, against a row decoded from the Parquet output, and
 *         <b>every column value is compared exactly</b> - byte-for-byte for
 *         decimal/binary columns, by underlying epoch value for date/timestamp
 *         columns - not just counted.</li>
 * </ol>
 * Only when every row and every column agree exactly between the Parquet
 * output and the live source table is the window marked {@code RECONCILED},
 * which is the sole precondition {@link com.archival.delete.SafeDeleteTasklet}
 * checks before deleting anything.
 * <p>
 * Trade-off: this holds every row of the window in memory twice (once as
 * decoded Parquet {@link GenericRecord}s, once as freshly-queried JDBC rows)
 * to do the comparison. For the bounded, multi-month windows this framework
 * is designed around that's a reasonable cost for the extra safety; a much
 * larger window would want a streaming merge-join instead.
 */
public class ParquetReconciliationTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(ParquetReconciliationTasklet.class);

    private final ArchivalAuditService auditService;
    private final JdbcTemplate jdbcTemplate;
    private final TableConfig tableConfig;
    private final LocalDate windowStart;
    private final LocalDate windowEnd;

    public ParquetReconciliationTasklet(ArchivalAuditService auditService, JdbcTemplate jdbcTemplate,
                                         TableConfig tableConfig, LocalDate windowStart, LocalDate windowEnd) {
        this.auditService = auditService;
        this.jdbcTemplate = jdbcTemplate;
        this.tableConfig = tableConfig;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        String tableName = tableConfig.getTableName();
        String pkColumn = tableConfig.getPrimaryKeyColumn();
        ArchivalAuditRecord audit = auditService.find(tableName, windowStart, windowEnd)
                .orElseThrow(() -> new IllegalStateException(
                        "No audit record found for '" + tableName + "' window [" + windowStart + ", " + windowEnd + ") - export step must run first"));

        if (audit.getStatus() != ArchivalRunStatus.EXPORTED) {
            throw new IllegalStateException("Refusing to reconcile '" + tableName + "' window [" + windowStart + ", " + windowEnd
                    + "): expected status EXPORTED but was " + audit.getStatus());
        }

        long exportedCount = Optional.ofNullable(audit.getExportedRecordCount()).orElse(0L);
        List<String> files = parseFileList(audit.getOutputFiles());

        if (files.isEmpty() && exportedCount == 0) {
            // Nothing was eligible in this window - trivially reconciled.
            auditService.markReconciled(tableName, windowStart, windowEnd, 0);
            return RepeatStatus.FINISHED;
        }

        try {
            // 1. Decode every Parquet row, keyed by primary key value.
            Map<String, GenericRecord> parquetRowsByPk = new HashMap<>();
            for (String file : files) {
                readParquetRowsInto(file, pkColumn, parquetRowsByPk);
            }
            long parquetRowCount = parquetRowsByPk.size();

            if (parquetRowCount != exportedCount) {
                fail(tableName, "Parquet row count (" + parquetRowCount + ") does not match exported row count (" + exportedCount
                        + ")" + (parquetRowCount < exportedCount
                        ? " - check for duplicate " + pkColumn + " values across the export's part files"
                        : ""));
                return RepeatStatus.FINISHED;
            }

            // 2. Re-read the current, live rows for this window from the source table.
            List<Map<String, Object>> dbRows = jdbcTemplate.query(SqlBuilder.selectForExport(tableConfig),
                    new GenericRowMapper(tableConfig),
                    java.sql.Date.valueOf(windowStart), java.sql.Date.valueOf(windowEnd));

            if (dbRows.size() != exportedCount) {
                fail(tableName, "Source table row count for window (" + dbRows.size() + ") no longer matches exported row count ("
                        + exportedCount + ") - data may have changed during export; refusing to delete");
                return RepeatStatus.FINISHED;
            }

            // 3. Content-match every live source row against its Parquet counterpart,
            //    column by column, not just by count.
            ColumnDefinition pkColumnDef = tableConfig.columnByName(pkColumn);
            for (Map<String, Object> dbRow : dbRows) {
                Object rawPk = dbRow.get(pkColumn);
                // Route through the same encoding a Parquet reader would hand back for this
                // column, so pkKey() compares apples to apples (e.g. a DECIMAL primary key's
                // JDBC BigDecimal vs. its raw Parquet ByteBuffer, not the two toString()s).
                String pkKey = pkKey(ParquetRecordConverter.convert(pkColumnDef, rawPk));
                GenericRecord parquetRow = parquetRowsByPk.remove(pkKey);
                if (parquetRow == null) {
                    fail(tableName, "Row with " + pkColumn + "=" + rawPk
                            + " exists in the source table for this window but was not found in the Parquet output");
                    return RepeatStatus.FINISHED;
                }
                for (ColumnDefinition column : tableConfig.getColumns()) {
                    Object expected = ParquetRecordConverter.convert(column, dbRow.get(column.getName()));
                    Object actual = parquetRow.get(column.getName());
                    if (!valuesMatch(expected, actual)) {
                        fail(tableName, "Column '" + column.getName() + "' mismatch for " + pkColumn + "=" + rawPk
                                + ": source table has [" + expected + "], Parquet output has [" + actual + "]");
                        return RepeatStatus.FINISHED;
                    }
                }
            }

            // Anything left over here has no matching source row - shouldn't happen once
            // the counts above already matched, but checked explicitly for safety.
            if (!parquetRowsByPk.isEmpty()) {
                fail(tableName, "Parquet output contains " + parquetRowsByPk.size()
                        + " row(s) with no matching row in the source table for this window (e.g. "
                        + pkColumn + "=" + parquetRowsByPk.keySet().iterator().next() + ")");
                return RepeatStatus.FINISHED;
            }

            log.info("Reconciliation for '{}' window [{}, {}): {} row(s) content-matched exactly, column by column, "
                            + "between the Parquet output and the live source table.",
                    tableName, windowStart, windowEnd, parquetRowCount);
            auditService.markReconciled(tableName, windowStart, windowEnd, parquetRowCount);
            return RepeatStatus.FINISHED;
        } catch (IOException e) {
            fail(tableName, "I/O error validating Parquet files: " + e.getMessage());
            return RepeatStatus.FINISHED;
        }
    }

    private void fail(String tableName, String message) {
        log.error("Reconciliation FAILED for '{}' window [{}, {}): {}", tableName, windowStart, windowEnd, message);
        auditService.markReconcileFailed(tableName, windowStart, windowEnd, message);
        throw new IllegalStateException(message);
    }

    /** Reads one Parquet file's rows, validating its schema and checking for cross-file duplicate keys. */
    private void readParquetRowsInto(String filePath, String pkColumn, Map<String, GenericRecord> out) throws IOException {
        Path path = Paths.get(filePath);
        if (!path.toFile().exists()) {
            throw new IOException("Expected Parquet file does not exist: " + filePath);
        }
        int expectedFieldCount = tableConfig.getColumns().size();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(path)).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                int actualFieldCount = record.getSchema().getFields().size();
                if (actualFieldCount != expectedFieldCount) {
                    throw new IOException("Schema mismatch in " + filePath + ": expected " + expectedFieldCount
                            + " columns, file has " + actualFieldCount);
                }
                String pkKey = pkKey(record.get(pkColumn));
                if (out.containsKey(pkKey)) {
                    throw new IOException("Duplicate " + pkColumn + "=" + pkKey + " found in Parquet output for this window "
                            + "(across one or more part files, most recently " + filePath + ")");
                }
                out.put(pkKey, record);
            }
        }
    }

    /**
     * Normalizes a primary key value - already in its Parquet/Avro-encoded
     * form (see {@link ParquetRecordConverter#convert}, e.g. an epoch-day
     * {@code Integer} for a DATE primary key, a raw {@code ByteBuffer} for a
     * DECIMAL one) - to a stable String usable as a map key. Callers on the
     * JDBC side MUST run the raw column value through
     * {@code ParquetRecordConverter.convert(pkColumnDefinition, rawValue)}
     * first, so both sides land in the same representation before reaching
     * here; a value read directly from a Parquet {@link GenericRecord} is
     * already in that form.
     * <p>
     * {@code ByteBuffer} needs special handling: its {@code toString()} only
     * describes position/limit/capacity, not content, so two different byte
     * sequences could otherwise collide into the same key.
     */
    private String pkKey(Object encodedPkValue) {
        if (encodedPkValue == null) {
            return null;
        }
        if (encodedPkValue instanceof ByteBuffer buffer) {
            ByteBuffer duplicate = buffer.duplicate().rewind();
            byte[] bytes = new byte[duplicate.remaining()];
            duplicate.get(bytes);
            return java.util.HexFormat.of().formatHex(bytes);
        }
        if (encodedPkValue instanceof CharSequence charSequence) {
            return charSequence.toString();
        }
        return String.valueOf(encodedPkValue);
    }

    /**
     * Compares one column's "expected" value (a live source-table value,
     * already run through {@link ParquetRecordConverter#convert}) against the
     * "actual" value read back from a Parquet {@link GenericRecord} for the
     * same column - both of which should, if the data truly matches, be in
     * the exact same underlying representation (epoch day/millis ints and
     * longs, raw decimal/binary {@code ByteBuffer}s, etc.). The only
     * remaining wrinkle is that Avro string fields decode as {@code Utf8}
     * rather than {@code String}, so string-like values are compared via
     * {@code toString()}.
     */
    private boolean valuesMatch(Object expected, Object actual) {
        if (expected == null || actual == null) {
            return expected == actual;
        }
        if (expected instanceof ByteBuffer expectedBytes && actual instanceof ByteBuffer actualBytes) {
            return expectedBytes.duplicate().rewind().equals(actualBytes.duplicate().rewind());
        }
        if (expected instanceof CharSequence && actual instanceof CharSequence) {
            return expected.toString().equals(actual.toString());
        }
        return Objects.equals(expected, actual);
    }

    private List<String> parseFileList(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        return List.of(csv.split(","));
    }
}
