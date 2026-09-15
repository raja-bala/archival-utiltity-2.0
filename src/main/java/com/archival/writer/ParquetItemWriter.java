package com.archival.writer;

import com.archival.io.LocalOutputFile;
import com.archival.model.TableConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generic Spring Batch {@link ItemStreamWriter} that writes every exported
 * row to Apache Parquet, using an Avro schema built at runtime from the
 * table's {@link TableConfig} (see {@link ParquetSchemaFactory}).
 * <p>
 * Rows are grouped into separate part files by their {@code fye} value
 * according to {@code TableConfig.partitionGranularity} (requirement:
 * "Group/split output by fye where appropriate"), and, when
 * {@code maxRowsPerFile > 0}, further split into additional part files once
 * a group grows past that size.
 * <p>
 * Not thread-safe; Spring Batch gives each step its own writer instance
 * (the writer is declared {@code @StepScope}), so this is not an issue in
 * practice.
 */
public class ParquetItemWriter implements ItemStreamWriter<Map<String, Object>> {

    private static final Logger log = LoggerFactory.getLogger(ParquetItemWriter.class);
    private static final DateTimeFormatter RUN_STAMP = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");

    private final TableConfig tableConfig;
    private final String outputBaseDir;
    private final LocalDate windowStart;
    private final LocalDate windowEnd;
    private final String runToken;
    private final Schema schema;

    private final Map<String, GroupWriter> openWriters = new HashMap<>();
    private final Set<String> producedFiles = new LinkedHashSet<>();
    private long totalWritten = 0;
    private final String runStamp = RUN_STAMP.format(java.time.LocalDateTime.now());

    public ParquetItemWriter(TableConfig tableConfig, String outputBaseDir, LocalDate windowStart, LocalDate windowEnd, String runToken) {
        this.tableConfig = tableConfig;
        this.outputBaseDir = outputBaseDir;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.runToken = runToken;
        this.schema = ParquetSchemaFactory.buildSchema(tableConfig);
    }

    @Override
    public void open(ExecutionContext executionContext) {
        // Nothing to pre-open: part files are created lazily per partition key on first row.
        log.info("ParquetItemWriter opened for table '{}' window [{}, {}) -> base dir {}",
                tableConfig.getTableName(), windowStart, windowEnd, resolvedBaseDir());
    }

    @Override
    public void write(Chunk<? extends Map<String, Object>> chunk) throws Exception {
        for (Map<String, Object> row : chunk) {
            String partitionKey = PartitionKeyResolver.resolve(tableConfig, row);
            GroupWriter groupWriter = openWriters.computeIfAbsent(partitionKey, this::newGroupWriter);
            groupWriter.write(row);
            totalWritten++;
        }
    }

    private GroupWriter newGroupWriter(String partitionKey) {
        return new GroupWriter(partitionKey);
    }

    @Override
    public void update(ExecutionContext executionContext) {
        // Row-level restart mid-partition is intentionally not supported: on restart after a
        // failure the export step re-runs from scratch for this window (Parquet part files use
        // create-not-overwrite semantics, so a partial file from a failed attempt must be cleared
        // before restart - see README "Restart & failure semantics").
    }

    @Override
    public void close() throws ItemStreamException {
        IOException firstError = null;
        for (GroupWriter gw : openWriters.values()) {
            try {
                gw.close();
            } catch (IOException e) {
                log.error("Failed to close Parquet writer for partition {}", gw.partitionKey, e);
                if (firstError == null) firstError = e;
            }
        }
        if (firstError != null) {
            throw new ItemStreamException("Failed to close one or more Parquet part files", firstError);
        }
        log.info("ParquetItemWriter closed: {} rows written across {} file(s) for table '{}' window [{}, {})",
                totalWritten, producedFiles.size(), tableConfig.getTableName(), windowStart, windowEnd);
    }

    public long getTotalWritten() {
        return totalWritten;
    }

    public List<String> getProducedFiles() {
        return new ArrayList<>(producedFiles);
    }

    private String resolvedBaseDir() {
        return (tableConfig.getOutputBaseDir() != null && !tableConfig.getOutputBaseDir().isBlank())
                ? tableConfig.getOutputBaseDir()
                : outputBaseDir;
    }

    /** Owns the (possibly rolling-over) sequence of Parquet part files for one fye partition key. */
    private class GroupWriter {
        private final String partitionKey;
        private ParquetWriter<GenericRecord> currentWriter;
        private long rowsInCurrentFile = 0;
        private int partIndex = 0;

        GroupWriter(String partitionKey) {
            this.partitionKey = partitionKey;
        }

        void write(Map<String, Object> row) throws IOException {
            if (currentWriter == null || (tableConfig.getMaxRowsPerFile() > 0 && rowsInCurrentFile >= tableConfig.getMaxRowsPerFile())) {
                rollOver();
            }
            GenericRecord record = ParquetRecordConverter.toGenericRecord(schema, tableConfig, row);
            currentWriter.write(record);
            rowsInCurrentFile++;
        }

        private void rollOver() throws IOException {
            if (currentWriter != null) {
                currentWriter.close();
            }
            Path path = buildPartFilePath(partitionKey, partIndex++);
            producedFiles.add(path.toString());
            currentWriter = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(path))
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build();
            rowsInCurrentFile = 0;
            log.info("Opened Parquet part file: {}", path);
        }

        void close() throws IOException {
            if (currentWriter != null) {
                currentWriter.close();
            }
        }
    }

    private Path buildPartFilePath(String partitionKey, int partIndex) {
        String safeKey = partitionKey.replaceAll("[^A-Za-z0-9_-]", "_");
        String safeRunToken = (runToken == null ? "norun" : runToken).replaceAll("[^A-Za-z0-9_-]", "_");
        String fileName = String.format("%s_fye-%s_run-%s_token-%s_part-%03d.parquet",
                tableConfig.getTableName(), safeKey, runStamp, safeRunToken, partIndex);
        return Paths.get(resolvedBaseDir(), tableConfig.getTableName(), safeKey, fileName);
    }
}
