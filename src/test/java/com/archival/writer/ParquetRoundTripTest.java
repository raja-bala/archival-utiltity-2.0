package com.archival.writer;

import com.archival.io.LocalInputFile;
import com.archival.io.LocalOutputFile;
import com.archival.model.ColumnDefinition;
import com.archival.model.ParquetColumnType;
import com.archival.model.SqlColumnType;
import com.archival.model.TableConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end check of the generic Parquet write/read pipeline: dynamic Avro
 * schema generation, row conversion, and the Hadoop-free local
 * {@code OutputFile}/{@code InputFile} implementations - without needing a
 * real MariaDB instance or a Spring context.
 */
class ParquetRoundTripTest {

    @Test
    void writesAndReadsBackAllColumnTypesFaithfully(@TempDir Path tempDir) throws Exception {
        TableConfig tableConfig = buildTableConfig();
        Schema schema = ParquetSchemaFactory.buildSchema(tableConfig);

        Map<String, Object> fullRow = new LinkedHashMap<>();
        fullRow.put("id", 42L);
        fullRow.put("fye", LocalDate.of(2018, 3, 15));
        fullRow.put("amount", new BigDecimal("1234.56"));
        fullRow.put("label", "closed-period");
        fullRow.put("active", true);
        fullRow.put("closed_at", LocalDateTime.of(2018, 3, 16, 9, 30, 0));

        Map<String, Object> rowWithNulls = new LinkedHashMap<>();
        rowWithNulls.put("id", 43L);
        rowWithNulls.put("fye", LocalDate.of(2018, 3, 16));
        rowWithNulls.put("amount", null);
        rowWithNulls.put("label", null);
        rowWithNulls.put("active", null);
        rowWithNulls.put("closed_at", null);

        Path file = tempDir.resolve("round-trip-test.parquet");
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new LocalOutputFile(file))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            writer.write(ParquetRecordConverter.toGenericRecord(schema, tableConfig, fullRow));
            writer.write(ParquetRecordConverter.toGenericRecord(schema, tableConfig, rowWithNulls));
        }

        List<GenericRecord> readBack = new java.util.ArrayList<>();
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new LocalInputFile(file)).build()) {
            GenericRecord record;
            while ((record = reader.read()) != null) {
                readBack.add(record);
            }
        }

        assertThat(readBack).hasSize(2);

        GenericRecord first = readBack.get(0);
        assertThat(first.get("id")).isEqualTo(42L);
        assertThat(decodeDate(first, "fye")).isEqualTo(LocalDate.of(2018, 3, 15));
        assertThat(decodeDecimal(first, "amount", 2)).isEqualByComparingTo("1234.56");
        assertThat(first.get("label").toString()).isEqualTo("closed-period");
        assertThat(first.get("active")).isEqualTo(true);
        assertThat(decodeMillis(first, "closed_at")).isEqualTo(
                LocalDateTime.of(2018, 3, 16, 9, 30, 0).toInstant(ZoneOffset.UTC).toEpochMilli());

        GenericRecord second = readBack.get(1);
        assertThat(second.get("id")).isEqualTo(43L);
        assertThat(second.get("amount")).isNull();
        assertThat(second.get("label")).isNull();
        assertThat(second.get("active")).isNull();
        assertThat(second.get("closed_at")).isNull();
    }

    private TableConfig buildTableConfig() {
        TableConfig config = new TableConfig();
        config.setTableName("round_trip_test");
        config.setPrimaryKeyColumn("id");
        config.setFyeColumn("fye");
        config.setColumns(List.of(
                new ColumnDefinition("id", SqlColumnType.BIGINT, ParquetColumnType.LONG, false, null, null),
                new ColumnDefinition("fye", SqlColumnType.DATE, ParquetColumnType.DATE, false, null, null),
                new ColumnDefinition("amount", SqlColumnType.DECIMAL, ParquetColumnType.DECIMAL, true, 12, 2),
                new ColumnDefinition("label", SqlColumnType.VARCHAR, ParquetColumnType.STRING, true, null, null),
                new ColumnDefinition("active", SqlColumnType.BOOLEAN, ParquetColumnType.BOOLEAN, true, null, null),
                new ColumnDefinition("closed_at", SqlColumnType.TIMESTAMP, ParquetColumnType.TIMESTAMP, true, null, null)
        ));
        return config;
    }

    private LocalDate decodeDate(GenericRecord record, String field) {
        Object union = record.get(field);
        int epochDay = ((Number) union).intValue();
        return LocalDate.ofEpochDay(epochDay);
    }

    private long decodeMillis(GenericRecord record, String field) {
        return ((Number) record.get(field)).longValue();
    }

    private BigDecimal decodeDecimal(GenericRecord record, String field, int scale) {
        ByteBuffer buffer = (ByteBuffer) record.get(field);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale);
    }
}
