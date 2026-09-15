package com.archival.writer;

import com.archival.model.ColumnDefinition;
import com.archival.model.TableConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;

/**
 * Converts a generic row ({@code Map<String,Object>}, as produced by
 * {@code GenericRowMapper}) into an Avro {@link GenericRecord} matching the
 * schema {@link ParquetSchemaFactory} built for the same {@link TableConfig}.
 * <p>
 * Date/time values are stored as UTC-based Avro logical types (days-since-epoch
 * for DATE, millis-since-epoch for TIMESTAMP); decimals are stored using the
 * standard Avro "bytes" decimal encoding (scaled two's-complement unscaled value).
 */
public final class ParquetRecordConverter {

    private ParquetRecordConverter() {
    }

    public static GenericRecord toGenericRecord(Schema schema, TableConfig config, Map<String, Object> row) {
        GenericData.Record record = new GenericData.Record(schema);
        for (ColumnDefinition column : config.getColumns()) {
            Object rawValue = row.get(column.getName());
            record.put(column.getName(), convert(column, rawValue));
        }
        return record;
    }

    /**
     * Converts one raw column value (as produced by {@code GenericRowMapper}
     * from a live JDBC row) into the exact same representation that value
     * would take inside an Avro {@link GenericRecord} written by this class -
     * e.g. an epoch-day {@code Integer} for a DATE column, an epoch-millis
     * {@code Long} for a TIMESTAMP column, a two's-complement {@code ByteBuffer}
     * for a DECIMAL column. Exposed (not just used internally by
     * {@link #toGenericRecord}) so that {@code ParquetReconciliationTasklet}
     * can convert a freshly-read source row the same way and compare it
     * directly, field for field, against what a Parquet reader hands back
     * for the same column.
     */
    public static Object convert(ColumnDefinition column, Object rawValue) {
        if (rawValue == null) {
            return null;
        }
        return switch (column.getParquetType()) {
            case STRING -> rawValue.toString();
            case INT -> ((Number) rawValue).intValue();
            case LONG -> ((Number) rawValue).longValue();
            case DOUBLE -> ((Number) rawValue).doubleValue();
            case FLOAT -> ((Number) rawValue).floatValue();
            case BOOLEAN -> rawValue instanceof Boolean b ? b : Boolean.parseBoolean(rawValue.toString());
            case BYTES -> rawValue instanceof byte[] bytes ? ByteBuffer.wrap(bytes) : rawValue;
            case DATE -> toEpochDay((LocalDate) rawValue);
            case TIMESTAMP -> toEpochMillis(rawValue);
            case DECIMAL -> toDecimalBytes((BigDecimal) rawValue, column.scaleOrDefault());
        };
    }

    private static int toEpochDay(LocalDate date) {
        return (int) date.toEpochDay();
    }

    private static long toEpochMillis(Object value) {
        if (value instanceof LocalDateTime ldt) {
            return ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        }
        if (value instanceof LocalDate ld) {
            return ld.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        }
        if (value instanceof java.sql.Timestamp ts) {
            return ts.toInstant().toEpochMilli();
        }
        throw new IllegalArgumentException("Cannot convert " + value.getClass() + " to a timestamp");
    }

    private static ByteBuffer toDecimalBytes(BigDecimal value, int scale) {
        BigDecimal scaled = value.setScale(scale, RoundingMode.HALF_UP);
        BigInteger unscaled = scaled.unscaledValue();
        return ByteBuffer.wrap(unscaled.toByteArray());
    }

    /**
     * Inverse of {@link #convert(ColumnDefinition, Object)}: turns a value
     * read back out of an Avro {@link GenericRecord} (e.g. by
     * {@code AvroParquetReader}) into the native Java type the JDBC driver
     * expects for an INSERT of that column - e.g. a {@code LocalDate} from
     * an epoch-day {@code Integer}, a {@code LocalDateTime} from an
     * epoch-millis {@code Long}, a {@code BigDecimal} from the raw
     * two's-complement {@code ByteBuffer} the DECIMAL encoding uses. Used by
     * {@code RestoreTasklet} to re-insert previously-exported Parquet rows
     * back into the source table.
     */
    public static Object decode(ColumnDefinition column, Object avroValue) {
        if (avroValue == null) {
            return null;
        }
        return switch (column.getParquetType()) {
            case STRING -> avroValue.toString();
            case INT -> ((Number) avroValue).intValue();
            case LONG -> ((Number) avroValue).longValue();
            case DOUBLE -> ((Number) avroValue).doubleValue();
            case FLOAT -> ((Number) avroValue).floatValue();
            case BOOLEAN -> avroValue instanceof Boolean b ? b : Boolean.parseBoolean(avroValue.toString());
            case BYTES -> toByteArray(avroValue);
            case DATE -> LocalDate.ofEpochDay(((Number) avroValue).longValue());
            case TIMESTAMP -> java.time.Instant.ofEpochMilli(((Number) avroValue).longValue())
                    .atZone(ZoneOffset.UTC).toLocalDateTime();
            case DECIMAL -> fromDecimalBytes(avroValue, column.scaleOrDefault());
        };
    }

    private static byte[] toByteArray(Object avroValue) {
        ByteBuffer buffer = ((ByteBuffer) avroValue).duplicate();
        buffer.rewind();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    private static BigDecimal fromDecimalBytes(Object avroValue, int scale) {
        byte[] bytes = toByteArray(avroValue);
        BigInteger unscaled = new BigInteger(bytes);
        return new BigDecimal(unscaled, scale);
    }
}
