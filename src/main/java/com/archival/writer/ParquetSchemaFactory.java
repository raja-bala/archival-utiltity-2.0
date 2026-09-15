package com.archival.writer;

import com.archival.model.ColumnDefinition;
import com.archival.model.TableConfig;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.Arrays;

/**
 * Builds an Avro schema at runtime from a {@link TableConfig}. This is what
 * lets one generic {@code ParquetItemWriter} serve every onboarded table:
 * the schema is data, not code.
 */
public final class ParquetSchemaFactory {

    private ParquetSchemaFactory() {
    }

    public static Schema buildSchema(TableConfig config) {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder
                .record(sanitizeRecordName(config.getTableName()))
                .namespace("com.archival.export")
                .fields();

        for (ColumnDefinition column : config.getColumns()) {
            Schema fieldSchema = baseSchema(column);
            if (column.isNullable()) {
                Schema nullableSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), fieldSchema));
                fields = fields.name(column.getName())
                        .type(nullableSchema)
                        .withDefault(null);
            } else {
                fields = fields.name(column.getName()).type(fieldSchema).noDefault();
            }
        }
        return fields.endRecord();
    }

    private static Schema baseSchema(ColumnDefinition column) {
        return switch (column.getParquetType()) {
            case STRING -> Schema.create(Schema.Type.STRING);
            case INT -> Schema.create(Schema.Type.INT);
            case LONG -> Schema.create(Schema.Type.LONG);
            case DOUBLE -> Schema.create(Schema.Type.DOUBLE);
            case FLOAT -> Schema.create(Schema.Type.FLOAT);
            case BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            case BYTES -> Schema.create(Schema.Type.BYTES);
            case DATE -> LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            case TIMESTAMP -> LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            case DECIMAL -> LogicalTypes.decimal(column.precisionOrDefault(), column.scaleOrDefault())
                    .addToSchema(Schema.create(Schema.Type.BYTES));
        };
    }

    private static String sanitizeRecordName(String tableName) {
        String sanitized = tableName.replaceAll("[^A-Za-z0-9_]", "_");
        return Character.isDigit(sanitized.charAt(0)) ? "_" + sanitized : sanitized;
    }
}
