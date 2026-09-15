package com.archival.processor;

import com.archival.model.ColumnDefinition;
import com.archival.model.TableConfig;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.validator.ValidationException;

import java.util.Map;

/**
 * Generic, table-agnostic row processor. It performs only structural
 * validation implied by the table's own declared metadata (non-nullable
 * columns must be present) - deliberately no business rules, since the
 * framework must stay free of table-specific logic. Applications that need
 * extra validation or enrichment can compose their own
 * {@code ItemProcessor} in front of/behind this one without modifying the
 * framework.
 */
public class GenericRecordValidatingProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>> {

    private final TableConfig tableConfig;

    public GenericRecordValidatingProcessor(TableConfig tableConfig) {
        this.tableConfig = tableConfig;
    }

    @Override
    public Map<String, Object> process(Map<String, Object> item) {
        for (ColumnDefinition column : tableConfig.getColumns()) {
            if (!column.isNullable() && item.get(column.getName()) == null) {
                throw new ValidationException("Row from table '" + tableConfig.getTableName()
                        + "' has null value for non-nullable column '" + column.getName() + "': " + item);
            }
        }
        return item;
    }
}
