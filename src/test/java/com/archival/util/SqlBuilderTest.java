package com.archival.util;

import com.archival.model.ColumnDefinition;
import com.archival.model.ParquetColumnType;
import com.archival.model.SqlColumnType;
import com.archival.model.TableConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SqlBuilderTest {

    private TableConfig tableConfig;

    @BeforeEach
    void setUp() {
        tableConfig = new TableConfig();
        tableConfig.setTableName("customer_orders");
        tableConfig.setPrimaryKeyColumn("order_id");
        tableConfig.setFyeColumn("fye");
        tableConfig.setColumns(List.of(
                new ColumnDefinition("order_id", SqlColumnType.BIGINT, ParquetColumnType.LONG, false, null, null),
                new ColumnDefinition("fye", SqlColumnType.DATE, ParquetColumnType.DATE, false, null, null),
                new ColumnDefinition("order_amount", SqlColumnType.DECIMAL, ParquetColumnType.DECIMAL, true, 12, 2)
        ));
    }

    @Test
    void selectForExport_includesAllColumnsWindowPredicateAndOrdering() {
        String sql = SqlBuilder.selectForExport(tableConfig);

        assertThat(sql)
                .startsWith("SELECT order_id, fye, order_amount FROM customer_orders")
                .contains("WHERE fye >= ? AND fye < ?")
                .endsWith("ORDER BY order_id ASC");
        assertThat(sql).doesNotContain("AND (");
    }

    @Test
    void selectForExport_appendsAdditionalWhereClauseWhenPresent() {
        tableConfig.setAdditionalWhereClause("status = 'CLOSED'");

        String sql = SqlBuilder.selectForExport(tableConfig);

        assertThat(sql).contains("AND (status = 'CLOSED')");
    }

    @Test
    void countInWindow_usesSameWindowPredicate() {
        String sql = SqlBuilder.countInWindow(tableConfig);

        assertThat(sql).isEqualTo("SELECT COUNT(*) FROM customer_orders WHERE fye >= ? AND fye < ?");
    }

    @Test
    void deleteBatch_usesDerivedTableSoItNeverSelectsDirectlyFromTargetTable() {
        String sql = SqlBuilder.deleteBatch(tableConfig);

        assertThat(sql)
                .startsWith("DELETE FROM customer_orders WHERE order_id IN (")
                .contains("SELECT order_id FROM (")
                .contains("ORDER BY order_id ASC")
                .contains("LIMIT ?")
                .contains(") archival_delete_batch");
    }
}
