package com.archival.reader;

import com.archival.model.ColumnDefinition;
import com.archival.model.TableConfig;
import org.springframework.jdbc.core.RowMapper;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps one JDBC row to a {@code LinkedHashMap<String,Object>} using only the
 * column list declared in a {@link TableConfig}. This is what keeps the
 * reader/processor/writer chain completely table-agnostic: downstream code
 * only ever sees generic maps, never a table-specific POJO.
 */
public class GenericRowMapper implements RowMapper<Map<String, Object>> {

    private final TableConfig tableConfig;

    public GenericRowMapper(TableConfig tableConfig) {
        this.tableConfig = tableConfig;
    }

    @Override
    public Map<String, Object> mapRow(ResultSet rs, int rowNum) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        for (ColumnDefinition column : tableConfig.getColumns()) {
            row.put(column.getName(), extract(rs, column));
        }
        return row;
    }

    private Object extract(ResultSet rs, ColumnDefinition column) throws SQLException {
        String name = column.getName();
        Object value = switch (column.getSqlType()) {
            case VARCHAR, CHAR, TEXT -> rs.getString(name);
            case INTEGER, SMALLINT, TINYINT -> (Integer) rs.getObject(name, Integer.class);
            case BIGINT -> (Long) rs.getObject(name, Long.class);
            case DECIMAL -> rs.getBigDecimal(name);
            case DOUBLE -> (Double) rs.getObject(name, Double.class);
            case FLOAT -> (Float) rs.getObject(name, Float.class);
            case DATE -> rs.getObject(name, LocalDate.class);
            case DATETIME, TIMESTAMP -> rs.getObject(name, LocalDateTime.class);
            case BOOLEAN -> (Boolean) rs.getObject(name, Boolean.class);
            case BLOB -> rs.getBytes(name);
        };
        return rs.wasNull() && !(value instanceof byte[]) ? null : value;
    }
}
