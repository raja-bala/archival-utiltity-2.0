package com.archival.reader;

import com.archival.model.TableConfig;
import com.archival.util.SqlBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Date;
import java.time.LocalDate;
import java.util.Map;

/**
 * Builds a {@link JdbcCursorItemReader} for any table, driven entirely by a
 * {@link TableConfig} and a resolved [windowStart, windowEnd) range. No
 * table-specific Java type is ever referenced - rows come out as
 * {@code Map<String,Object>}.
 */
@Component
public class GenericTableItemReaderFactory {

    private final DataSource dataSource;

    public GenericTableItemReaderFactory(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public JdbcCursorItemReader<Map<String, Object>> create(TableConfig tableConfig, LocalDate windowStart, LocalDate windowEnd) {
        JdbcCursorItemReader<Map<String, Object>> reader = new JdbcCursorItemReader<>();
        reader.setName("archivalReader_" + tableConfig.getTableName());
        reader.setDataSource(dataSource);
        reader.setSql(SqlBuilder.selectForExport(tableConfig));
        reader.setFetchSize(tableConfig.getFetchSize());
        reader.setVerifyCursorPosition(false);
        reader.setPreparedStatementSetter(new ArgumentPreparedStatementSetter(
                new Object[]{Date.valueOf(windowStart), Date.valueOf(windowEnd)}));
        reader.setRowMapper(new GenericRowMapper(tableConfig));
        return reader;
    }
}
