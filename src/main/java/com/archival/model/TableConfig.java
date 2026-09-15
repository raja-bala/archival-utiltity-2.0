package com.archival.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * Full, self-contained description of one archivable source table for a
 * single job execution.
 * <p>
 * There is no per-table config file. A {@code TableConfig} is assembled at
 * runtime by {@code com.archival.config.TableConfigFactory} from:
 * <ul>
 *     <li>command-line arguments - {@code --tableName} (required),
 *         {@code --primaryKeyColumn} (default {@code id}),
 *         {@code --additionalWhereClause}, {@code --partitionGranularity};</li>
 *     <li>the fixed {@code fye} column convention every source table is
 *         expected to follow ({@link #FYE_COLUMN});</li>
 *     <li>the table's actual column list and types, discovered via JDBC
 *         metadata by {@code com.archival.introspection.SchemaIntrospector}.</li>
 * </ul>
 * The generic reader, processor, Parquet writer, reconciliation and delete
 * steps are all driven purely by the contents of this object - the
 * framework has zero compiled-in knowledge of any specific table, column or
 * application, and onboarding a new table requires no code or config
 * changes at all: just point {@code --tableName} at it.
 */
@Data
@NoArgsConstructor
public class TableConfig {

    /** Fixed convention: every source table this framework processes has a date/datetime column with this name. */
    public static final String FYE_COLUMN = "fye";

    /** Physical table name in MariaDB. Also used as the audit table's identifying key. */
    private String tableName;

    /** Primary key column, used for ordering, chunked deletes and reconciliation range checks. Defaults to "id". */
    private String primaryKeyColumn;

    /** The date/datetime column this framework filters and partitions on - always {@link #FYE_COLUMN}. */
    private String fyeColumn = FYE_COLUMN;

    /** Columns to select/export, in table order, discovered via JDBC metadata. Includes every column in the table. */
    private List<ColumnDefinition> columns = new ArrayList<>();

    /** Optional extra SQL predicate ANDed into every generated query, e.g. "status = 'CLOSED'". */
    private String additionalWhereClause;

    /** JDBC fetch size for the streaming cursor reader. */
    private int fetchSize = 1000;

    /** Spring Batch chunk size for the export step. */
    private int chunkSize = 500;

    /** Row batch size used when issuing chunked DELETE statements. */
    private int deleteBatchSize = 500;

    /** Base output directory Parquet files are written under (a per-table subfolder is added automatically). */
    private String outputBaseDir;

    /** How exported rows are grouped into separate Parquet files. */
    private PartitionGranularity partitionGranularity = PartitionGranularity.MONTH;

    /** Target rows per Parquet file before rolling over to a new part file within the same partition (0 = unlimited). */
    private long maxRowsPerFile = 0;

    public List<String> columnNames() {
        return columns.stream().map(ColumnDefinition::getName).toList();
    }

    public ColumnDefinition columnByName(String name) {
        return columns.stream()
                .filter(c -> c.getName().equalsIgnoreCase(name))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "Column '" + name + "' referenced but not found on table '" + tableName + "'"));
    }
}
