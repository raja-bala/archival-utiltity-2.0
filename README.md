# archival-utility

A generic, reusable Spring Boot + Spring Batch archival/export framework that:

1. Reads aged records out of MariaDB.
2. Identifies rows whose `fye` date is more than a configurable retention period old (default **5 years**).
3. Processes exactly one **4-month `fye` window** per job execution.
4. Exports the selected rows to **Apache Parquet** files, grouped/split by `fye`.
5. Validates/reconciles the generated Parquet files against the source data — not just row counts, but every column value of every row, matched by primary key.
6. Deletes rows from MariaDB **only** after that window has been successfully exported and reconciled.
7. Supports any number of source tables from any number of applications with **zero code or config-file changes** — a table is onboarded purely by naming it on the command line.
8. Can **restore** a previously exported-and-deleted window back into its source table on demand, from the same Parquet files, with its own durable audit trail (`--operation=restore`; see [Restoring archived data](#restoring-archived-data)).

## Why it's built this way

The framework code (`com.archival.*`) never references a specific table, column, or application, and there is **no per-table config file** to write or maintain. Every table this framework processes is expected to follow two fixed conventions:

- it has a primary key column, which is named `id` unless you say otherwise;
- it has a date/datetime column named `fye`, which is what retention, windowing, and Parquet partitioning are all based on.

Given just the table name (and, if needed, an override for the primary key column), the framework discovers everything else — every column's name, SQL type, nullability, and (for decimals) precision/scale — directly from the database via JDBC metadata (`DatabaseMetaData.getColumns()`, see `com.archival.introspection.SchemaIntrospector`). Onboarding a 6th, 7th, ... table is "point `--tableName` at it", not "add a YAML file" or "write Java".

Five sample tables (representing five different applications) are included in the docker-compose demo:

| Table                   | Sample "owning" app | Primary key       | Partitioning |
|-------------------------|---------------------|--------------------|--------------|
| `customer_orders`       | sales-app           | `order_id`         | monthly      |
| `invoice_records`       | billing-app         | `invoice_id`       | monthly      |
| `shipment_logs`         | logistics-app       | `shipment_id`      | monthly      |
| `audit_events`          | compliance-app      | `event_id`         | yearly       |
| `subscription_history`  | subscriptions-app   | `subscription_id`  | monthly      |

None of these five happen to use the `id` default, so each demo command below passes `--primaryKeyColumn` explicitly. A table whose primary key actually is named `id` needs no such flag at all.

`customer_orders` is the one fully wired into the docker-compose demo below.

## Architecture

```
ArchivalApplicationRunner        (resolves the window, launches the job)
        |
        v
      Spring Batch Job "archivalJob"
        |
        +-- exportStep     (chunk: JDBC cursor read -> validate -> Parquet write)
        |
        +-- reconcileStep  (tasklet: every Parquet row vs. its live DB row, column by column)
        |
        +-- deleteStep     (tasklet: chunked DELETE, gated on reconcileStep having succeeded)

      Spring Batch Job "archivalRestoreJob"  (separate, on-demand - see below)
        |
        +-- restoreStep    (tasklet: read Parquet -> decode -> re-insert into the source table)
```

Every step-scoped bean reads a single `ArchivalRunContext` (table config + resolved window),
built once per job execution from job parameters — see `com.archival.job.ArchivalJobConfig`.

Key building blocks:

- `TableConfig` / `ColumnDefinition` (`com.archival.model`) — the entire in-memory description of a table for one job execution, assembled at runtime (not loaded from a file).
- `SchemaIntrospector` (`com.archival.introspection`) — discovers a table's full column list and types via JDBC `DatabaseMetaData`.
- `TableConfigFactory` (`com.archival.config`) — builds a `TableConfig` from `--tableName` / `--primaryKeyColumn` / `--additionalWhereClause` / `--partitionGranularity` plus `SchemaIntrospector`'s column list and the framework-wide defaults in `application.yml`. This is the only class that replaces what used to be a YAML config loader.
- `DateWindowCalculator` (`com.archival.util`) — computes the `[windowStart, windowEnd)` range: explicit `--windowStart`, else the audit table's watermark, else `MIN(fye)` on first run; `windowEnd = min(windowStart + windowMonths, today - retentionYears)`.
- `SqlBuilder` (`com.archival.util`) — the only place that assembles table-specific SQL text (select/count/chunked-delete), from `TableConfig` metadata.
- `GenericTableItemReaderFactory` / `GenericRowMapper` (`com.archival.reader`) — a `JdbcCursorItemReader<Map<String,Object>>` built purely from a `TableConfig`.
- `ParquetSchemaFactory` / `ParquetRecordConverter` / `ParquetItemWriter` (`com.archival.writer`) — builds an Avro schema at runtime, converts each row, and writes Parquet files grouped by `fye` (see `PartitionGranularity`).
- `com.archival.io.LocalOutputFile` / `LocalInputFile` — Parquet `OutputFile`/`InputFile` implementations that write/read the local filesystem directly, so the framework does **not** need a Hadoop/HDFS runtime.
- `ParquetReconciliationTasklet` (`com.archival.reconciliation`) — decodes every row out of the Parquet output, re-reads the live rows for the same window from the source table, and matches them by primary key, comparing **every column value exactly** (not just counts) via `ParquetRecordConverter.convert`, which encodes a live JDBC value into the same representation a Parquet reader hands back for that column. Only when every row and every column agree exactly is the window marked reconciled.
- `SafeDeleteTasklet` (`com.archival.delete`) — deletes in small batches via a derived-table subquery (portable across MariaDB/MySQL and H2), re-verifying the row count immediately before deleting.
- `RestoreTasklet` (`com.archival.restore`) — the inverse of export+delete: reads the same Parquet part files recorded for a window, decodes every value back into its native Java type via `ParquetRecordConverter.decode` (the exact inverse of `convert`), and re-inserts every row into the source table inside one transaction, only when that window's audit status is `DELETED` (or a previously failed restore, `RESTORE_FAILED`) and the source table currently has zero rows in that window. Runs as its own standalone job (`archivalRestoreJob`), invoked on demand — see [Restoring archived data](#restoring-archived-data).
- `ArchivalAuditService` / `archival_job_audit` table (`com.archival.tracking`) — durable, cross-run tracking keyed by `(tableName, windowStart, windowEnd)`. This is what gates the delete step, provides the watermark for the next run, makes reruns idempotent, and (via `restored_record_count` and the `RESTORE_*` statuses) tracks every restore attempt too.

## Job parameters

```
java -jar archival-utility.jar \
    --tableName=customer_orders \
    [--primaryKeyColumn=order_id] \
    [--additionalWhereClause="status = 'CLOSED'"] \
    [--partitionGranularity=MONTH] \
    [--windowStart=2015-01-01] \
    [--retentionYears=5] \
    [--windowMonths=4] \
    [--dryRun=false]
```

| Parameter | Required | Default | Meaning |
|---|---|---|---|
| `operation` | no | `archive` | `archive` (default, the flow described above) or `restore` (see [Restoring archived data](#restoring-archived-data)). |
| `tableName` | yes | - | The physical MariaDB table to archive from. Must have a `fye` date/datetime column. |
| `primaryKeyColumn` | no | `id` (`archival.default-primary-key-column`) | Only needed when the table's primary key isn't literally named `id`. |
| `additionalWhereClause` | no | none | Extra SQL predicate ANDed into every generated query, e.g. `status = 'CLOSED'`. |
| `partitionGranularity` | no | `MONTH` (`archival.default-partition-granularity`) | How exported rows are grouped into Parquet files: `EXACT_DATE`, `MONTH`, `YEAR`, or `NONE`. |
| `windowStart` | no | audit watermark, else `MIN(fye)` | ISO date to start this run's window from. |
| `retentionYears` | no | `archival.retention-years` (5) | Only data older than this is ever eligible. |
| `windowMonths` | no | `archival.window-months` (4) | Width of the window processed by this execution. |
| `dryRun` | no | `false` | Export + reconcile normally, but skip the physical delete. |

If nothing is eligible yet (the computed window would touch data younger than the retention cutoff), the runner logs that and exits `0` without launching a Spring Batch job at all.

## Restart & failure semantics

- The job's identifying parameters are `tableName` + `windowStart` + `windowEnd`. If a run fails partway (e.g. reconciliation fails), re-running the **same command** resumes that `JobInstance` from the failed step, per normal Spring Batch restart behavior — it does not silently move on to a new window.
- The delete step never runs unless the reconcile step completed successfully in the *same* Spring Batch flow (`.next()` chaining stops on failure), and it independently re-checks the audit table's status and re-counts the source table immediately before deleting, as defense in depth.
- The watermark used for auto-computing the next window only advances past a window once it reaches `DELETED` status, so a failed/skipped delete never causes data to be silently skipped on the next run.
- Parquet part files are written with create-not-overwrite semantics. If an export step is retried after a low-level crash (not a clean Spring Batch failure) that left a partial file behind, clear that file before restarting, or point `--windowStart` at a fresh run — the framework does not attempt in-place resume of a partially written Parquet file.

## Restoring archived data

`--operation=restore` runs a separate, standalone Spring Batch job (`archivalRestoreJob`) that reads Parquet file(s) you name explicitly and re-inserts every row back into its original source table — the inverse of the normal archive flow, for the case where deleted data turns out to be needed again.

Restore deliberately takes **no window and no base path/directory as input**. You name the actual Parquet file(s) you have in hand; the framework validates each one, purely by file name, against what `archival_job_audit.output_files` recorded as that table's real export output, and recovers which window they belong to from that same lookup. A file whose name isn't recognized is refused before anything is read from disk or touched in the database.

```
java -jar archival-utility.jar \
    --operation=restore \
    --tableName=customer_orders \
    --primaryKeyColumn=order_id \
    --parquetFile=/data/archival/output/customer_orders/2015-01/customer_orders_fye-2015-01_run-20260101T090000_token-123_part-000.parquet \
    --parquetFile=/data/archival/output/customer_orders/2015-02/customer_orders_fye-2015-02_run-20260101T090000_token-123_part-000.parquet
```

| Parameter | Required | Meaning |
|---|---|---|
| `operation` | yes | Must be `restore`. |
| `tableName` | yes | Same table that was archived. |
| `primaryKeyColumn` | no (default `id`) | Same value used when that window was archived. |
| `parquetFile` | **yes, repeatable** | One or more Parquet files to restore from. Each is validated by file name against `archival_job_audit.output_files` for this table; a file whose name was never recorded there is refused. All files given in one run must belong to the same archived window — mixing files from two different windows is refused. |
| `additionalWhereClause` / `partitionGranularity` | no | Accepted for consistency but not required for a restore to succeed. |

Because the file is the input, not the window, a file can be restored from wherever it currently lives — e.g. retrieved from cold storage to a brand-new path — not only from the exact path it was originally exported to; only its file name has to match what's on record.

What it does, step by step (`RestoreTasklet`, `com.archival.restore`):

1. For every given `--parquetFile`, looks up `archival_job_audit` for this `tableName` by that file's base name (`ArchivalAuditService.findByOutputFileName`) — matched against every entry in each row's `output_files`, not the full path. If any file's name isn't found, the whole restore is refused immediately, before any database read/write. If the given files resolve to more than one distinct `(windowStart, windowEnd)`, the restore is also refused — one window's files at a time.
2. If that window's status is already `RESTORED`, the run is a no-op (idempotent — safe to re-run the same command).
3. Otherwise, it **refuses** unless the status is `DELETED` (the normal case: fully archived) or `RESTORE_FAILED` (retrying a previous failed attempt) — a window that was never deleted, or whose export/reconciliation never succeeded, cannot be "restored" because there's nothing to restore it from.
4. It **refuses** if the source table already has any rows in that window — restoring on top of existing rows would create duplicates.
5. It reads every given Parquet file, decoding each Avro value back into its original Java type (`ParquetRecordConverter.decode`, the exact inverse of the encoding used on export) — dates from epoch-days, timestamps from epoch-millis (UTC), decimals from their unscaled two's-complement bytes, and so on.
6. It checks the total number of rows read out of the given files matches the window's recorded exported/reconciled count before inserting anything — so restoring only some of a window's files (an incomplete set) is caught and refused, not silently accepted as a partial restore.
7. It re-inserts every row — including its original primary key value — into the source table in a single transaction.
8. It re-counts the source table for the window immediately afterward and verifies that count matches, then marks the audit row `RESTORED` with the restored row count. Any failure along the way marks the row `RESTORE_FAILED` with a reason, leaving it eligible for a retry.

The audit table's `restored_record_count` column, together with the `RESTORE_STARTED` / `RESTORED` / `RESTORE_SKIPPED` / `RESTORE_FAILED` statuses, gives every restore attempt the same durable, queryable trail the export/reconcile/delete side already has — query `archival_job_audit` for a given `table_name` to see the full lifecycle of any window, archived or restored.

Restoring is deliberately a manual, explicit action (there is no automatic "undo the last delete" trigger) — you always name the exact file(s) you want brought back, and they always have to check out against the audit trail first.

## Onboarding a new table

1. Make sure the table has a `fye` date/datetime column (required) and a primary key (any name).
2. Run it:

   ```
   java -jar archival-utility.jar --tableName=my_table --primaryKeyColumn=my_pk_column
   ```

   (omit `--primaryKeyColumn` entirely if the primary key is named `id`.)

No Java changes, no config file, no rebuild.

## Local demo (docker-compose)

```
docker compose up -d
mvn spring-boot:run -Dspring-boot.run.arguments="--tableName=customer_orders --primaryKeyColumn=order_id --windowStart=2015-01-01"
```

Other seeded tables can be run the same way, e.g.:

```
mvn spring-boot:run -Dspring-boot.run.arguments="--tableName=invoice_records --primaryKeyColumn=invoice_id --windowStart=2015-01-01"
mvn spring-boot:run -Dspring-boot.run.arguments="--tableName=audit_events --primaryKeyColumn=event_id --partitionGranularity=YEAR --windowStart=2015-01-01"
```

`docker/init-scripts/01-schema.sql` and `02-seed.sql` create and seed all five sample tables (an ~11-year spread of `fye` dates per table), so repeated runs walk forward through several 4-month windows. Output Parquet files land under `/data/archival/output/<table>/...` by default (override with `ARCHIVAL_OUTPUT_DIR`).

## Running against real MariaDB in production

- Configure `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USER`/`DB_PASSWORD` (see `application.yml`).
- `spring.batch.jdbc.initialize-schema` is set to `always`, so Spring Batch's own `BATCH_*` tables are auto-created on startup against MariaDB (or any other database) - no manual `schema-mysql.sql` import needed. This is safe to leave on permanently: once the tables exist, Boot's batch schema initializer runs with continue-on-error semantics, so the `CREATE TABLE` statements simply no-op (logged at debug level) on every later startup instead of failing it. Set it to `never` instead if you'd rather manage that schema yourself (e.g. via a DBA-controlled migration process). The framework's own `archival_job_audit` table is separately, idempotently self-creating (`db/schema-audit.sql`, applied via `spring.sql.init`, using `CREATE TABLE IF NOT EXISTS`) and also safe to leave on `always`.
- `archival.delete-enabled=false` is a global kill switch that keeps export+reconcile running but skips every physical delete — useful for a first production rollout.
- The database user needs enough visibility for `DatabaseMetaData.getColumns()` to see the target table (i.e. `SELECT`/schema-inspection privileges on it), in addition to `SELECT`/`DELETE`.

## Tests

- `DateWindowCalculatorTest`, `SqlBuilderTest` — pure unit tests of the window/SQL logic.
- `ParquetRoundTripTest` — writes and reads back every supported column type through the real Avro schema factory and the Hadoop-free local `OutputFile`/`InputFile`, with no Spring context and no database.
- `ArchivalJobIntegrationTest` — full `@SpringBootTest` run of the real three-step job against an in-memory H2 database standing in for MariaDB: seeds rows inside and outside the target window, runs `export -> reconcile -> delete`, and asserts only the in-window rows were deleted, Parquet files exist on disk, and the audit table reflects a completed, reconciled, deleted run. This table's primary key is deliberately named `order_id` (not `id`), to exercise the `--primaryKeyColumn` override path end to end. The same class also runs `archivalRestoreJob` end to end, passing the real output file paths recorded by an archive run (never a window) as `--parquetFile`-equivalent job parameters: `archiveThenRestore_bringsBackTheExactSameRows` runs export→reconcile→delete followed by a restore and asserts every row (including original primary keys) comes back exactly; `rerunningRestoreAfterCompletion_isIdempotentAndNeverDuplicatesRows` covers idempotency; `restoreRefusesAFileNotRecordedInTheAuditTrail` and `restoreRefusesFilesThatBelongToDifferentWindows` cover the file-name validation and cross-window refusal added for the file-based restore input model. (The test class also clears Spring Batch's own `BATCH_*` metadata tables in `@BeforeEach`, since the test datasource is one named in-memory H2 instance kept alive for the whole test run — without that, tests that reuse the same identifying job parameters as an earlier test would collide with a leftover completed `JobInstance`.)

Run everything with:

```
mvn clean verify
```

> **Note on this delivery:** this project was written and manually reviewed in an environment without network access to Maven Central, so `mvn clean verify` has not actually been executed here. The code has been checked carefully against the Spring Batch 5.x / Spring Boot 3.5.x / Parquet 1.14.x / Avro 1.11.x APIs it uses, but please run the build yourself (locally or in CI) before treating it as verified.
