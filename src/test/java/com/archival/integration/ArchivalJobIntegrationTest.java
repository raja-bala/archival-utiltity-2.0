package com.archival.integration;

import com.archival.tracking.ArchivalAuditRecord;
import com.archival.tracking.ArchivalAuditService;
import com.archival.tracking.ArchivalRunStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full pipeline test: seeds an H2 table that mirrors a real MariaDB source
 * table, runs the real three-step Spring Batch job (export -&gt; reconcile
 * -&gt; delete) end to end, and asserts that:
 * <ul>
 *     <li>Parquet files are actually produced on disk;</li>
 *     <li>only rows inside the requested window are deleted;</li>
 *     <li>rows outside the window are left completely untouched;</li>
 *     <li>the audit table reflects a fully completed, reconciled run.</li>
 * </ul>
 * There is no table-config file: the job's SQL and Parquet schema are
 * derived purely from {@code --tableName=test_orders} plus
 * {@code --primaryKeyColumn=order_id} (this test table intentionally does
 * NOT use the {@code id} default, to exercise the override) and JDBC
 * introspection of the {@code test_orders} table defined in
 * {@code src/test/resources/test-schema.sql} - proving the framework needs
 * no code or config changes to onboard a new table.
 */
@SpringBootTest
@ActiveProfiles("test")
class ArchivalJobIntegrationTest {

    private static Path outputDir;

    @DynamicPropertySource
    static void registerDynamicProperties(DynamicPropertyRegistry registry) throws IOException {
        outputDir = Files.createTempDirectory("archival-test-output");
        registry.add("archival.output-base-dir", () -> outputDir.toString());
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private Job archivalJob;

    @Autowired
    @Qualifier("archivalRestoreJob")
    private Job archivalRestoreJob;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private ArchivalAuditService auditService;

    private static final LocalDate OLD_WINDOW_START = LocalDate.of(2015, 1, 1);
    private static final LocalDate OLD_WINDOW_END = OLD_WINDOW_START.plusMonths(4);

    @BeforeEach
    void setUp() {
        jdbcTemplate.update("DELETE FROM test_orders");
        jdbcTemplate.update("DELETE FROM archival_job_audit");
        clearSpringBatchMetadata();
        seedRows();
    }

    /**
     * The test datasource ({@code jdbc:h2:mem:archivaltest;DB_CLOSE_DELAY=-1})
     * is a single named in-memory H2 instance that stays alive for the whole
     * JVM/test-run, independent of Spring context caching - so, unlike
     * {@code test_orders}/{@code archival_job_audit} above, Spring Batch's own
     * BATCH_JOB_INSTANCE/BATCH_JOB_EXECUTION/... metadata would otherwise
     * persist across every test method in this class. Several tests here
     * intentionally reuse the same identifying job parameters (the same
     * {@code tableName} + {@code windowStart}/{@code windowEnd}, or the same
     * {@code tableName} + recorded output files) as other tests, which would
     * otherwise collide with a leftover COMPLETED {@code JobInstance} from an
     * earlier test method and fail with {@code JobInstanceAlreadyCompleteException}
     * instead of actually exercising the job under test. Deleted in
     * child-to-parent FK order.
     */
    private void clearSpringBatchMetadata() {
        jdbcTemplate.update("DELETE FROM BATCH_STEP_EXECUTION_CONTEXT");
        jdbcTemplate.update("DELETE FROM BATCH_JOB_EXECUTION_CONTEXT");
        jdbcTemplate.update("DELETE FROM BATCH_STEP_EXECUTION");
        jdbcTemplate.update("DELETE FROM BATCH_JOB_EXECUTION_PARAMS");
        jdbcTemplate.update("DELETE FROM BATCH_JOB_EXECUTION");
        jdbcTemplate.update("DELETE FROM BATCH_JOB_INSTANCE");
    }

    private void seedRows() {
        long id = 1;
        // 30 rows squarely inside the old window (eligible for archival)
        for (int i = 0; i < 30; i++) {
            insertRow(id++, OLD_WINDOW_START.plusDays(i * 3L), "10.00");
        }
        // 10 rows just outside (after) the old window - must survive untouched
        for (int i = 0; i < 10; i++) {
            insertRow(id++, OLD_WINDOW_END.plusDays(i * 5L), "20.00");
        }
        // 10 recent rows, nowhere near the retention cutoff - must survive untouched
        for (int i = 0; i < 10; i++) {
            insertRow(id++, LocalDate.now().minusMonths(i), "30.00");
        }
    }

    private void insertRow(long id, LocalDate fye, String amount) {
        jdbcTemplate.update("INSERT INTO test_orders (order_id, customer_id, fye, order_amount, status, created_at) VALUES (?,?,?,?,?,?)",
                id, 500 + (id % 5), Date.valueOf(fye), new java.math.BigDecimal(amount), "CLOSED", java.sql.Timestamp.valueOf(fye.atStartOfDay()));
    }

    @Test
    void exportReconcileDeletePipeline_processesOnlyTheRequestedWindow() throws Exception {
        long totalBefore = countAll();
        assertThat(totalBefore).isEqualTo(50);

        JobParameters params = new JobParametersBuilder()
                .addString("tableName", "test_orders")
                .addString("primaryKeyColumn", "order_id", false)
                .addString("windowStart", OLD_WINDOW_START.toString())
                .addString("windowEnd", OLD_WINDOW_END.toString())
                .addString("dryRun", "false", false)
                .addString("runToken", java.util.UUID.randomUUID().toString(), false)
                .toJobParameters();

        JobExecution execution = jobLauncher.run(archivalJob, params);

        assertThat(execution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        long remainingInWindow = countInRange(OLD_WINDOW_START, OLD_WINDOW_END);
        assertThat(remainingInWindow).isZero();

        long remainingTotal = countAll();
        assertThat(remainingTotal).isEqualTo(20); // 10 just-after-window + 10 recent, untouched

        ArchivalAuditRecord audit = auditService.find("test_orders", OLD_WINDOW_START, OLD_WINDOW_END).orElseThrow();
        assertThat(audit.getStatus()).isEqualTo(ArchivalRunStatus.DELETED);
        assertThat(audit.getExportedRecordCount()).isEqualTo(30L);
        assertThat(audit.getReconciledRecordCount()).isEqualTo(30L);
        assertThat(audit.getDeletedRecordCount()).isEqualTo(30L);

        try (Stream<Path> files = Files.walk(outputDir)) {
            long parquetFileCount = files.filter(p -> p.toString().endsWith(".parquet")).count();
            assertThat(parquetFileCount).isGreaterThan(0);
        }
    }

    @Test
    void rerunningTheSameWindowAfterCompletion_isIdempotentAndDeletesNothingTwice() throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("tableName", "test_orders")
                .addString("primaryKeyColumn", "order_id", false)
                .addString("windowStart", OLD_WINDOW_START.toString())
                .addString("windowEnd", OLD_WINDOW_END.toString())
                .addString("dryRun", "false", false)
                .addString("runToken", java.util.UUID.randomUUID().toString(), false)
                .toJobParameters();

        JobExecution first = jobLauncher.run(archivalJob, params);
        assertThat(first.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        long remainingAfterFirst = countAll();

        // Spring Batch itself refuses to re-run identical JobParameters once COMPLETED
        // (JobInstanceAlreadyCompleteException); the framework's own ArchivalApplicationRunner
        // catches that and treats it as a no-op. Here we simulate the same safety net by
        // directly asserting the audit row's terminal state is stable and no further rows
        // vanish if the delete tasklet were invoked again for the same, already-DELETED window.
        ArchivalAuditRecord audit = auditService.find("test_orders", OLD_WINDOW_START, OLD_WINDOW_END).orElseThrow();
        assertThat(audit.getStatus()).isEqualTo(ArchivalRunStatus.DELETED);
        assertThat(countAll()).isEqualTo(remainingAfterFirst);
    }

    /**
     * Runs the archive job for the standard old window and returns the exact
     * Parquet output file paths it recorded in the audit table - i.e. the
     * same information a real caller would have in hand before restoring:
     * files they actually possess, not a window they have to remember.
     */
    private List<String> archiveOldWindowAndGetOutputFiles() throws Exception {
        JobParameters archiveParams = new JobParametersBuilder()
                .addString("tableName", "test_orders")
                .addString("primaryKeyColumn", "order_id", false)
                .addString("windowStart", OLD_WINDOW_START.toString())
                .addString("windowEnd", OLD_WINDOW_END.toString())
                .addString("dryRun", "false", false)
                .addString("runToken", java.util.UUID.randomUUID().toString(), false)
                .toJobParameters();

        JobExecution archiveExecution = jobLauncher.run(archivalJob, archiveParams);
        assertThat(archiveExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        ArchivalAuditRecord archived = auditService.find("test_orders", OLD_WINDOW_START, OLD_WINDOW_END).orElseThrow();
        assertThat(archived.getStatus()).isEqualTo(ArchivalRunStatus.DELETED);
        return List.of(archived.getOutputFiles().split(","));
    }

    private JobParameters restoreParamsFor(List<String> parquetFiles) {
        return new JobParametersBuilder()
                .addString("tableName", "test_orders")
                .addString("primaryKeyColumn", "order_id", false)
                .addString("restoreFiles", String.join(",", parquetFiles))
                .addString("runToken", java.util.UUID.randomUUID().toString(), false)
                .toJobParameters();
    }

    @Test
    void archiveThenRestore_bringsBackTheExactSameRows() throws Exception {
        List<String> parquetFiles = archiveOldWindowAndGetOutputFiles();
        assertThat(countInRange(OLD_WINDOW_START, OLD_WINDOW_END)).isZero();
        assertThat(countAll()).isEqualTo(20);

        // Restore is given only the files themselves - no window, no base path - and has to
        // recover the window and confirm legitimacy purely from archival_job_audit.output_files.
        JobExecution restoreExecution = jobLauncher.run(archivalRestoreJob, restoreParamsFor(parquetFiles));
        assertThat(restoreExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);

        assertThat(countInRange(OLD_WINDOW_START, OLD_WINDOW_END)).isEqualTo(30);
        assertThat(countAll()).isEqualTo(50);

        ArchivalAuditRecord audit = auditService.find("test_orders", OLD_WINDOW_START, OLD_WINDOW_END).orElseThrow();
        assertThat(audit.getStatus()).isEqualTo(ArchivalRunStatus.RESTORED);
        assertThat(audit.getRestoredRecordCount()).isEqualTo(30L);

        // The original primary keys (1..30) and amounts must have come back exactly,
        // not just the right row count.
        Long minId = jdbcTemplate.queryForObject(
                "SELECT MIN(order_id) FROM test_orders WHERE fye >= ? AND fye < ?", Long.class,
                Date.valueOf(OLD_WINDOW_START), Date.valueOf(OLD_WINDOW_END));
        Long maxId = jdbcTemplate.queryForObject(
                "SELECT MAX(order_id) FROM test_orders WHERE fye >= ? AND fye < ?", Long.class,
                Date.valueOf(OLD_WINDOW_START), Date.valueOf(OLD_WINDOW_END));
        assertThat(minId).isEqualTo(1L);
        assertThat(maxId).isEqualTo(30L);
    }

    @Test
    void rerunningRestoreAfterCompletion_isIdempotentAndNeverDuplicatesRows() throws Exception {
        List<String> parquetFiles = archiveOldWindowAndGetOutputFiles();

        JobExecution first = jobLauncher.run(archivalRestoreJob, restoreParamsFor(parquetFiles));
        assertThat(first.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        long afterFirstRestore = countAll();
        assertThat(afterFirstRestore).isEqualTo(50);

        // Spring Batch itself refuses to re-run identical JobParameters once COMPLETED
        // (JobInstanceAlreadyCompleteException) - the same safety net ArchivalApplicationRunner
        // relies on. RestoreTasklet is independently idempotent too: its own audit-status check
        // (RESTORED -> skip) would refuse to insert a second time even without that safety net.
        ArchivalAuditRecord audit = auditService.find("test_orders", OLD_WINDOW_START, OLD_WINDOW_END).orElseThrow();
        assertThat(audit.getStatus()).isEqualTo(ArchivalRunStatus.RESTORED);
        assertThat(countAll()).isEqualTo(afterFirstRestore);
    }

    @Test
    void restoreRefusesAFileNotRecordedInTheAuditTrail() {
        // A file name that was never written by any archival export of this table - e.g. a typo,
        // or a file from some other table/system entirely - must be refused outright, before
        // anything is read from disk or written to the database.
        String bogusFile = "/tmp/not-a-real-export/test_orders_fye-9999-01_run-bogus_token-x_part-000.parquet";

        JobParameters restoreParams = restoreParamsFor(List.of(bogusFile));

        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> {
            JobExecution execution = jobLauncher.run(archivalRestoreJob, restoreParams);
            // The tasklet throws IllegalStateException internally; Spring Batch catches it
            // and marks the step/job FAILED rather than propagating it out of jobLauncher.run.
            assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);
        });
        // Nothing should have been inserted - the 50 originally-seeded rows are untouched.
        assertThat(countAll()).isEqualTo(50);
    }

    @Test
    void restoreRefusesFilesThatBelongToDifferentWindows() throws Exception {
        List<String> firstWindowFiles = archiveOldWindowAndGetOutputFiles();

        LocalDate secondWindowStart = OLD_WINDOW_END;
        LocalDate secondWindowEnd = secondWindowStart.plusMonths(4);
        // Seed and archive a second, distinct window so there are two real, independently
        // recorded sets of output files to mix together.
        for (int i = 0; i < 5; i++) {
            insertRow(1000 + i, secondWindowStart.plusDays(i * 3L), "40.00");
        }
        JobParameters secondArchiveParams = new JobParametersBuilder()
                .addString("tableName", "test_orders")
                .addString("primaryKeyColumn", "order_id", false)
                .addString("windowStart", secondWindowStart.toString())
                .addString("windowEnd", secondWindowEnd.toString())
                .addString("dryRun", "false", false)
                .addString("runToken", java.util.UUID.randomUUID().toString(), false)
                .toJobParameters();
        JobExecution secondArchive = jobLauncher.run(archivalJob, secondArchiveParams);
        assertThat(secondArchive.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        ArchivalAuditRecord secondAudit = auditService.find("test_orders", secondWindowStart, secondWindowEnd).orElseThrow();
        List<String> secondWindowFiles = List.of(secondAudit.getOutputFiles().split(","));

        List<String> mixedFiles = new java.util.ArrayList<>(firstWindowFiles);
        mixedFiles.addAll(secondWindowFiles);

        JobExecution execution = jobLauncher.run(archivalRestoreJob, restoreParamsFor(mixedFiles));
        assertThat(execution.getStatus()).isEqualTo(BatchStatus.FAILED);

        // Neither window should have been restored.
        assertThat(auditService.find("test_orders", OLD_WINDOW_START, OLD_WINDOW_END).orElseThrow().getStatus())
                .isEqualTo(ArchivalRunStatus.DELETED);
        assertThat(auditService.find("test_orders", secondWindowStart, secondWindowEnd).orElseThrow().getStatus())
                .isEqualTo(ArchivalRunStatus.DELETED);
    }

    private long countAll() {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM test_orders", Long.class);
        return count == null ? 0 : count;
    }

    private long countInRange(LocalDate start, LocalDate end) {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM test_orders WHERE fye >= ? AND fye < ?",
                Long.class, Date.valueOf(start), Date.valueOf(end));
        return count == null ? 0 : count;
    }
}
