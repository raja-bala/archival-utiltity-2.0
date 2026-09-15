package com.archival.job;

import com.archival.config.ArchivalProperties;
import com.archival.config.TableConfigFactory;
import com.archival.delete.SafeDeleteTasklet;
import com.archival.listener.ArchivalJobListener;
import com.archival.listener.ExportStepListener;
import com.archival.model.PartitionGranularity;
import com.archival.model.TableConfig;
import com.archival.processor.GenericRecordValidatingProcessor;
import com.archival.reader.GenericTableItemReaderFactory;
import com.archival.reconciliation.ParquetReconciliationTasklet;
import com.archival.restore.RestoreTasklet;
import com.archival.tracking.ArchivalAuditService;
import com.archival.writer.ParquetItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.repeat.CompletionPolicy;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Wires the single, generic three-step archival job:
 * <pre>
 *   exportStep (chunk: read MariaDB -&gt; validate -&gt; write Parquet)
 *       -&gt; reconcileStep (tasklet: validate Parquet vs export count vs live DB count)
 *           -&gt; deleteStep (tasklet: delete from MariaDB, gated on reconcileStep having succeeded)
 * </pre>
 * Every bean that needs to know *which* table or *which* window is
 * declared {@code @JobScope}/{@code @StepScope} and reads it from
 * {@link ArchivalRunContext}, which itself is resolved once per job
 * execution from job parameters (see {@link ArchivalApplicationRunner}).
 * Nothing here references a specific table name, column, or application -
 * onboarding a new table requires no code or config changes at all: the
 * table name (and, if it isn't {@code id}, the primary key column) is
 * simply passed on the command line, and every other column is discovered
 * via JDBC metadata by {@link TableConfigFactory}.
 */
@Configuration
public class ArchivalJobConfig {

    // ------------------------------------------------------------------
    // Per-execution context, shared by every step in the run
    // ------------------------------------------------------------------

    @Bean
    @JobScope
    public ArchivalRunContext archivalRunContext(
            TableConfigFactory tableConfigFactory,
            @Value("#{jobParameters['tableName']}") String tableName,
            @Value("#{jobParameters['primaryKeyColumn']}") String primaryKeyColumn,
            @Value("#{jobParameters['additionalWhereClause']}") String additionalWhereClause,
            @Value("#{jobParameters['partitionGranularity']}") String partitionGranularityParam,
            @Value("#{jobParameters['windowStart']}") String windowStartParam,
            @Value("#{jobParameters['windowEnd']}") String windowEndParam,
            @Value("#{jobParameters['dryRun'] ?: 'false'}") String dryRunParam,
            @Value("#{jobParameters['runToken']}") String runToken) {
        PartitionGranularity partitionGranularity = (partitionGranularityParam == null || partitionGranularityParam.isBlank())
                ? null
                : PartitionGranularity.valueOf(partitionGranularityParam);
        return new ArchivalRunContext(
                tableConfigFactory.buildFull(tableName, primaryKeyColumn, additionalWhereClause, partitionGranularity),
                LocalDate.parse(windowStartParam),
                LocalDate.parse(windowEndParam),
                Boolean.parseBoolean(dryRunParam),
                runToken);
    }

    @Bean
    @JobScope
    public ArchivalJobListener archivalJobListener(ArchivalAuditService auditService, ArchivalRunContext ctx) {
        return new ArchivalJobListener(auditService, ctx.tableConfig(), ctx.windowStart(), ctx.windowEnd());
    }

    // ------------------------------------------------------------------
    // Export step: MariaDB -> validate -> Parquet
    // ------------------------------------------------------------------

    @Bean
    @StepScope
    public JdbcCursorItemReader<Map<String, Object>> exportReader(GenericTableItemReaderFactory readerFactory, ArchivalRunContext ctx) {
        return readerFactory.create(ctx.tableConfig(), ctx.windowStart(), ctx.windowEnd());
    }

    @Bean
    @StepScope
    public GenericRecordValidatingProcessor exportProcessor(ArchivalRunContext ctx) {
        return new GenericRecordValidatingProcessor(ctx.tableConfig());
    }

    @Bean
    @StepScope
    public ParquetItemWriter exportWriter(ArchivalRunContext ctx, ArchivalProperties properties) {
        return new ParquetItemWriter(ctx.tableConfig(), properties.getOutputBaseDir(), ctx.windowStart(), ctx.windowEnd(), ctx.runToken());
    }

    @Bean
    @StepScope
    public ExportStepListener exportStepListener(ArchivalAuditService auditService, ParquetItemWriter exportWriter, ArchivalRunContext ctx) {
        return new ExportStepListener(auditService, exportWriter, ctx.tableConfig().getTableName(), ctx.windowStart(), ctx.windowEnd());
    }

    @Bean
    @StepScope
    public CompletionPolicy exportChunkCompletionPolicy(ArchivalRunContext ctx) {
        return new SimpleCompletionPolicy(Math.max(1, ctx.tableConfig().getChunkSize()));
    }

    @Bean
    public Step exportStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                            JdbcCursorItemReader<Map<String, Object>> exportReader,
                            GenericRecordValidatingProcessor exportProcessor,
                            ParquetItemWriter exportWriter,
                            ExportStepListener exportStepListener,
                            CompletionPolicy exportChunkCompletionPolicy) {
        return new StepBuilder("exportStep", jobRepository)
                .<Map<String, Object>, Map<String, Object>>chunk(exportChunkCompletionPolicy, transactionManager)
                .reader(exportReader)
                .processor(exportProcessor)
                .writer(exportWriter)
                .listener(exportStepListener)
                .build();
    }

    // ------------------------------------------------------------------
    // Reconcile step: validate Parquet output against export + live DB counts
    // ------------------------------------------------------------------

    @Bean
    @StepScope
    public ParquetReconciliationTasklet reconciliationTasklet(ArchivalAuditService auditService, JdbcTemplate jdbcTemplate, ArchivalRunContext ctx) {
        return new ParquetReconciliationTasklet(auditService, jdbcTemplate, ctx.tableConfig(), ctx.windowStart(), ctx.windowEnd());
    }

    @Bean
    public Step reconcileStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                               ParquetReconciliationTasklet reconciliationTasklet) {
        return new StepBuilder("reconcileStep", jobRepository)
                .tasklet(reconciliationTasklet, transactionManager)
                .build();
    }

    // ------------------------------------------------------------------
    // Delete step: remove reconciled rows from MariaDB
    // ------------------------------------------------------------------

    @Bean
    @StepScope
    public SafeDeleteTasklet safeDeleteTasklet(ArchivalAuditService auditService, JdbcTemplate jdbcTemplate,
                                                TransactionTemplate archivalTransactionTemplate,
                                                ArchivalRunContext ctx, ArchivalProperties properties) {
        return new SafeDeleteTasklet(auditService, jdbcTemplate, archivalTransactionTemplate,
                ctx.tableConfig(), ctx.windowStart(), ctx.windowEnd(), properties.isDeleteEnabled(), ctx.dryRun());
    }

    @Bean
    public Step deleteStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                            SafeDeleteTasklet safeDeleteTasklet) {
        return new StepBuilder("deleteStep", jobRepository)
                .tasklet(safeDeleteTasklet, transactionManager)
                .build();
    }

    // ------------------------------------------------------------------
    // Job
    // ------------------------------------------------------------------

    @Bean
    public Job archivalJob(JobRepository jobRepository, ArchivalJobListener archivalJobListener,
                            Step exportStep, Step reconcileStep, Step deleteStep) {
        return new JobBuilder("archivalJob", jobRepository)
                .listener(archivalJobListener)
                .start(exportStep)
                .next(reconcileStep)
                .next(deleteStep)
                .build();
    }

    // ------------------------------------------------------------------
    // Restore job: re-insert a previously exported+deleted window's data
    // back into the source table from caller-supplied Parquet file(s).
    //
    // Deliberately a separate, standalone job (not a step tacked onto
    // archivalJob) - it is invoked explicitly, on demand, against Parquet
    // file(s) that were already produced by a real archival export, not as
    // part of that normal flow. It deliberately does NOT reuse the
    // @JobScope ArchivalRunContext bean the archive job uses: that bean
    // requires windowStart/windowEnd job parameters, but restore never
    // takes a window (or any other "path") as input - only the file(s) to
    // restore from - and resolves/validates the window itself, from the
    // audit trail, by file name (see RestoreTasklet). It also has no job
    // listener: a listener modeled on ArchivalJobListener would call
    // auditService.recordStarted(...) in beforeJob(), which resets the
    // audit row's status to STARTED - clobbering the DELETED/RESTORE_FAILED
    // status RestoreTasklet itself needs to see as a precondition. All audit
    // bookkeeping for a restore run is therefore owned entirely by
    // RestoreTasklet.
    // ------------------------------------------------------------------

    @Bean
    @StepScope
    public TableConfig restoreTableConfig(
            TableConfigFactory tableConfigFactory,
            @Value("#{jobParameters['tableName']}") String tableName,
            @Value("#{jobParameters['primaryKeyColumn']}") String primaryKeyColumn,
            @Value("#{jobParameters['additionalWhereClause']}") String additionalWhereClause,
            @Value("#{jobParameters['partitionGranularity']}") String partitionGranularityParam) {
        PartitionGranularity partitionGranularity = (partitionGranularityParam == null || partitionGranularityParam.isBlank())
                ? null
                : PartitionGranularity.valueOf(partitionGranularityParam);
        return tableConfigFactory.buildFull(tableName, primaryKeyColumn, additionalWhereClause, partitionGranularity);
    }

    @Bean
    @StepScope
    public RestoreTasklet restoreTasklet(ArchivalAuditService auditService, JdbcTemplate jdbcTemplate,
                                          TransactionTemplate archivalTransactionTemplate,
                                          TableConfig restoreTableConfig,
                                          @Value("#{jobParameters['restoreFiles']}") String restoreFilesParam) {
        List<String> restoreFiles = (restoreFilesParam == null || restoreFilesParam.isBlank())
                ? List.of()
                : Arrays.stream(restoreFilesParam.split(",")).map(String::trim).toList();
        return new RestoreTasklet(auditService, jdbcTemplate, archivalTransactionTemplate, restoreTableConfig, restoreFiles);
    }

    @Bean
    public Step restoreStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                             RestoreTasklet restoreTasklet) {
        return new StepBuilder("restoreStep", jobRepository)
                .tasklet(restoreTasklet, transactionManager)
                .build();
    }

    @Bean
    public Job archivalRestoreJob(JobRepository jobRepository, Step restoreStep) {
        return new JobBuilder("archivalRestoreJob", jobRepository)
                .start(restoreStep)
                .build();
    }
}
