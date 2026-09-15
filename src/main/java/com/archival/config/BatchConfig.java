package com.archival.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Deliberately thin: Spring Boot's own Spring Batch auto-configuration
 * supplies the {@code JobRepository}, {@code JobLauncher} and
 * {@code PlatformTransactionManager} beans (backed by whatever
 * {@code DataSource} is configured), so this class does not redeclare
 * {@code @EnableBatchProcessing} - doing so would switch off that
 * auto-configuration and require wiring all of it back by hand.
 * <p>
 * {@code spring.batch.job.enabled=false} (see application.yml) stops Boot
 * from auto-running every {@code Job} bean on startup, because
 * {@link com.archival.job.ArchivalApplicationRunner} launches the job itself
 * with a window computed at runtime.
 */
@Configuration
public class BatchConfig {

    @Bean
    public TransactionTemplate archivalTransactionTemplate(PlatformTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }
}
