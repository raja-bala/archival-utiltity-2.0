-- A minimal, standalone sample table for trying the archival utility out
-- directly, separate from the five multi-app demo tables in
-- 01-schema.sql/02-seed.sql. Its primary key is literally named `id`, so it
-- needs no --primaryKeyColumn override at all - just:
--
--   mvn spring-boot:run -Dspring-boot.run.arguments="--tableName=sample_records --windowStart=2015-01-01"
--
-- (see the bottom of this file for how to load it into an already-running
-- docker-compose MariaDB container, since files here only auto-run against a
-- brand-new, empty data volume.)

CREATE TABLE IF NOT EXISTS sample_records (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id   BIGINT NOT NULL,
    fye           DATE NOT NULL,
    amount        DECIMAL(12,2) NOT NULL,
    status        VARCHAR(20) NOT NULL,
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP,
    INDEX ix_sample_records_fye (fye)
) ENGINE=InnoDB;

-- Populate ~10.5 years of data (2015-01-01 through ~today), one row every 2
-- days, so a run has plenty of rows older than the 5-year retention cutoff
-- to archive, plus recent rows that must be left alone. A little under
-- 2,000 rows.
--
-- fye is normalized to the LAST DAY of its calendar month (e.g. a row whose
-- raw date would be 2015-01-15 gets fye = 2015-01-31 instead), so every row
-- in the same month shares one fye value - a natural fit for the default
-- MONTH partitionGranularity, and a common real-world convention for a
-- "fiscal year end" style column.
--
-- Generated via a classic cross-joined "digits" trick instead of a
-- recursive CTE, so it needs no session variable at all (no
-- cte_max_recursion_depth, no vendor-specific recursion-depth setting) and
-- runs unmodified on MariaDB or plain MySQL, from the CLI or any GUI client.
INSERT INTO sample_records (customer_id, fye, amount, status, is_active, created_at)
SELECT
    100 + (n % 40)                                                AS customer_id,
    LAST_DAY(DATE_ADD('2015-01-01', INTERVAL (n * 2) DAY))        AS fye,
    ROUND(15 + (RAND() * 985), 2)                                  AS amount,
    ELT(1 + (n % 4), 'NEW', 'PROCESSING', 'COMPLETED', 'CANCELLED') AS status,
    (n % 5 <> 0)                                                    AS is_active,
    TIMESTAMP(DATE_ADD('2015-01-01', INTERVAL (n * 2) DAY), '08:00:00') AS created_at
FROM (
    SELECT d1.n + d2.n * 10 + d3.n * 100 + d4.n * 1000 AS n
    FROM (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
          UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d1
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
          UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d2
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4
          UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) d3
    CROSS JOIN (SELECT 0 AS n UNION ALL SELECT 1) d4
) seq
WHERE n <= 1900;

-- ---------------------------------------------------------------------
-- Already loaded an earlier version of this table and just want to fix the
-- fye values in place, without truncating/reloading? Run this on its own:
--
--   UPDATE sample_records SET fye = LAST_DAY(fye);
--
-- (safe to re-run: LAST_DAY() of a date that's already the last day of its
-- month returns that same date unchanged.)
-- ---------------------------------------------------------------------

-- ---------------------------------------------------------------------
-- To load this into a MariaDB container that's already running (instead of
-- recreating the volume so docker-entrypoint-initdb.d picks it up fresh):
--
--   docker compose cp docker/init-scripts/03-sample-records.sql archival-mariadb:/tmp/sample-records.sql
--   docker compose exec mariadb sh -c 'mysql -u root -prootpass archival_demo < /tmp/sample-records.sql'
--
-- Or, from the project root, pipe it in directly:
--
--   docker compose exec -T mariadb mysql -u root -prootpass archival_demo < docker/init-scripts/03-sample-records.sql
--
-- Or paste the whole file into any GUI SQL client (TablePlus, DBeaver,
-- Sequel Ace, MySQL Workbench, etc.) connected to host localhost:3306,
-- database archival_demo, user archival / password archival (or root /
-- rootpass) - this version has no session-variable dependency, so it works
-- there unmodified too.
-- ---------------------------------------------------------------------
