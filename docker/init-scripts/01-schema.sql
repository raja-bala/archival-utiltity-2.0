-- Demo schema for the archival-utility docker-compose environment.
-- Five tables representing five different source applications. There is no
-- table-config file for any of them - each is archived purely by passing
-- --tableName=<this table> (and --primaryKeyColumn=<its PK column>, since
-- none of these use the "id" default) on the command line; every other
-- column is discovered automatically via JDBC metadata at runtime.

CREATE TABLE IF NOT EXISTS customer_orders (
    order_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id   BIGINT NOT NULL,
    fye           DATE NOT NULL,
    order_amount  DECIMAL(12,2) NOT NULL,
    status        VARCHAR(20),
    created_at    TIMESTAMP,
    INDEX ix_customer_orders_fye (fye)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS invoice_records (
    invoice_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id     BIGINT NOT NULL,
    fye            DATE NOT NULL,
    invoice_total  DECIMAL(14,2) NOT NULL,
    currency       VARCHAR(3) NOT NULL,
    paid           BOOLEAN DEFAULT TRUE,
    created_at     TIMESTAMP,
    INDEX ix_invoice_records_fye (fye)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS shipment_logs (
    shipment_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
    warehouse_code  VARCHAR(10) NOT NULL,
    fye             DATE NOT NULL,
    weight_kg       DOUBLE,
    carrier         VARCHAR(50),
    delivered_at    TIMESTAMP,
    INDEX ix_shipment_logs_fye (fye)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS audit_events (
    event_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    actor_id      BIGINT,
    fye           DATE NOT NULL,
    event_type    VARCHAR(50) NOT NULL,
    payload       TEXT,
    recorded_at   TIMESTAMP,
    INDEX ix_audit_events_fye (fye)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS subscription_history (
    subscription_id  BIGINT AUTO_INCREMENT PRIMARY KEY,
    customer_id      BIGINT NOT NULL,
    fye              DATE NOT NULL,
    plan_code        VARCHAR(20) NOT NULL,
    monthly_fee      DECIMAL(10,2),
    active           BOOLEAN DEFAULT FALSE,
    updated_at       TIMESTAMP,
    INDEX ix_subscription_history_fye (fye)
) ENGINE=InnoDB;
