-- Seed data spanning ~11.5 years so there is plenty of data both older and
-- younger than the default 5-year retention cutoff, letting the demo job
-- process several distinct 4-month windows across repeated runs.

SET SESSION cte_max_recursion_depth = 10000;

-- customer_orders: one row per day from 2015-01-01 to ~today (fully wired demo table)
INSERT INTO customer_orders (customer_id, fye, order_amount, status, created_at)
WITH RECURSIVE seq(n) AS (
    SELECT 0
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 4290
)
SELECT
    1000 + (n % 250)                                            AS customer_id,
    DATE_ADD('2015-01-01', INTERVAL n DAY)                      AS fye,
    ROUND(20 + (RAND() * 480), 2)                                AS order_amount,
    ELT(1 + (n % 4), 'NEW', 'SHIPPED', 'DELIVERED', 'CANCELLED') AS status,
    TIMESTAMP(DATE_ADD('2015-01-01', INTERVAL n DAY), '09:30:00') AS created_at
FROM seq;

-- invoice_records: one row every 2 days from 2016-01-01
SET SESSION cte_max_recursion_depth = 10000;
INSERT INTO invoice_records (account_id, fye, invoice_total, currency, paid, created_at)
WITH RECURSIVE seq(n) AS (
    SELECT 0
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 1900
)
SELECT
    2000 + (n % 120)                                  AS account_id,
    DATE_ADD('2016-01-01', INTERVAL (n * 2) DAY)       AS fye,
    ROUND(50 + (RAND() * 950), 2)                       AS invoice_total,
    ELT(1 + (n % 3), 'USD', 'EUR', 'SGD')               AS currency,
    TRUE                                                 AS paid,
    TIMESTAMP(DATE_ADD('2016-01-01', INTERVAL (n * 2) DAY), '14:00:00') AS created_at
FROM seq;

-- shipment_logs: one row every 3 days from 2017-01-01
SET SESSION cte_max_recursion_depth = 10000;
INSERT INTO shipment_logs (warehouse_code, fye, weight_kg, carrier, delivered_at)
WITH RECURSIVE seq(n) AS (
    SELECT 0
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 1200
)
SELECT
    ELT(1 + (n % 5), 'WH-SGP', 'WH-KUL', 'WH-BKK', 'WH-MNL', 'WH-HAN') AS warehouse_code,
    DATE_ADD('2017-01-01', INTERVAL (n * 3) DAY)                       AS fye,
    ROUND(0.5 + (RAND() * 40), 2)                                       AS weight_kg,
    ELT(1 + (n % 3), 'DHL', 'FedEx', 'Local Courier')                   AS carrier,
    TIMESTAMP(DATE_ADD('2017-01-01', INTERVAL (n * 3) DAY), '18:00:00') AS delivered_at
FROM seq;

-- audit_events: one row every 5 days from 2014-06-01
SET SESSION cte_max_recursion_depth = 10000;
INSERT INTO audit_events (actor_id, fye, event_type, payload, recorded_at)
WITH RECURSIVE seq(n) AS (
    SELECT 0
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 900
)
SELECT
    3000 + (n % 60)                                              AS actor_id,
    DATE_ADD('2014-06-01', INTERVAL (n * 5) DAY)                 AS fye,
    ELT(1 + (n % 4), 'LOGIN', 'PERMISSION_CHANGE', 'DATA_EXPORT', 'CONFIG_UPDATE') AS event_type,
    JSON_OBJECT('note', CONCAT('synthetic-event-', n))            AS payload,
    TIMESTAMP(DATE_ADD('2014-06-01', INTERVAL (n * 5) DAY), '00:00:00') AS recorded_at
FROM seq;

-- subscription_history: one row every 4 days from 2016-06-01, all inactive (eligible under additionalWhereClause)
SET SESSION cte_max_recursion_depth = 10000;
INSERT INTO subscription_history (customer_id, fye, plan_code, monthly_fee, active, updated_at)
WITH RECURSIVE seq(n) AS (
    SELECT 0
    UNION ALL
    SELECT n + 1 FROM seq WHERE n < 950
)
SELECT
    1000 + (n % 250)                                   AS customer_id,
    DATE_ADD('2016-06-01', INTERVAL (n * 4) DAY)        AS fye,
    ELT(1 + (n % 3), 'BASIC', 'PRO', 'ENTERPRISE')       AS plan_code,
    ROUND(9.99 + (n % 3) * 20, 2)                        AS monthly_fee,
    FALSE                                                  AS active,
    TIMESTAMP(DATE_ADD('2016-06-01', INTERVAL (n * 4) DAY), '00:00:00') AS updated_at
FROM seq;
