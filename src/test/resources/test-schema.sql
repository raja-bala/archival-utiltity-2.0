CREATE TABLE IF NOT EXISTS test_orders (
    order_id      BIGINT PRIMARY KEY,
    customer_id   BIGINT NOT NULL,
    fye           DATE NOT NULL,
    order_amount  DECIMAL(12,2) NOT NULL,
    status        VARCHAR(20),
    created_at    TIMESTAMP
);
