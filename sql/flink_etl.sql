CREATE TABLE sales_events_kafka (
    sale_id         STRING,
    user_id         STRING,
    product_id      INT,
    quantity        INT,
    unit_price      DECIMAL(10, 2),
    total_amount    DECIMAL(12, 2),
    event_timestamp STRING,
    ts AS TO_TIMESTAMP(event_timestamp, 'yyyy-MM-dd HH:mm:ss')
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales-events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink_consumer_group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE fact_sale_sink (
    sale_id         STRING,
    user_id         STRING,
    product_id      INT,
    date_id         DATE,
    quantity        INT,
    unit_price      DECIMAL(10, 2),
    total_amount    DECIMAL(12, 2),
    event_timestamp TIMESTAMP(3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/streammark',
    'table-name' = 'fact_sales',
    'username' = 'streammark',
    'password' = 'streammark'
);

INSERT INTO fact_sale_sink
SELECT
    sale_id, user_id, product_id,
    CAST(ts AS DATE) AS date_id,
    quantity, unit_price, total_amount,
    ts AS event_timestamp
FROM sales_events_kafka
WHERE quantity > 0
    AND unit_price > 0
    AND sale_id IS NOT NULL
    AND user_id IS NOT NULL
    AND product_id IS NOT NULL
    AND ts IS NOT NULL;