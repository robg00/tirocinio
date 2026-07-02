-- =====================================================================
-- Configurazione Checkpointing (fault tolerance)
-- =====================================================================
SET 'execution.checkpointing.interval' = '60000';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause' = '30000';
SET 'execution.checkpointing.timeout' = '600000';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'state.backend' = 'hashmap';
SET 'state.checkpoints.dir' = 'file:///tmp/flink-checkpoints';
SET 'state.savepoints.dir' = 'file:///tmp/flink-savepoints';

-- =====================================================================
-- Tabella sorgente Kafka con Watermark
-- =====================================================================
CREATE TABLE sales_events_kafka (
    sale_id         STRING,
    user_id         STRING,
    product_id      INT,
    quantity        INT,
    unit_price      DECIMAL(10, 2),
    total_amount    DECIMAL(12, 2),
    event_timestamp STRING,
    ts AS TO_TIMESTAMP(event_timestamp, 'yyyy-MM-dd HH:mm:ss'),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'sales-events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink_consumer_group',
    'format' = 'json',
    'scan.startup.mode' = 'latest-offset'
);

CREATE TABLE invalid_events_kafka (
    sale_id         STRING,
    user_id         STRING,
    product_id      INT,
    quantity        INT,
    unit_price      DECIMAL(10, 2),
    total_amount    DECIMAL(12, 2),
    event_timestamp STRING,
    error_reason    STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'invalid-sales-events',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink_consumer_group',
    'format' = 'json'
);

CREATE TABLE fact_sale_sink (
    sale_id         STRING,
    user_id         STRING,
    product_id      INT,
    date_id         DATE,
    quantity        INT,
    unit_price      DECIMAL(10, 2),
    total_amount    DECIMAL(12, 2),
    value_band      STRING,
    sale_hour       INT,
    sale_minute     INT,
    day_of_week     STRING,
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
    CASE
        WHEN total_amount < 50 THEN 'low'
        WHEN total_amount < 200 THEN 'medium'
        ELSE 'high'
    END AS value_band,
    CAST(EXTRACT(HOUR FROM ts) AS INT) AS sale_hour,
    CAST(EXTRACT(MINUTE FROM ts) AS INT) AS sale_minute,
    DATE_FORMAT(ts, 'EEEE') AS day_of_week,
    ts AS event_timestamp
FROM sales_events_kafka
WHERE quantity > 0
    AND unit_price > 0
    AND sale_id IS NOT NULL
    AND user_id IS NOT NULL
    AND product_id IS NOT NULL
    AND ts IS NOT NULL;

INSERT INTO invalid_events_kafka
SELECT
    sale_id, user_id, product_id, quantity, unit_price, total_amount,
    event_timestamp,
    CASE
        WHEN quantity <= 0 THEN 'quantity_zero'
        WHEN unit_price <= 0 THEN 'negative_price'
        WHEN sale_id IS NULL OR user_id IS NULL OR product_id IS NULL THEN 'missing_field'
        WHEN ts IS NULL THEN 'corrupted_timestamp'
        ELSE 'unknown'
    END AS error_reason
FROM sales_events_kafka
WHERE NOT (quantity > 0
    AND unit_price > 0
    AND sale_id IS NOT NULL
    AND user_id IS NOT NULL
    AND product_id IS NOT NULL
    AND ts IS NOT NULL);