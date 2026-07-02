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
