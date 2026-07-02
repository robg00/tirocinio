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
