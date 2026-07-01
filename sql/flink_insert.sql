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
