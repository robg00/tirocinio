CREATE TABLE IF NOT EXISTS agg_sales_windowed (
    id SERIAL PRIMARY KEY,
    sale_count INTEGER NOT NULL,
    total_revenue DECIMAL(12, 2) NOT NULL,
    avg_order_value DECIMAL(10, 2) NOT NULL,
    window_end TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);