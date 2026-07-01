INSERT INTO dim_date (date_id, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend)
SELECT
    d::DATE AS date_id,
    d::DATE AS full_date,
    EXTRACT(YEAR FROM d)::SMALLINT AS year,
    EXTRACT(QUARTER FROM d)::SMALLINT AS quarter,
    EXTRACT(MONTH FROM d)::SMALLINT AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(DAY FROM d)::SMALLINT AS day,
    EXTRACT(DOW FROM d)::SMALLINT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(DOW FROM d) IN (0, 6) AS is_weekend
FROM GENERATE_SERIES('2025-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) AS d;
