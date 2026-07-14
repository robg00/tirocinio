DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_user CASCADE;

CREATE TABLE dim_user (
    user_id     VARCHAR(20)  PRIMARY KEY,
    segment     VARCHAR(20)  NOT NULL CHECK (segment IN ('new', 'regular', 'vip'))
);

CREATE TABLE dim_product (
    product_id  INTEGER        PRIMARY KEY,
    name        VARCHAR(100)   NOT NULL,
    category    VARCHAR(50)    NOT NULL CHECK (category IN ('Electronics', 'Home', 'Clothing', 'Sports')),
    brand       VARCHAR(50)    NOT NULL,
    base_price  DECIMAL(10, 2) NOT NULL CHECK (base_price > 0)
);

CREATE TABLE dim_date (
    date_id     DATE        PRIMARY KEY,
    full_date   DATE        NOT NULL,
    year        INTEGER     NOT NULL CHECK (year >= 2000 AND year <= 2100),
    quarter     INTEGER     NOT NULL CHECK (quarter IN (1, 2, 3, 4)),
    month       INTEGER     NOT NULL CHECK (month >= 1 AND month <= 12),
    month_name  VARCHAR(10) NOT NULL,
    day         INTEGER     NOT NULL CHECK (day >= 1 AND day <= 31),
    day_of_week INTEGER     NOT NULL CHECK (day_of_week >= 0 AND day_of_week <= 6),
    day_name    VARCHAR(10) NOT NULL,
    is_weekend  BOOLEAN     NOT NULL
);

CREATE TABLE fact_sales (
    sale_id         VARCHAR(36)    PRIMARY KEY,
    user_id         VARCHAR(20)    NOT NULL REFERENCES dim_user(user_id),
    product_id      INTEGER        NOT NULL REFERENCES dim_product(product_id),
    date_id         DATE           NOT NULL REFERENCES dim_date(date_id),
    quantity        INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price      DECIMAL(10, 2) NOT NULL CHECK (unit_price > 0),
    total_amount    DECIMAL(12, 2) NOT NULL CHECK (total_amount > 0),
    value_band      VARCHAR(10)    NOT NULL CHECK (value_band IN ('low', 'medium', 'high')),
    sale_hour       SMALLINT       NOT NULL CHECK (sale_hour >= 0 AND sale_hour <= 23),
    sale_minute     SMALLINT       NOT NULL CHECK (sale_minute >= 0 AND sale_minute <= 59),
    day_of_week     VARCHAR(10)    NOT NULL,
    event_timestamp TIMESTAMPTZ    NOT NULL
);

CREATE INDEX idx_fact_user        ON fact_sales(user_id);
CREATE INDEX idx_fact_product     ON fact_sales(product_id);
CREATE INDEX idx_fact_date        ON fact_sales(date_id);
CREATE INDEX idx_fact_value_band  ON fact_sales(value_band);
CREATE INDEX idx_fact_day_of_week ON fact_sales(day_of_week);
