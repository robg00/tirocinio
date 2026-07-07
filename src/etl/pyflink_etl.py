from pyflink.table import EnvironmentSettings, TableEnvironment


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.execute_sql("""
        CREATE TABLE sales_events_kafka (
            sale_id         STRING,
            user_id         STRING,
            product_id      INT,
            quantity        INT,
            unit_price      DECIMAL(10, 2),
            total_amount    DECIMAL(12, 2),
            event_timestamp STRING,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sales-events',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'pyflink-table-group',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE agg_sales_windowed_sink (
            sale_count      INT,
            total_revenue   DECIMAL(12, 2),
            avg_order_value DECIMAL(10, 2),
            window_end      TIMESTAMP(3)
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/streammark',
            'table-name' = 'agg_sales_windowed',
            'username' = 'streammark',
            'password' = 'streammark'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO agg_sales_windowed_sink
        SELECT
            CAST(COUNT(*) AS INT) AS sale_count,
            CAST(SUM(total_amount) AS DECIMAL(12, 2)) AS total_revenue,
            CAST(AVG(total_amount) AS DECIMAL(10, 2)) AS avg_order_value,
            TUMBLE_END(proc_time, INTERVAL '30' SECOND) AS window_end
        FROM sales_events_kafka
        WHERE total_amount IS NOT NULL AND quantity > 0
        GROUP BY TUMBLE(proc_time, INTERVAL '30' SECOND)
    """).wait()


if __name__ == "__main__":
    main()
