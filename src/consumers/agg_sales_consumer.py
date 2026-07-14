import json
import os
from datetime import datetime, timezone

from kafka import KafkaConsumer
import psycopg2

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
PG_HOST = os.environ.get("PG_HOST", "localhost")
PG_PORT = int(os.environ.get("PG_PORT", "5433"))
PG_DB = os.environ.get("PG_DB", "streammark")
PG_USER = os.environ.get("PG_USER", "streammark")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "streammark")


def _get_pg_connection():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def _init_table():
    conn = _get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS agg_sales_windowed (
                    id SERIAL PRIMARY KEY,
                    sale_count INTEGER NOT NULL,
                    total_revenue DECIMAL(12,2) NOT NULL,
                    avg_order_value DECIMAL(10,2) NOT NULL,
                    window_end TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
        conn.commit()
    finally:
        conn.close()


def main():
    _init_table()
    consumer = KafkaConsumer(
        "agg-sales-windowed",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        group_id="agg-sales-consumer",
    )
    for msg in consumer:
        conn = None
        try:
            event = json.loads(msg.value.decode("utf-8"))
            conn = _get_pg_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO agg_sales_windowed "
                    "(sale_count, total_revenue, avg_order_value, window_end) "
                    "VALUES (%s, %s, %s, %s)",
                    (
                        event["sale_count"], event["total_revenue"],
                        event["avg_order_value"],
                        datetime.now(timezone.utc),
                    ),
                )
            conn.commit()
        except Exception as e:
            print(f"Error: {e}", flush=True)
        finally:
            if conn is not None:
                conn.close()


if __name__ == "__main__":
    main()