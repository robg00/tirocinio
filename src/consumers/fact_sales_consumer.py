import json
import os

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
                CREATE TABLE IF NOT EXISTS fact_sales (
                    sale_id         VARCHAR(36) PRIMARY KEY,
                    user_id         VARCHAR(20)    NOT NULL,
                    product_id      INTEGER        NOT NULL,
                    date_id         DATE           NOT NULL,
                    quantity        INTEGER        NOT NULL,
                    unit_price      DECIMAL(10,2)  NOT NULL,
                    total_amount    DECIMAL(12,2)  NOT NULL,
                    value_band      VARCHAR(10)    NOT NULL,
                    sale_hour       SMALLINT       NOT NULL,
                    sale_minute     SMALLINT       NOT NULL,
                    day_of_week     VARCHAR(10)    NOT NULL,
                    event_timestamp TIMESTAMPTZ    NOT NULL
                )
            """)
        conn.commit()
    finally:
        conn.close()


def main():
    _init_table()
    consumer = KafkaConsumer(
        "valid-sales",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        group_id="fact-sales-consumer",
    )
    for msg in consumer:
        conn = None
        try:
            event = json.loads(msg.value.decode("utf-8"))
            conn = _get_pg_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO fact_sales "
                    "(sale_id, user_id, product_id, date_id, quantity, unit_price, "
                    "total_amount, value_band, sale_hour, sale_minute, day_of_week, event_timestamp) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (sale_id) DO NOTHING",
                    (
                        event["sale_id"], event["user_id"], event["product_id"],
                        event["date_id"], event["quantity"], event["unit_price"],
                        event["total_amount"], event["value_band"],
                        event["sale_hour"], event["sale_minute"],
                        event["day_of_week"], event["event_timestamp"],
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