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
                CREATE TABLE IF NOT EXISTS anomaly_alerts (
                    id SERIAL PRIMARY KEY,
                    sale_id VARCHAR(50) NOT NULL,
                    anomaly_type VARCHAR(50) NOT NULL,
                    anomaly_code INTEGER,
                    event_timestamp VARCHAR(50),
                    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)
        conn.commit()
    finally:
        conn.close()


def main():
    _init_table()
    consumer = KafkaConsumer(
        "anomaly-alerts",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        group_id="anomaly-consumer",
    )
    for msg in consumer:
        conn = None
        try:
            event = json.loads(msg.value.decode("utf-8"))
            conn = _get_pg_connection()
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO anomaly_alerts (sale_id, anomaly_type, event_timestamp) "
                    "VALUES (%s, %s, %s)",
                    (event.get("sale_id"), event.get("anomaly_type"),
                     event.get("event_timestamp")),
                )
            conn.commit()
        except Exception as e:
            print(f"Error: {e}", flush=True)
        finally:
            if conn is not None:
                conn.close()


if __name__ == "__main__":
    main()