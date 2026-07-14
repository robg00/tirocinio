import json
import time
import uuid
import random
from datetime import datetime, timezone
from kafka import KafkaProducer


PRODUCT_IDS = [101, 203, 305, 410, 512]
USERS = [f"user_{i:03d}" for i in range(1, 21)]


def generate_sale(anomaly_type=None):
    sale_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    sale = {
        "sale_id": sale_id,
        "user_id": random.choice(USERS),
        "product_id": random.choice(PRODUCT_IDS),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(5.0, 200.0), 2),
        "total_amount": 0.0,
        "event_timestamp": now,
    }

    if anomaly_type == "missing_field":
        del sale["user_id"]
        del sale["product_id"]
    elif anomaly_type == "quantity_zero":
        sale["quantity"] = 0
    elif anomaly_type == "negative_price":
        sale["unit_price"] = -random.uniform(10.0, 50.0)
    elif anomaly_type == "corrupted_timestamp":
        sale["event_timestamp"] = "corrupted-timestamp"
    elif anomaly_type == "high_value":
        sale["unit_price"] = 5000.0
        sale["quantity"] = 3

    sale["total_amount"] = round(sale.get("quantity", 0) * sale.get("unit_price", 0), 2)
    return sale


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Producer avviato. Ctrl+C per fermare.")
    print("Invio messaggi su 'sales-events'...")

    anomaly_types = [None, None, None, "missing_field", "quantity_zero",
                     "negative_price", "corrupted_timestamp", "high_value"]

    try:
        while True:
            anomaly = random.choice(anomaly_types)
            sale = generate_sale(anomaly)

            producer.send("sales-events", sale)
            producer.flush()

            label = anomaly or "normale"
            print(f"  [+] {label:20s} | {sale['sale_id'][:8]}... | ${sale['total_amount']:.2f}")

            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("\nProducer fermato.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
