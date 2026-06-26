import json
import random
import time
from datetime import datetime, timedelta, timezone

from faker import Faker
from kafka import KafkaProducer

from config.generator_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    NUM_USERS,
    ERROR_RATE,
    MIN_INTERVAL,
    MAX_INTERVAL,
    QUANTITY_DISTRIBUTION,
    USER_SEGMENTS,
    PRICE_VOLATILITY,
    TIMESTAMP_WINDOW_HOURS,
)

fake = Faker("it_IT")


def load_products(path="config/products.json"):
    with open(path) as f:
        return json.load(f)


def generate_users(n):
    return [
        {
            "user_id": f"U-{i:04d}",
            "segment": random.choices(
                list(USER_SEGMENTS.keys()), weights=list(USER_SEGMENTS.values())
            )[0],
        }
        for i in range(1, n + 1)
    ]


def generate_valid_event(sale_counter, products, users, event_timestamp=None):
    sale_id = f"S-{sale_counter:06d}"
    user = random.choice(users)
    product = random.choice(products)

    base_price = product["base_price"]
    unit_price = round(
        base_price * random.uniform(1 - PRICE_VOLATILITY, 1 + PRICE_VOLATILITY), 2
    )
    quantity = random.choices(
        list(QUANTITY_DISTRIBUTION.keys()),
        weights=list(QUANTITY_DISTRIBUTION.values()),
    )[0]

    return {
        "sale_id": sale_id,
        "user_id": user["user_id"],
        "product_id": product["product_id"],
        "quantity": quantity,
        "unit_price": unit_price,
        "event_timestamp": event_timestamp.isoformat() if event_timestamp else None,
    }


def generate_invalid_event(sale_counter, products, users, event_timestamp=None):
    event = generate_valid_event(sale_counter, products, users, event_timestamp)
    error_type = random.choice(
        ["quantity_zero", "negative_price", "missing_field", "corrupted_timestamp"]
    )

    if error_type == "quantity_zero":
        event["quantity"] = 0
    elif error_type == "negative_price":
        event["unit_price"] = -5.00
    elif error_type == "missing_field":
        removed_field = random.choice(["sale_id", "user_id", "product_id"])
        event.pop(removed_field)
    elif error_type == "corrupted_timestamp":
        event["event_timestamp"] = "NOT_A_TIMESTAMP"

    return event, error_type


def main():
    print("Loading products...")
    products = load_products()

    print("Generating users...")
    users = generate_users(NUM_USERS)

    print("Initializing Kafka Producer")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        )
        print("Successfully connected to Apache Kafka.")
    except Exception as e:
        print(f"Critical connection error: {e}")
        print("Ensure Docker containers are running via 'docker compose up -d'.")
        return

    print(
        f"Starting stream transmission on topic '{KAFKA_TOPIC}'... (Ctrl+C to stop)\n"
    )
    sale_counter = 1

    while True:
        event_timestamp = datetime.now(timezone.utc) - timedelta(
            hours=random.uniform(0, TIMESTAMP_WINDOW_HOURS)
        )

        if random.random() < ERROR_RATE:
            event, error = generate_invalid_event(
                sale_counter, products, users, event_timestamp
            )
            log_tag = f"[ANOMALY - {error.upper()}]"
        else:
            event = generate_valid_event(sale_counter, products, users, event_timestamp)
            log_tag = "[VALID]"

        s_id = event.get("sale_id", "MISSING")
        u_id = event.get("user_id", "MISSING")
        p_id = event.get("product_id", "MISSING")
        qty = event.get("quantity", 0)
        price = event.get("unit_price", 0)

        try:
            message_key = event.get("sale_id", None)
            producer.send(topic=KAFKA_TOPIC, key=message_key, value=event)
            print(
                f"Generated event: {s_id} | user={u_id} | product={p_id} | quantity={qty} | price={price} {log_tag}"
            )
        except Exception as e:
            print(f"[TRANSMISSION ERROR] Could not send event: {e}")

        sale_counter += 1
        time.sleep(random.uniform(MIN_INTERVAL, MAX_INTERVAL))


if __name__ == "__main__":
    main()
