import json
import random
import time
from datetime import datetime, timedelta, timezone

from faker import Faker
from kafka import KafkaProducer, JsonSerializer, DefaultSerializer

from config.generator_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    NUM_USERS,
    ERROR_RATE,
    MIN_INTERVAL,
    MAX_INTERVAL,
    QUANTITY_DISTRIBUTION,
    USER_SEGMENTS,
    USER_SEGMENT_WEIGHTS,
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
            "segment": random.choices(USER_SEGMENTS, weights=USER_SEGMENT_WEIGHTS.values())[0],
        }
        for i in range(1, n + 1)
    ]

def genera_evento_valido(sale_counter, products, users, event_timestamp=None):
    sale_id = f"S-{sale_counter:06d}"
    user = random.choice(users)
    product = random.choice(products)

    base_price = product["base_price"]
    unit_price = round(base_price * random.uniform(1 - PRICE_VOLATILITY, 1 + PRICE_VOLATILITY), 2)
    quantity = random.choices(
        list(QUANTITY_DISTRIBUTION.keys()),
        weights=QUANTITY_DISTRIBUTION.values(),
    )[0]

    return {
        "sale_id": sale_id,
        "user_id": user["user_id"],
        "product_id": product["product_id"],
        "quantity": quantity,
        "unit_price": unit_price,
        "event_timestamp": event_timestamp.isoformat() if event_timestamp else None,
    }

def genera_evento_non_valido(sale_counter, products, users, event_timestamp=None):
    evento = genera_evento_valido(sale_counter, products, users, event_timestamp)
    tipo_errore = random.choice(["quantita_zero", "prezzo_negativo", "campo_mancante", "timestamp_corrotto"])

    if tipo_errore == "quantita_zero":
        evento["quantity"] = 0
    elif tipo_errore == "prezzo_negativo":
        evento["unit_price"] = -5.00
    elif tipo_errore == "campo_mancante":
        campo_rimosso = random.choice(["sale_id", "user_id", "product_id"])
        evento.pop(campo_rimosso)
    elif tipo_errore == "timestamp_corrotto":
        evento["event_timestamp"] = "NOT_A_TIMESTAMP"

    return evento, tipo_errore

def main():
    print("Caricamento prodotti...")
    products = load_products()

    print("Generazione utenti...")
    users = generate_users(NUM_USERS)

    print("Inizializzazione Kafka Producer")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=JsonSerializer(),
            key_serializer=DefaultSerializer(),
        )
        print("Connessione con Apache Kafka stabilita con successo.")
    except Exception as e:
        print(f"Errore critico di connessione: {e}")
        print("Assicurati che i container Docker siano attivi tramite 'docker compose up -d'.")
        return

    print(f"Inizio trasmissione streaming sul topic '{KAFKA_TOPIC}'... (Ctrl+C per interrompere)\n")
    sale_counter = 1

    while True:
        event_timestamp = datetime.now(timezone.utc) - timedelta(
            hours=random.uniform(0, TIMESTAMP_WINDOW_HOURS)
        )

        if random.random() < ERROR_RATE:
            evento, errore = genera_evento_non_valido(sale_counter, products, users, event_timestamp)
            tag_log = f"[ANOMALIA - {errore.upper()}]"
        else:
            evento = genera_evento_valido(sale_counter, products, users, event_timestamp)
            tag_log = "[VALIDO]"

        s_id = evento.get("sale_id", "MANCANTE")
        u_id = evento.get("user_id", "MANCANTE")
        p_id = evento.get("product_id", "MANCANTE")
        qty = evento.get("quantity", 0)
        price = evento.get("unit_price", 0)

        try:
            chiave_messaggio = evento.get("sale_id", None)
            producer.send(topic=KAFKA_TOPIC, key=chiave_messaggio, value=evento)
            print(f"Generated event: {s_id} | user={u_id} | product={p_id} | quantity={qty} | price={price} {tag_log}")
        except Exception as e:
            print(f"[ERRORE TRASMISSIONE] Impossibile inviare l'evento: {e}")

        sale_counter += 1
        time.sleep(random.uniform(MIN_INTERVAL, MAX_INTERVAL))

if __name__ == "__main__":
    main()