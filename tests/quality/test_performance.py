import json
from datetime import datetime, timezone

import pytest

from scripts.event_generator import (
    load_products,
    generate_users,
    generate_valid_event,
    generate_invalid_event,
)

PRODUCTS = load_products()
USERS = generate_users(100)


@pytest.mark.parametrize("fn", ["valid", "invalid"])
def test_generation_throughput(benchmark, fn):
    fn_map = {
        "valid": lambda i: generate_valid_event(i, PRODUCTS, USERS, datetime.now(timezone.utc)),
        "invalid": lambda i: generate_invalid_event(i, PRODUCTS, USERS, datetime.now(timezone.utc)),
    }
    f = fn_map[fn]
    benchmark.extra_info["tipo"] = fn
    benchmark.group = "generation"
    benchmark.weeks = 1
    _ = benchmark(lambda: [f(i) for i in range(1_000)])


def test_serialization_throughput(benchmark):
    events = [generate_valid_event(i, PRODUCTS, USERS, datetime.now(timezone.utc)) for i in range(1_000)]
    benchmark.group = "serialization"
    benchmark.weeks = 1
    result = benchmark(lambda: [json.dumps(ev, default=str).encode("utf-8") for ev in events])
    assert len(result) == 1_000


def test_full_pipeline(benchmark):
    benchmark.group = "pipeline"
    benchmark.weeks = 1
    result = benchmark(lambda: [
        json.dumps(generate_valid_event(i, PRODUCTS, USERS, datetime.now(timezone.utc)), default=str).encode("utf-8")
        for i in range(1_000)
    ])
    assert len(result) == 1_000


def test_message_size():
    sizes = []
    for i in range(100_000):
        ev = generate_valid_event(i, PRODUCTS, USERS, datetime.now(timezone.utc))
        serialized = json.dumps(ev, default=str).encode("utf-8")
        sizes.append(len(serialized))
    avg_size = sum(sizes) / len(sizes)
    max_size = max(sizes)
    min_size = min(sizes)
    print(f"\n  Average size: {avg_size:.1f} bytes")
    print(f"  Min size:     {min_size} bytes")
    print(f"  Max size:     {max_size} bytes")
    assert 100 <= avg_size <= 500


@pytest.mark.skip(reason="Requires Kafka running")
def test_kafka_producer_throughput(benchmark):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
    )

    events = [
        generate_valid_event(i, PRODUCTS, USERS, datetime.now(timezone.utc))
        for i in range(1_000)
    ]

    def _produce():
        for i, ev in enumerate(events):
            producer.send(topic="sales-events", key=f"S-{i:06d}", value=ev)
        producer.flush()

    benchmark.group = "kafka"
    benchmark.weeks = 1
    benchmark(_produce)
    producer.close()
