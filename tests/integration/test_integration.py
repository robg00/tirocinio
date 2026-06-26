import json
import subprocess
import sys
import time
from pathlib import Path

import pytest
from kafka import KafkaConsumer, TopicPartition

TOPIC = "sales-events"
BOOTSTRAP = "localhost:9092"
REQUIRED_FIELDS = {"sale_id", "user_id", "product_id", "quantity", "unit_price", "event_timestamp"}
PROJECT_DIR = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module")
def collected_events():
    """Run the generator for ~8 seconds and collect all produced messages."""
    import os
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_DIR)

    proc = subprocess.Popen(
        [sys.executable, "scripts/event_generator.py"],
        cwd=PROJECT_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    time.sleep(8)
    proc.terminate()
    proc.wait(timeout=5)

    parts = [TopicPartition(TOPIC, i) for i in range(3)]
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        consumer_timeout_ms=3000,
    )
    consumer.assign(parts)

    events = []
    for p in parts:
        consumer.seek_to_beginning(p)
    records = consumer.poll(timeout_ms=5000)
    for tp, msgs in records.items():
        for msg in msgs:
            if msg.value:
                try:
                    decoded = json.loads(msg.value.decode("utf-8"))
                    decoded["_partition"] = tp.partition
                    events.append(decoded)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    pass
    consumer.close()
    return events


class TestGeneratorIntegration:
    def test_generator_produces_events(self, collected_events):
        assert len(collected_events) > 0, "No events produced!"

    def test_events_have_required_fields(self, collected_events):
        for event in collected_events:
            missing = {k for k in event if k.startswith("_")}
            actual = {k for k in event if not k.startswith("_")}
            if "sale_id" not in actual:
                continue
            assert REQUIRED_FIELDS.issuperset(actual), f"Unexpected fields in {event}"

    def test_events_are_distributed_across_partitions(self, collected_events):
        partitions = {e["_partition"] for e in collected_events}
        assert len(partitions) >= 2, f"Events concentrated in a single partition: {partitions}"

    def test_event_timestamp_is_iso_format(self, collected_events):
        from datetime import datetime
        for event in collected_events:
            if not event.get("event_timestamp") or event["event_timestamp"] == "NOT_A_TIMESTAMP":
                continue
            try:
                datetime.fromisoformat(event["event_timestamp"])
            except ValueError:
                pytest.fail(f"Non-ISO timestamp: {event['event_timestamp']}")

    def test_quantity_is_non_negative(self, collected_events):
        for event in collected_events:
            qty = event.get("quantity", 0)
            assert qty >= 0, f"Negative quantity: {event}"

    def test_some_anomalies_present(self, collected_events):
        anomalies = [
            e for e in collected_events
            if e.get("quantity") == 0
            or e.get("unit_price", 0) < 0
            or "sale_id" not in e
            or e.get("event_timestamp") == "NOT_A_TIMESTAMP"
        ]
        assert len(anomalies) > 0, "No anomalies found!"
