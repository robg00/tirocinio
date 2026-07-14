import json
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
import pytest

KAFKA_BOOTSTRAP = "localhost:9092"

_PRODUCER: KafkaProducer | None = None


def _get_producer() -> KafkaProducer:
    global _PRODUCER
    if _PRODUCER is None:
        _PRODUCER = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    return _PRODUCER


def _close_producer() -> None:
    global _PRODUCER
    if _PRODUCER is not None:
        _PRODUCER.close()
        _PRODUCER = None


def _send_event(event: dict) -> bool:
    try:
        future = _get_producer().send("sales-events", event)
        future.get(timeout=10)
        return True
    except Exception:
        return False


def _wait_for_event(topic: str, sale_id: str, timeout_s: int = 25) -> dict | None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        consumer_timeout_ms=timeout_s * 1000,
    )
    for msg in consumer:
        try:
            data = json.loads(msg.value.decode("utf-8"))
            if data.get("sale_id") == sale_id:
                consumer.close()
                return data
        except json.JSONDecodeError:
            continue
    consumer.close()
    return None


def _send_and_wait(topic: str, sale_id: str, event: dict) -> dict | None:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="latest",
        consumer_timeout_ms=25000,
    )
    if not _send_event(event):
        consumer.close()
        return None
    for msg in consumer:
        try:
            data = json.loads(msg.value.decode("utf-8"))
            if data.get("sale_id") == sale_id:
                consumer.close()
                return data
        except json.JSONDecodeError:
            continue
    consumer.close()
    return None


class TestETLSystemPipeline:
    @pytest.mark.usefixtures("etl_job")
    def test_valid_event_goes_to_valid_sales(self):
        sale_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        event = {
            "sale_id": sale_id,
            "user_id": "U-0001",
            "product_id": 101,
            "quantity": 3,
            "unit_price": 25.0,
            "total_amount": 75.0,
            "event_timestamp": now,
        }
        enriched = _send_and_wait("valid-sales", sale_id, event)
        assert enriched is not None, f"Event {sale_id} not found in valid-sales"
        assert enriched.get("date_id") == datetime.fromisoformat(now).strftime("%Y-%m-%d")
        assert enriched.get("value_band") == "medium"
        assert enriched.get("sale_hour") == datetime.fromisoformat(now).hour
        assert enriched.get("day_of_week") is not None

    @pytest.mark.usefixtures("etl_job")
    def test_missing_user_id_goes_to_invalid_sales(self):
        sale_id = str(uuid.uuid4())
        event = {
            "sale_id": sale_id,
            "quantity": 2,
            "unit_price": 50.0,
            "total_amount": 100.0,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        msg = _send_and_wait("invalid-sales-events", sale_id, event)
        assert msg is not None, f"Event {sale_id} not found in invalid-sales-events"
        assert msg.get("error_reason") == "missing_field"

    @pytest.mark.usefixtures("etl_job")
    def test_quantity_zero_goes_to_invalid(self):
        sale_id = str(uuid.uuid4())
        event = {
            "sale_id": sale_id,
            "user_id": "U-0001",
            "product_id": 101,
            "quantity": 0,
            "unit_price": 50.0,
            "total_amount": 0.0,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        msg = _send_and_wait("invalid-sales-events", sale_id, event)
        assert msg is not None
        assert msg.get("error_reason") == "quantity_zero"

    @pytest.mark.usefixtures("etl_job")
    def test_negative_price_goes_to_invalid(self):
        sale_id = str(uuid.uuid4())
        event = {
            "sale_id": sale_id,
            "user_id": "U-0001",
            "product_id": 101,
            "quantity": 1,
            "unit_price": -10.0,
            "total_amount": -10.0,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        msg = _send_and_wait("invalid-sales-events", sale_id, event)
        assert msg is not None
        assert msg.get("error_reason") == "negative_price"

    @pytest.mark.usefixtures("etl_job")
    def test_corrupted_timestamp_goes_to_invalid(self):
        sale_id = str(uuid.uuid4())
        event = {
            "sale_id": sale_id,
            "user_id": "U-0001",
            "product_id": 101,
            "quantity": 2,
            "unit_price": 50.0,
            "total_amount": 100.0,
            "event_timestamp": "corrupted-timestamp",
        }
        msg = _send_and_wait("invalid-sales-events", sale_id, event)
        assert msg is not None
        assert msg.get("error_reason") == "corrupted_timestamp"