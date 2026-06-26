import json
import time

import pytest
from kafka import KafkaProducer, KafkaConsumer, TopicPartition

TOPIC = "sales-events"
BOOTSTRAP = "localhost:9092"


@pytest.fixture(scope="module")
def producer():
    p = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
    )
    yield p
    p.close()


@pytest.fixture
def consumer():
    c = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        auto_offset_reset="earliest",
        consumer_timeout_ms=3000,
        group_id="test-group",
    )
    yield c
    c.close()


def _all_partitions():
    return [TopicPartition(TOPIC, i) for i in range(3)]


class TestKafkaConnection:
    def test_broker_is_reachable(self):
        p = KafkaProducer(bootstrap_servers=BOOTSTRAP)
        metadata = p.partitions_for(TOPIC)
        assert metadata is not None
        p.close()

    def test_topic_exists(self):
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        metadata = admin.describe_topics([TOPIC])
        topic_names = [t["name"] for t in metadata]
        assert TOPIC in topic_names
        admin.close()

    def test_topic_has_three_partitions(self):
        from kafka.admin import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
        metadata = admin.describe_topics([TOPIC])
        topic = next(t for t in metadata if t["name"] == TOPIC)
        assert len(topic["partitions"]) == 3
        admin.close()


class TestProduceAndConsume:
    def _get_end_offsets(self):
        c = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
        parts = _all_partitions()
        c.assign(parts)
        offsets = {}
        for p in parts:
            c.seek_to_end(p)
            offsets[p] = c.position(p)
        c.close()
        return offsets

    def _consume_from_offsets(self, offsets, sale_id, timeout_s=10):
        import time
        start = time.time()
        c = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        )
        c.assign(list(offsets.keys()))
        for p, offset in offsets.items():
            c.seek(p, offset)
        while time.time() - start < timeout_s:
            records = c.poll(timeout_ms=1000)
            for tp, msgs in records.items():
                for msg in msgs:
                    if msg.value is not None and msg.value.get("sale_id") == sale_id:
                        c.close()
                        return msg.value
        c.close()
        pytest.fail(f"Evento {sale_id} non trovato in {timeout_s}s")

    def test_produce_and_consume_valid_event(self, producer):
        offsets = self._get_end_offsets()
        event = {
            "sale_id": "S-TEST-001",
            "user_id": "U-TEST",
            "product_id": 101,
            "quantity": 2,
            "unit_price": 99.99,
            "event_timestamp": "2026-06-24T12:00:00",
        }
        producer.send(TOPIC, value=event).get(timeout=5)
        result = self._consume_from_offsets(offsets, "S-TEST-001")
        assert result["quantity"] == 2

    def test_produce_event_with_error_field(self, producer):
        offsets = self._get_end_offsets()
        event = {
            "sale_id": "S-TEST-ERR-001",
            "user_id": "U-TEST",
            "product_id": 101,
            "quantity": 0,
            "unit_price": 99.99,
            "event_timestamp": "2026-06-24T12:00:00",
        }
        producer.send(TOPIC, value=event).get(timeout=5)
        result = self._consume_from_offsets(offsets, "S-TEST-ERR-001")
        assert result["quantity"] == 0

    def test_generator_produces_to_topic(self, consumer):
        parts = _all_partitions()
        consumer.assign(parts)
        total = 0
        for p in parts:
            consumer.seek_to_end(p)
            total += consumer.position(p)
        assert total > 0, "Nessun messaggio trovato!"

    def test_message_has_valid_json(self, consumer):
        parts = _all_partitions()
        consumer.assign(parts)
        for p in parts:
            consumer.seek_to_beginning(p)
        records = consumer.poll(timeout_ms=3000)
        for tp, msgs in records.items():
            for msg in msgs:
                if msg.value is None:
                    continue
                assert isinstance(msg.value, dict)
                required = {"sale_id", "user_id", "product_id", "quantity", "unit_price", "event_timestamp"}
                assert required.issuperset(msg.value.keys())
                return
