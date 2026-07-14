import json
from datetime import datetime, timezone

from etl.sales_splitter import split_event


def _valid_event():
    return {
        "sale_id": "test-001",
        "user_id": "U-0001",
        "product_id": 101,
        "quantity": 3,
        "unit_price": 25.0,
        "total_amount": 75.0,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def test_benchmark_valid_event(benchmark):
    event = _valid_event()
    result = benchmark(split_event, event)
    assert len(result) == 1
    assert result[0][0] == "valid"


def test_benchmark_missing_field(benchmark):
    event = {"sale_id": "test-002"}
    result = benchmark(split_event, event)
    assert len(result) == 1
    assert result[0][0] == "invalid"


def test_benchmark_invalid_timestamp(benchmark):
    event = {
        **_valid_event(),
        "event_timestamp": "corrupted",
    }
    result = benchmark(split_event, event)
    assert len(result) == 1
    assert result[0][0] == "invalid"