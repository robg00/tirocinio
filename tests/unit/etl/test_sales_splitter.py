import json
from datetime import datetime, timezone

import pytest

from etl.sales_splitter import split_event


def _valid_event(**overrides) -> dict:
    base = {
        "sale_id": "S-000001",
        "user_id": "U-0001",
        "product_id": 101,
        "quantity": 2,
        "unit_price": 50.0,
        "total_amount": 100.0,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }
    base.update(overrides)
    return base


class TestSplitEventValid:
    def test_returns_valid_tag(self):
        result = split_event(_valid_event())
        assert result[0][0] == "valid"

    def test_enriches_with_date_id(self):
        event = _valid_event(event_timestamp="2026-07-09T12:30:00+00:00")
        result = split_event(event)
        assert result[0][1]["date_id"] == "2026-07-09"

    def test_enriches_with_value_band_low(self):
        result = split_event(_valid_event(total_amount=30.0))
        assert result[0][1]["value_band"] == "low"

    def test_enriches_with_value_band_medium(self):
        result = split_event(_valid_event(total_amount=100.0))
        assert result[0][1]["value_band"] == "medium"

    def test_enriches_with_value_band_high(self):
        result = split_event(_valid_event(total_amount=250.0))
        assert result[0][1]["value_band"] == "high"

    def test_enriches_with_sale_hour(self):
        event = _valid_event(event_timestamp="2026-07-09T15:45:00+00:00")
        result = split_event(event)
        assert result[0][1]["sale_hour"] == 15
        assert result[0][1]["sale_minute"] == 45

    def test_enriches_with_day_of_week(self):
        event = _valid_event(event_timestamp="2026-07-09T12:00:00+00:00")
        result = split_event(event)
        assert result[0][1]["day_of_week"] == "Thursday"

    def test_preserves_original_fields(self):
        event = _valid_event()
        result = split_event(event)
        for key in ("sale_id", "user_id", "product_id", "quantity", "unit_price", "total_amount"):
            assert result[0][1][key] == event[key]

    def test_boundary_low_medium(self):
        result = split_event(_valid_event(total_amount=49.99))
        assert result[0][1]["value_band"] == "low"

        result = split_event(_valid_event(total_amount=50.0))
        assert result[0][1]["value_band"] == "medium"

    def test_boundary_medium_high(self):
        result = split_event(_valid_event(total_amount=199.99))
        assert result[0][1]["value_band"] == "medium"

        result = split_event(_valid_event(total_amount=200.0))
        assert result[0][1]["value_band"] == "high"


class TestSplitEventMissingField:
    @pytest.mark.parametrize("missing", ["sale_id", "user_id", "product_id"])
    def test_returns_invalid(self, missing):
        event = _valid_event(**{missing: None})
        del event[missing]
        result = split_event(event)
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "missing_field"

    def test_all_three_missing(self):
        event = {
            "quantity": 1,
            "unit_price": 10.0,
            "total_amount": 10.0,
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        result = split_event(event)
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "missing_field"
        assert result[0][1]["sale_id"] == "N/A"
        assert result[0][1]["user_id"] == "N/A"
        assert result[0][1]["product_id"] == "N/A"


class TestSplitEventQuantityZero:
    def test_quantity_zero(self):
        result = split_event(_valid_event(quantity=0))
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "quantity_zero"

    def test_quantity_negative(self):
        result = split_event(_valid_event(quantity=-5))
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "quantity_zero"

    def test_stops_before_timestamp_validation(self):
        result = split_event(_valid_event(quantity=0, event_timestamp="corrupted-timestamp"))
        assert result[0][1]["error_reason"] == "quantity_zero"


class TestSplitEventNegativePrice:
    def test_negative_price(self):
        result = split_event(_valid_event(unit_price=-10.0))
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "negative_price"

    def test_zero_price(self):
        result = split_event(_valid_event(unit_price=0.0))
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "negative_price"


class TestSplitEventCorruptedTimestamp:
    def test_corrupted_timestamp_string(self):
        result = split_event(_valid_event(event_timestamp="corrupted-timestamp"))
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "corrupted_timestamp"

    def test_empty_timestamp(self):
        result = split_event(_valid_event(event_timestamp=""))
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "corrupted_timestamp"

    def test_missing_timestamp(self):
        event = _valid_event()
        del event["event_timestamp"]
        result = split_event(event)
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "corrupted_timestamp"

    def test_none_timestamp(self):
        result = split_event(_valid_event(event_timestamp=None))
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "corrupted_timestamp"


class TestSplitEventAlwaysReturnsList:
    def test_valid_returns_one_element(self):
        assert len(split_event(_valid_event())) == 1

    def test_invalid_returns_one_element(self):
        event = _valid_event(quantity=0)
        assert len(split_event(event)) == 1


class TestSplitEventEdgeCases:
    def test_total_amount_zero(self):
        result = split_event(_valid_event(total_amount=0.0))
        assert result[0][1]["value_band"] == "low"

    def test_total_amount_exact_low(self):
        result = split_event(_valid_event(total_amount=0.01))
        assert result[0][1]["value_band"] == "low"

    def test_total_amount_exact_medium(self):
        result = split_event(_valid_event(total_amount=50.0))
        assert result[0][1]["value_band"] == "medium"

    def test_total_amount_exact_high(self):
        result = split_event(_valid_event(total_amount=200.0))
        assert result[0][1]["value_band"] == "high"

    def test_enriched_fields_all_present(self):
        result = split_event(_valid_event())
        enriched = result[0][1]
        assert "date_id" in enriched
        assert "value_band" in enriched
        assert "sale_hour" in enriched
        assert "sale_minute" in enriched
        assert "day_of_week" in enriched

    def test_invalid_error_reason_always_present(self):
        cases = [
            ({}, "missing_field"),
            (_valid_event(quantity=0), "quantity_zero"),
            (_valid_event(unit_price=0), "negative_price"),
            (_valid_event(event_timestamp="garbage"), "corrupted_timestamp"),
        ]
        for event, reason in cases:
            result = split_event(event)
            assert result[0][1]["error_reason"] == reason, f"Expected {reason}"

    def test_timestamp_with_timezone(self):
        event = _valid_event(event_timestamp="2026-07-09T10:30:00+05:30")
        result = split_event(event)
        assert result[0][1]["date_id"] == "2026-07-09"
        assert result[0][1]["sale_hour"] == 10
        assert result[0][1]["sale_minute"] == 30

    def test_timestamp_utc_zulu(self):
        event = _valid_event(event_timestamp="2026-07-09T10:30:00Z")
        result = split_event(event)
        assert result[0][1]["date_id"] == "2026-07-09"

    def test_multi_field_validation_ordering(self):
        event = _valid_event(quantity=0, unit_price=-10, event_timestamp="bad")
        result = split_event(event)
        assert result[0][1]["error_reason"] == "quantity_zero"

    def test_all_missing_fields_defaults(self):
        event = {"irrelevant": True}
        result = split_event(event)
        assert result[0][1]["sale_id"] == "N/A"
        assert result[0][1]["user_id"] == "N/A"
        assert result[0][1]["product_id"] == "N/A"
        assert result[0][1]["quantity"] == 0
        assert result[0][1]["unit_price"] == 0
        assert result[0][1]["total_amount"] == 0
        assert result[0][1]["event_timestamp"] == "N/A"

    def test_quantity_negative_large(self):
        result = split_event(_valid_event(quantity=-100))
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "quantity_zero"

    def test_unit_price_negative_large(self):
        result = split_event(_valid_event(unit_price=-9999.99))
        assert result[0][0] == "invalid"
        assert result[0][1]["error_reason"] == "negative_price"

    def test_numeric_field_types(self):
        result = split_event(_valid_event(quantity=3, unit_price=49.99, total_amount=149.97))
        assert isinstance(result[0][1]["quantity"], int)
        assert isinstance(result[0][1]["unit_price"], float)
        assert isinstance(result[0][1]["total_amount"], (int, float))

    def test_day_of_week_values(self):
        events = [
            ("2026-07-06T00:00:00+00:00", "Monday"),
            ("2026-07-07T00:00:00+00:00", "Tuesday"),
            ("2026-07-08T00:00:00+00:00", "Wednesday"),
            ("2026-07-09T00:00:00+00:00", "Thursday"),
            ("2026-07-10T00:00:00+00:00", "Friday"),
            ("2026-07-11T00:00:00+00:00", "Saturday"),
            ("2026-07-12T00:00:00+00:00", "Sunday"),
        ]
        for ts, expected_day in events:
            result = split_event(_valid_event(event_timestamp=ts))
            assert result[0][1]["day_of_week"] == expected_day, f"Failed for {ts}"
