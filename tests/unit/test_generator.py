import json
import os
import tempfile

import pytest

from scripts.event_generator import (
    load_products,
    generate_users,
    generate_valid_event,
    generate_invalid_event,
)

PRODUCTS = load_products()
USERS = generate_users(100)


class TestLoadProducts:
    def test_returns_list(self):
        assert isinstance(PRODUCTS, list)

    def test_has_24_products(self):
        assert len(PRODUCTS) == 24

    def test_each_product_has_required_keys(self):
        required = {"product_id", "name", "category", "base_price", "brand"}
        for product in PRODUCTS:
            assert required.issubset(product.keys()), f"Missing fields in {product}"

    def test_product_id_is_integer(self):
        for product in PRODUCTS:
            assert isinstance(product["product_id"], int)

    def test_base_price_is_positive(self):
        for product in PRODUCTS:
            assert product["base_price"] > 0

    def test_products_from_json_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([{"product_id": 1, "name": "Test", "category": "T", "base_price": 10.0, "brand": "B"}], f)
            f.flush()
            result = load_products(f.name)
            assert len(result) == 1
            assert result[0]["product_id"] == 1
            os.unlink(f.name)


class TestGenerateUsers:
    def test_returns_correct_count(self):
        assert len(generate_users(50)) == 50
        assert len(generate_users(100)) == 100

    def test_each_user_has_id_and_segment(self):
        for user in USERS:
            assert "user_id" in user
            assert "segment" in user

    def test_user_id_format(self):
        for user in USERS:
            assert user["user_id"].startswith("U-")
            assert len(user["user_id"]) == 6

    def test_segment_is_valid(self):
        valid = {"new", "regular", "vip"}
        for user in USERS:
            assert user["segment"] in valid


class TestGenerateValidEvent:
    def test_returns_dict_with_all_keys(self):
        event = generate_valid_event(1, PRODUCTS, USERS, None)
        expected = {"sale_id", "user_id", "product_id", "quantity", "unit_price", "total_amount", "event_timestamp"}
        assert expected.issubset(event.keys())

    def test_sale_id_format(self):
        event = generate_valid_event(1, PRODUCTS, USERS, None)
        assert event["sale_id"] == "S-000001"
        event = generate_valid_event(999, PRODUCTS, USERS, None)
        assert event["sale_id"] == "S-000999"

    def test_quantity_is_positive(self):
        for _ in range(100):
            event = generate_valid_event(1, PRODUCTS, USERS, None)
            assert event["quantity"] >= 1

    def test_unit_price_is_positive(self):
        for _ in range(100):
            event = generate_valid_event(1, PRODUCTS, USERS, None)
            assert event["unit_price"] > 0

    def test_user_id_from_pool(self):
        user_ids = {u["user_id"] for u in USERS}
        for _ in range(50):
            event = generate_valid_event(1, PRODUCTS, USERS, None)
            assert event["user_id"] in user_ids

    def test_product_id_from_catalog(self):
        product_ids = {p["product_id"] for p in PRODUCTS}
        for _ in range(50):
            event = generate_valid_event(1, PRODUCTS, USERS, None)
            assert event["product_id"] in product_ids

    def test_timestamp_format_when_provided(self):
        from datetime import datetime, timezone, timedelta
        ts = datetime.now(timezone.utc) - timedelta(hours=5)
        event = generate_valid_event(1, PRODUCTS, USERS, ts)
        assert event["event_timestamp"] == ts.strftime("%Y-%m-%d %H:%M:%S")

    def test_timestamp_is_none_when_not_provided(self):
        event = generate_valid_event(1, PRODUCTS, USERS, None)
        assert event["event_timestamp"] is None


class TestGenerateInvalidEvent:
    def test_returns_tuple(self):
        result = generate_invalid_event(1, PRODUCTS, USERS, None)
        assert isinstance(result, tuple)
        assert len(result) == 2

    @pytest.mark.parametrize("error_type", ["quantity_zero", "negative_price", "missing_field", "corrupted_timestamp"])
    def test_injects_specific_error(self, error_type):
        event, detected_type = None, None
        for _ in range(500):
            ev, err = generate_invalid_event(1, PRODUCTS, USERS, None)
            if err == error_type:
                event, detected_type = ev, err
                break
        assert event is not None, f"No event with error {error_type} generated in 500 attempts"

        if error_type == "quantity_zero":
            assert event["quantity"] == 0
        elif error_type == "negative_price":
            assert event["unit_price"] < 0
        elif error_type == "missing_field":
            missing = {"sale_id", "user_id", "product_id"} - set(event.keys())
            assert len(missing) > 0
        elif error_type == "corrupted_timestamp":
            assert event["event_timestamp"] == "corrupted-timestamp"

    def test_missing_field_is_one_of_expected(self):
        found = set()
        for _ in range(500):
            ev, err = generate_invalid_event(1, PRODUCTS, USERS, None)
            if err == "missing_field":
                missing = {"sale_id", "user_id", "product_id"} - set(ev.keys())
                found.update(missing)
        assert found == {"sale_id", "user_id", "product_id"}, f"Missing: {found}"

    def test_valid_event_still_valid_for_non_error_types(self):
        for _ in range(500):
            ev, err = generate_invalid_event(1, PRODUCTS, USERS, None)
            if err in ("quantity_zero", "negative_price", "corrupted_timestamp"):
                assert "sale_id" in ev
                assert "user_id" in ev
                assert "product_id" in ev
