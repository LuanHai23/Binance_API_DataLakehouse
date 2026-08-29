import json
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from kafka_producer.event_contract import build_agg_trade_event


VALID_RAW_EVENT = {
    "e": "aggTrade",
    "s": "btcusdt",
    "a": 123,
    "p": "50000.12000000",
    "q": "0.00100000",
    "f": 120,
    "l": 123,
    "T": 1720000000123,
    "m": False,
}


class TestBuildAggTradeEvent(unittest.TestCase):

    def test_valid_event_returns_expected_schema_and_is_json_serializable(self):
        raw = VALID_RAW_EVENT.copy()

        event = build_agg_trade_event(
            raw,
            ingested_at=datetime(
                2026,
                8,
                28,
                12,
                0,
                tzinfo=timezone.utc,
            ),
        )

        expected_fields = {
            "event_id",
            "source",
            "event_type",
            "schema_version",
            "symbol",
            "trade_id",
            "first_trade_id",
            "last_trade_id",
            "price",
            "quantity",
            "trade_time",
            "price_raw",
            "quantity_raw",
            "event_time",
            "ingested_at",
            "is_buyer_maker",
        }

        self.assertEqual(set(event.keys()), expected_fields)

        self.assertEqual(
            event["event_id"],
            "binance:aggTrade:BTCUSDT:123",
        )
        self.assertEqual(event["source"], "binance_aggTrade")
        self.assertEqual(event["event_type"], "aggTrade")
        self.assertEqual(event["schema_version"], 1)
        self.assertEqual(event["symbol"], "BTCUSDT")

        self.assertEqual(event["trade_id"], 123)
        self.assertEqual(event["first_trade_id"], 120)
        self.assertEqual(event["last_trade_id"], 123)

        self.assertIsInstance(event["price"], float)
        self.assertIsInstance(event["quantity"], float)

        self.assertEqual(event["price_raw"], "50000.12000000")
        self.assertEqual(event["quantity_raw"], "0.00100000")

        self.assertEqual(event["trade_time"], 1720000000123)

        self.assertTrue(event["event_time"].endswith("Z"))
        self.assertTrue(event["ingested_at"].endswith("Z"))

        self.assertIs(event["is_buyer_maker"], False)

        # Must be JSON serializable.
        json.dumps(event, allow_nan=False)

    def test_event_id_is_deterministic_for_same_symbol_and_trade_id(self):
        raw1 = VALID_RAW_EVENT.copy()
        raw2 = VALID_RAW_EVENT.copy()

        event1 = build_agg_trade_event(raw1)
        event2 = build_agg_trade_event(raw2)

        self.assertEqual(
            event1["event_id"],
            event2["event_id"],
        )

        self.assertEqual(
            event1["event_id"],
            "binance:aggTrade:BTCUSDT:123",
        )

    def test_missing_required_field_raises_value_error(self):
        raw = VALID_RAW_EVENT.copy()
        del raw["e"]

        with self.assertRaises(ValueError):
            build_agg_trade_event(raw)

    def test_wrong_event_type_raises_value_error(self):
        raw = VALID_RAW_EVENT.copy()
        raw["e"] = "trade"

        with self.assertRaises(ValueError):
            build_agg_trade_event(raw)

    def test_invalid_price_raises_value_error(self):
        invalid_prices = [
            "NaN",
            "Infinity",
            "-Infinity",
            "0",
            "-1",
            "1E9999",
        ]

        for invalid_price in invalid_prices:
            with self.subTest(price=invalid_price):
                raw = VALID_RAW_EVENT.copy()
                raw["p"] = invalid_price

                with self.assertRaises(ValueError):
                    build_agg_trade_event(raw)

    def test_invalid_quantity_raises_value_error(self):
        invalid_quantities = [
            "NaN",
            "Infinity",
            "-Infinity",
            "0",
            "-1",
            "1E9999",
        ]

        for invalid_quantity in invalid_quantities:
            with self.subTest(quantity=invalid_quantity):
                raw = VALID_RAW_EVENT.copy()
                raw["q"] = invalid_quantity

                with self.assertRaises(ValueError):
                    build_agg_trade_event(raw)

    def test_must_be_boolean(self):
        raw = VALID_RAW_EVENT.copy()
        raw["m"] = "false"

        with self.assertRaises(ValueError):
            build_agg_trade_event(raw)

    def test_naive_ingested_at_raises_value_error(self):
        raw = VALID_RAW_EVENT.copy()

        naive_datetime = datetime(
            2026,
            8,
            28,
            12,
            0,
        )

        with self.assertRaises(ValueError):
            build_agg_trade_event(
                raw,
                ingested_at=naive_datetime,
            )

    def test_boolean_trade_id_raises_value_error(self):
        raw = VALID_RAW_EVENT.copy()
        raw["a"] = True

        with self.assertRaises(ValueError):
            build_agg_trade_event(raw)

    def test_fractional_trade_id_raises_value_error(self):
        raw = VALID_RAW_EVENT.copy()
        raw["a"] = 123.9

        with self.assertRaises(ValueError):
            build_agg_trade_event(raw)

    def test_negative_trade_id_raises_value_error(self):
        raw = VALID_RAW_EVENT.copy()
        raw["a"] = -1

        with self.assertRaises(ValueError):
            build_agg_trade_event(raw)

    def test_trade_id_rejects_non_int_types(self):
        invalid_trade_ids = [
            123.0,
            Decimal("123.9"),
            "123",
        ]

        for invalid_trade_id in invalid_trade_ids:
            with self.subTest(trade_id=invalid_trade_id):
                raw = VALID_RAW_EVENT.copy()
                raw["a"] = invalid_trade_id

                with self.assertRaises(ValueError):
                    build_agg_trade_event(raw)

if __name__ == "__main__":
    unittest.main()