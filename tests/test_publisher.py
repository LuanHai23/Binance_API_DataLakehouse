import json
import unittest

from kafka_producer.publisher import serialize_event


class TestSerializeEvent(unittest.TestCase):

    def test_valid_event_returns_bytes_and_round_trips(self):
        event = {
            "event_id": "binance:aggTrade:BTCUSDT:123",
            "symbol": "BTCUSDT",
            "price": 50000.12,
            "quantity": 0.001,
            "event_type": "aggTrade",
        }

        original = event.copy()

        result = serialize_event(event)

        self.assertIsInstance(result, bytes)

        decoded = result.decode("utf-8")
        deserialized = json.loads(decoded)

        self.assertEqual(deserialized, event)

        # Input event must not be mutated.
        self.assertEqual(event, original)

    def test_payload_is_compact(self):
        event = {
            "symbol": "BTCUSDT",
            "price": 50000.12,
            "quantity": 0.001,
        }

        result = serialize_event(event)

        self.assertNotIn(b": ", result)
        self.assertNotIn(b", ", result)

    def test_non_finite_float_raises_value_error(self):
        invalid_events = [
            {"price": float("inf")},
            {"price": float("nan")},
        ]

        for event in invalid_events:
            with self.subTest(event=event):
                with self.assertRaises(ValueError):
                    serialize_event(event)


if __name__ == "__main__":
    unittest.main()