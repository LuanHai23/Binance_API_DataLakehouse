import json
import unittest
from unittest.mock import Mock

from kafka_producer import Binance_kafka_producer as binance_producer


class TestBinanceProducer(unittest.TestCase):

    def setUp(self):
        binance_producer.msg_count = 0
        self.publisher = Mock()

    def tearDown(self):
        binance_producer.msg_count = 0

    def test_valid_message_publishes_once(self):
        raw_message = {
            "data": {
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
        }

        binance_producer.on_message(
            Mock(),
            json.dumps(raw_message),
            publisher=self.publisher,
        )

        self.publisher.publish.assert_called_once()

    def test_published_event_has_expected_contract_fields(self):
        raw_message = {
            "data": {
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
        }

        binance_producer.on_message(
            Mock(),
            json.dumps(raw_message),
            publisher=self.publisher,
        )

        self.publisher.publish.assert_called_once()

        event = self.publisher.publish.call_args.args[0]

        self.assertEqual(
            event["event_id"],
            "binance:aggTrade:BTCUSDT:123",
        )
        self.assertEqual(
            event["schema_version"],
            1,
        )
        self.assertEqual(
            event["symbol"],
            "BTCUSDT",
        )

    def test_message_without_data_is_not_published(self):
        raw_message = {
            "stream": "btcusdt@aggTrade",
        }

        binance_producer.on_message(
            Mock(),
            json.dumps(raw_message),
            publisher=self.publisher,
        )

        self.publisher.publish.assert_not_called()

    def test_invalid_json_is_not_published(self):
        binance_producer.on_message(
            Mock(),
            "{invalid-json",
            publisher=self.publisher,
        )

        self.publisher.publish.assert_not_called()

    def test_publisher_error_propagates(self):
        raw_message = {
            "data": {
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
        }

        self.publisher.publish.side_effect = RuntimeError(
            "Kafka delivery failed"
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "Kafka delivery failed",
        ):
            binance_producer.on_message(
                Mock(),
                json.dumps(raw_message),
                publisher=self.publisher,
            )

        self.publisher.publish.assert_called_once()


if __name__ == "__main__":
    unittest.main()