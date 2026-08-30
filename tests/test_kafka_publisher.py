import json
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from kafka_producer.kafka_publisher import KafkaPublisher


class TestKafkaPublisher(unittest.TestCase):

    def setUp(self):
        self.producer = Mock()
        self.admin_client = Mock()

        self.publisher = KafkaPublisher(
            bootstrap_servers="localhost:9092",
            topic="binance_aggtrade",
            producer=self.producer,
            admin_client=self.admin_client,
        )

    def test_empty_configuration_raises_value_error(self):
        with self.assertRaises(ValueError):
            KafkaPublisher(
                bootstrap_servers="",
                topic="binance_aggtrade",
                producer=self.producer,
                admin_client=self.admin_client,
            )

        with self.assertRaises(ValueError):
            KafkaPublisher(
                bootstrap_servers="   ",
                topic="binance_aggtrade",
                producer=self.producer,
                admin_client=self.admin_client,
            )

        with self.assertRaises(ValueError):
            KafkaPublisher(
                bootstrap_servers=123,
                topic="binance_aggtrade",
                producer=self.producer,
                admin_client=self.admin_client,
            )

        with self.assertRaises(ValueError):
            KafkaPublisher(
                bootstrap_servers="localhost:9092",
                topic="",
                producer=self.producer,
                admin_client=self.admin_client,
            )

        with self.assertRaises(ValueError):
            KafkaPublisher(
                bootstrap_servers="localhost:9092",
                topic="   ",
                producer=self.producer,
                admin_client=self.admin_client,
            )

        with self.assertRaises(ValueError):
            KafkaPublisher(
                bootstrap_servers="localhost:9092",
                topic=123,
                producer=self.producer,
                admin_client=self.admin_client,
            )

        with self.assertRaises(ValueError):
            KafkaPublisher(
                bootstrap_servers="localhost:9092",
                topic="binance_aggtrade",
                producer=self.producer,
                admin_client=self.admin_client,
                flush_timeout_seconds=0,
            )

        with self.assertRaises(ValueError):
            KafkaPublisher(
                bootstrap_servers="localhost:9092",
                topic="binance_aggtrade",
                producer=self.producer,
                admin_client=self.admin_client,
                flush_timeout_seconds=-1,
            )

    def test_existing_topic_does_not_create_topic(self):
        self.admin_client.list_topics.return_value = SimpleNamespace(
            topics={
                "binance_aggtrade": object(),
            }
        )

        self.publisher.ensure_topic()

        self.admin_client.create_topics.assert_not_called()

    def test_missing_topic_creates_topic_and_waits_for_result(self):
        self.admin_client.list_topics.return_value = SimpleNamespace(
            topics={}
        )

        future = Mock()

        self.admin_client.create_topics.return_value = {
            "binance_aggtrade": future,
        }

        self.publisher.ensure_topic()

        self.admin_client.create_topics.assert_called_once()

        future.result.assert_called_once_with()

    def test_publish_sends_topic_utf8_key_and_compact_json_bytes(self):
        event = {
            "symbol": "BTCUSDT",
            "price": 50000.12,
            "quantity": 0.001,
        }

        self.publisher.publish(event)

        self.producer.produce.assert_called_once()

        call = self.producer.produce.call_args

        self.assertEqual(
            call.kwargs["topic"],
            "binance_aggtrade",
        )

        self.assertEqual(
            call.kwargs["key"],
            b"BTCUSDT",
        )

        payload = call.kwargs["value"]

        self.assertIsInstance(payload, bytes)

        self.assertNotIn(b": ", payload)
        self.assertNotIn(b", ", payload)

        decoded = json.loads(
            payload.decode("utf-8")
        )

        self.assertEqual(decoded, event)

        self.assertEqual(
            call.kwargs["callback"],
            self.publisher._delivery_callback,
        )

        self.producer.poll.assert_called_once_with(0)

    def test_missing_or_empty_symbol_raises_value_error(self):
        invalid_events = [
            {},
            {"symbol": ""},
            {"symbol": "   "},
            {"symbol": None},
            {"symbol": 123},
        ]

        for event in invalid_events:
            with self.subTest(event=event):
                with self.assertRaises(ValueError):
                    self.publisher.publish(event)

        self.producer.produce.assert_not_called()

    def test_close_with_zero_remaining_messages_succeeds(self):
        self.producer.flush.return_value = 0

        self.publisher.close()

        self.producer.flush.assert_called_once_with(30.0)

    def test_close_with_remaining_messages_raises_runtime_error(self):
        self.producer.flush.return_value = 2

        with self.assertRaises(RuntimeError):
            self.publisher.close()

        self.producer.flush.assert_called_once_with(30.0)

    def test_delivery_callback_with_success_does_not_store_error(self):
        self.publisher._delivery_callback(None, Mock())

        self.assertIsNone(
            self.publisher._delivery_error
        )

    def test_delivery_callback_with_error_stores_error(self):
        delivery_error = RuntimeError(
            "broker delivery failed"
        )

        self.publisher._delivery_callback(
            delivery_error,
            Mock(),
        )

        self.assertIs(
            self.publisher._delivery_error,
            delivery_error,
        )

    def test_close_with_delivery_error_raises_runtime_error(self):
        delivery_error = RuntimeError(
            "broker delivery failed"
        )

        self.publisher._delivery_error = delivery_error
        self.producer.flush.return_value = 0

        with self.assertRaisesRegex(
            RuntimeError,
            "Kafka delivery failed",
        ):
            self.publisher.close()

        self.producer.flush.assert_called_once_with(30.0)


if __name__ == "__main__":
    unittest.main()