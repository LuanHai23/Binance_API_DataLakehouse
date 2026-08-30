import json
import unittest
from unittest.mock import Mock

from kafka_producer.pubsub_publisher import PubSubPublisher


VALID_EVENT = {
    "event_id": "binance:aggTrade:BTCUSDT:123",
    "symbol": "BTCUSDT",
    "schema_version": 1,
    "price": 50000.12,
    "quantity": 0.001,
}


class TestPubSubPublisher(unittest.TestCase):

    def setUp(self):
        self.client = Mock()
        self.client.topic_path.return_value = (
            "projects/test-project/topics/binance-events"
        )

        self.publisher = PubSubPublisher(
            project_id="test-project",
            topic_id="binance-events",
            client=self.client,
        )

    def test_invalid_project_topic_or_timeout_raises_value_error(self):
        with self.assertRaises(ValueError):
            PubSubPublisher(
                project_id="",
                topic_id="binance-events",
                client=self.client,
            )

        with self.assertRaises(ValueError):
            PubSubPublisher(
                project_id=123,
                topic_id="binance-events",
                client=self.client,
            )

        with self.assertRaises(ValueError):
            PubSubPublisher(
                project_id="test-project",
                topic_id="",
                client=self.client,
            )

        with self.assertRaises(ValueError):
            PubSubPublisher(
                project_id="test-project",
                topic_id=123,
                client=self.client,
            )

        with self.assertRaises(ValueError):
            PubSubPublisher(
                project_id="test-project",
                topic_id="binance-events",
                client=self.client,
                publish_timeout_seconds=0,
            )

        with self.assertRaises(ValueError):
            PubSubPublisher(
                project_id="test-project",
                topic_id="binance-events",
                client=self.client,
                publish_timeout_seconds=True,
            )

        with self.assertRaises(ValueError):
            PubSubPublisher(
                project_id="test-project",
                topic_id="binance-events",
                client=self.client,
                publish_timeout_seconds=float("inf"),
            )

        with self.assertRaises(ValueError):
            PubSubPublisher(
                project_id="test-project",
                topic_id="binance-events",
                client=self.client,
                publish_timeout_seconds=float("nan"),
            )

    def test_constructor_uses_topic_path(self):
        self.client.topic_path.assert_called_once_with(
            "test-project",
            "binance-events",
        )

        self.assertEqual(
            self.publisher.topic_path,
            "projects/test-project/topics/binance-events",
        )

    def test_publish_sends_topic_payload_and_attributes(self):
        future = Mock()
        self.client.publish.return_value = future

        event = VALID_EVENT.copy()

        self.publisher.publish(event)

        self.client.publish.assert_called_once()

        args, kwargs = self.client.publish.call_args

        self.assertEqual(
            args[0],
            "projects/test-project/topics/binance-events",
        )

        payload = args[1]

        self.assertIsInstance(payload, bytes)

        decoded = json.loads(
            payload.decode("utf-8")
        )

        self.assertEqual(decoded, event)

        self.assertEqual(
            kwargs["event_id"],
            "binance:aggTrade:BTCUSDT:123",
        )

        self.assertEqual(
            kwargs["symbol"],
            "BTCUSDT",
        )

        self.assertEqual(
            kwargs["schema_version"],
            "1",
        )

        future.add_done_callback.assert_called_once()

    def test_publish_trims_event_id_and_symbol_attributes(self):
        future = Mock()
        self.client.publish.return_value = future

        event = VALID_EVENT.copy()
        event["event_id"] = (
            "  binance:aggTrade:BTCUSDT:123  "
        )
        event["symbol"] = "  BTCUSDT  "

        self.publisher.publish(event)

        _, kwargs = self.client.publish.call_args

        self.assertEqual(
            kwargs["event_id"],
            "binance:aggTrade:BTCUSDT:123",
        )

        self.assertEqual(
            kwargs["symbol"],
            "BTCUSDT",
        )

    def test_missing_required_attributes_do_not_publish(self):
        invalid_events = [
            {
                "symbol": "BTCUSDT",
                "schema_version": 1,
            },
            {
                "event_id": "binance:aggTrade:BTCUSDT:123",
                "schema_version": 1,
            },
            {
                "event_id": "binance:aggTrade:BTCUSDT:123",
                "symbol": "BTCUSDT",
            },
        ]

        for event in invalid_events:
            with self.subTest(event=event):
                with self.assertRaises(ValueError):
                    self.publisher.publish(event)

        self.client.publish.assert_not_called()

    def test_invalid_schema_version_does_not_publish(self):
        invalid_versions = [
            None,
            False,
            0,
            -1,
            "1",
            {},
        ]

        for version in invalid_versions:
            with self.subTest(schema_version=version):
                event = VALID_EVENT.copy()
                event["schema_version"] = version

                with self.assertRaises(ValueError):
                    self.publisher.publish(event)

                self.client.publish.assert_not_called()

    def test_success_callback_drains_future_and_close_succeeds(self):
        future = Mock()
        future.result.return_value = None

        self.client.publish.return_value = future

        self.publisher.publish(
            VALID_EVENT.copy()
        )

        callback = (
            future.add_done_callback.call_args.args[0]
        )

        callback(future)

        self.publisher.close()

        self.client.stop.assert_called_once()
        future.result.assert_called_once_with()

    def test_failed_callback_causes_close_to_raise_runtime_error(self):
        future = Mock()

        publish_error = RuntimeError(
            "pubsub delivery failed"
        )

        future.result.side_effect = publish_error

        self.client.publish.return_value = future

        self.publisher.publish(
            VALID_EVENT.copy()
        )

        callback = (
            future.add_done_callback.call_args.args[0]
        )

        callback(future)

        with self.assertRaisesRegex(
            RuntimeError,
            "Pub/Sub publish failed",
        ):
            self.publisher.close()

        self.client.stop.assert_called_once()

    def test_pending_future_is_waited_with_timeout(self):
        future = Mock()

        # Keep the future pending from the callback perspective.
        self.client.publish.return_value = future

        self.publisher.publish(
            VALID_EVENT.copy()
        )

        self.publisher.close()

        self.client.stop.assert_called_once()

        future.result.assert_called_with(
            timeout=30.0
        )

    def test_previous_publish_error_fails_fast(self):
        future = Mock()

        publish_error = RuntimeError(
            "first publish failed"
        )

        future.result.side_effect = publish_error

        self.client.publish.return_value = future

        self.publisher.publish(
            VALID_EVENT.copy()
        )

        callback = (
            future.add_done_callback.call_args.args[0]
        )

        callback(future)

        with self.assertRaisesRegex(
            RuntimeError,
            "Previous Pub/Sub publish failed",
        ):
            self.publisher.publish(
                VALID_EVENT.copy()
            )

        self.assertEqual(
            self.client.publish.call_count,
            1,
        )

    def test_close_calls_stop_only_once(self):
        self.publisher.close()
        self.publisher.close()

        self.client.stop.assert_called_once()

    def test_publish_after_close_raises_runtime_error(self):
        self.publisher.close()

        with self.assertRaisesRegex(
            RuntimeError,
            "Pub/Sub publisher is closed",
        ):
            self.publisher.publish(
                VALID_EVENT.copy()
            )

        self.client.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()