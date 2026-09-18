import unittest
from unittest.mock import Mock, patch

from kafka_producer.publisher_factory import create_publisher


class TestCreatePublisher(unittest.TestCase):

    @patch("kafka_producer.publisher_factory.KafkaPublisher")
    def test_missing_backend_defaults_to_kafka(
        self,
        mock_kafka_publisher,
    ):
        mock_instance = Mock()
        mock_kafka_publisher.return_value = mock_instance

        config = {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "KAFKA_TOPIC": "binance-events",
        }

        result = create_publisher(config)

        mock_kafka_publisher.assert_called_once_with(
            bootstrap_servers="localhost:9092",
            topic="binance-events",
        )

        self.assertIs(result, mock_instance)

    @patch("kafka_producer.publisher_factory.KafkaPublisher")
    def test_kafka_backend_is_normalized(
        self,
        mock_kafka_publisher,
    ):
        config = {
            "PUBLISH_BACKEND": "  KAFKA  ",
            "KAFKA_BOOTSTRAP_SERVERS": " localhost:9092 ",
            "KAFKA_TOPIC": " binance-events ",
        }

        create_publisher(config)

        mock_kafka_publisher.assert_called_once_with(
            bootstrap_servers="localhost:9092",
            topic="binance-events",
        )

    @patch("kafka_producer.publisher_factory.PubSubPublisher")
    def test_pubsub_backend_uses_project_and_topic(
        self,
        mock_pubsub_publisher,
    ):
        mock_instance = Mock()
        mock_pubsub_publisher.return_value = mock_instance

        config = {
            "PUBLISH_BACKEND": " pubsub ",
            "GCP_PROJECT_ID": " test-project ",
            "PUBSUB_TOPIC_ID": " binance-events ",
        }

        result = create_publisher(config)

        mock_pubsub_publisher.assert_called_once_with(
            project_id="test-project",
            topic_id="binance-events",
        )

        self.assertIs(result, mock_instance)

    def test_missing_kafka_config_raises_value_error(self):
        with self.assertRaises(ValueError):
            create_publisher({
                "PUBLISH_BACKEND": "kafka",
            })

    def test_missing_pubsub_config_raises_value_error(self):
        with self.assertRaises(ValueError):
            create_publisher({
                "PUBLISH_BACKEND": "pubsub",
                "GCP_PROJECT_ID": "test-project",
            })

    def test_unsupported_backend_raises_value_error(self):
        with self.assertRaises(ValueError):
            create_publisher({
                "PUBLISH_BACKEND": "rabbitmq",
            })

    @patch("kafka_producer.publisher_factory.KafkaPublisher")
    def test_factory_does_not_call_start(
        self,
        mock_kafka_publisher,
    ):
        mock_instance = Mock()
        mock_kafka_publisher.return_value = mock_instance

        config = {
            "PUBLISH_BACKEND": "kafka",
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "KAFKA_TOPIC": "binance-events",
        }

        create_publisher(config)

        mock_instance.start.assert_not_called()

if __name__ == "__main__":
    unittest.main()