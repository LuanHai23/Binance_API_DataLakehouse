from typing import Any, Mapping

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from kafka_producer.publisher import serialize_event


class KafkaPublisher:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        producer=None,
        admin_client=None,
        flush_timeout_seconds: float = 30.0,
    ) -> None:
        # Strict configuration validation.
        if (
            not isinstance(bootstrap_servers, str)
            or not bootstrap_servers.strip()
        ):
            raise ValueError(
                "bootstrap_servers must be a non-empty string"
            )

        if (
            not isinstance(topic, str)
            or not topic.strip()
        ):
            raise ValueError(
                "topic must be a non-empty string"
            )

        if flush_timeout_seconds <= 0:
            raise ValueError(
                "flush_timeout_seconds must be greater than 0"
            )

        self.bootstrap_servers = bootstrap_servers.strip()
        self.topic = topic.strip()
        self.flush_timeout_seconds = flush_timeout_seconds

        # Kafka producer configuration preserved from the
        # existing Binance producer.
        producer_config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": "binance-aggtrade-producer",
            "queue.buffering.max.messages": 1000000,
            "queue.buffering.max.ms": 1000,
            "compression.type": "snappy",
            "acks": "all",
        }

        admin_config = {
            "bootstrap.servers": self.bootstrap_servers,
        }

        # Dependency injection keeps unit tests offline.
        self.producer = (
            producer
            if producer is not None
            else Producer(producer_config)
        )

        self.admin_client = (
            admin_client
            if admin_client is not None
            else AdminClient(admin_config)
        )

        # Delivery error captured asynchronously by Kafka callback.
        self._delivery_error = None

    def start(self) -> None:
        self.ensure_topic()

    def ensure_topic(self) -> None:
        metadata = self.admin_client.list_topics(timeout=10)

        if self.topic in metadata.topics:
            return

        new_topic = NewTopic(
            self.topic,
            num_partitions=1,
            replication_factor=1,
        )

        futures = self.admin_client.create_topics(
            [new_topic]
        )

        future = futures[self.topic]

        # Let topic creation errors propagate.
        future.result()

    def _delivery_callback(self, err, msg) -> None:
        """
        Store Kafka delivery failures for handling during close().
        """
        if err is not None:
            self._delivery_error = err

    def publish(self, event: Mapping[str, Any]) -> None:
        symbol = event.get("symbol")

        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError(
                "event['symbol'] must be a non-empty string"
            )

        payload = serialize_event(event)
        key = symbol.strip().encode("utf-8")

        self.producer.produce(
            topic=self.topic,
            key=key,
            value=payload,
            callback=self._delivery_callback,
        )

        # Give librdkafka an opportunity to invoke delivery callbacks
        # without blocking the publisher.
        self.producer.poll(0)

    def close(self) -> None:
        remaining = self.producer.flush(
            self.flush_timeout_seconds
        )

        if remaining != 0:
            raise RuntimeError(
                f"{remaining} Kafka message(s) "
                "were not delivered before timeout"
            )

        if self._delivery_error is not None:
            raise RuntimeError(
                f"Kafka delivery failed: {self._delivery_error}"
            )