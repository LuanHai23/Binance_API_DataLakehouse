from typing import Mapping

from kafka_producer.kafka_publisher import KafkaPublisher
from kafka_producer.pubsub_publisher import PubSubPublisher
from kafka_producer.publisher import EventPublisher


def _required_config(config: Mapping[str, str],key: str,) -> str:
    value = config.get(key)

    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Required configuration {key!r} must be a non-empty string")

    return value.strip()


def create_publisher(config: Mapping[str, str],) -> EventPublisher:
    backend = config.get("PUBLISH_BACKEND", "kafka")

    if not isinstance(backend, str):
        raise ValueError("PUBLISH_BACKEND must be a string")

    backend = backend.strip().lower()

    if backend == "kafka":
        bootstrap_servers = _required_config(
            config,
            "KAFKA_BOOTSTRAP_SERVERS",
        )

        topic = _required_config(
            config,
            "KAFKA_TOPIC",
        )

        return KafkaPublisher(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
        )

    if backend == "pubsub":
        project_id = _required_config(
            config,
            "GCP_PROJECT_ID",
        )

        topic_id = _required_config(
            config,
            "PUBSUB_TOPIC_ID",
        )

        return PubSubPublisher(
            project_id=project_id,
            topic_id=topic_id,
        )

    raise ValueError(
        f"Unsupported PUBLISH_BACKEND: {backend!r}"
    )