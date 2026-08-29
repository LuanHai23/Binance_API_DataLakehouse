import json
from typing import Any, Mapping, Protocol


class EventPublisher(Protocol):
    """
    Interface for publishing canonical events.

    Implementations may publish events to Kafka, Pub/Sub,
    or another messaging system.
    """

    def publish(self, event: Mapping[str, Any]) -> None:
        ...

    def close(self) -> None:
        ...


def serialize_event(event: Mapping[str, Any]) -> bytes:

    payload = json.dumps(
        dict(event),
        allow_nan=False,
        separators=(",", ":"),
    )

    return payload.encode("utf-8")