import math
import threading
from typing import Any, Mapping

from google.cloud import pubsub_v1

from kafka_producer.publisher import serialize_event


class PubSubPublisher:
    def __init__(
        self,
        *,
        project_id: str,
        topic_id: str,
        client=None,
        publish_timeout_seconds: float = 30.0,
    ) -> None:
        if not isinstance(project_id, str) or not project_id.strip():
            raise ValueError(
                "project_id must be a non-empty string"
            )

        if not isinstance(topic_id, str) or not topic_id.strip():
            raise ValueError(
                "topic_id must be a non-empty string"
            )

        if (
            isinstance(publish_timeout_seconds, bool)
            or not isinstance(publish_timeout_seconds, (int, float))
            or not math.isfinite(publish_timeout_seconds)
            or publish_timeout_seconds <= 0
        ):
            raise ValueError(
                "publish_timeout_seconds must be a finite number "
                "greater than 0"
            )

        self.project_id = project_id.strip()
        self.topic_id = topic_id.strip()
        self.publish_timeout_seconds = float(
            publish_timeout_seconds
        )

        # Dependency injection keeps tests offline.
        self.client = (
            client
            if client is not None
            else pubsub_v1.PublisherClient()
        )

        self.topic_path = self.client.topic_path(
            self.project_id,
            self.topic_id,
        )

        # Async publisher lifecycle state.
        self._pending_futures = set()
        self._lock = threading.Lock()
        self._publish_error = None
        self._closed = False

    def start(self) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError(
                    "Pub/Sub publisher is closed"
                )

    def publish(self, event: Mapping[str, Any]) -> None:
        with self._lock:
            if self._closed:
                raise RuntimeError(
                    "Pub/Sub publisher is closed"
                )

            # Also fail fast after a previous async publish failure.
            if self._publish_error is not None:
                raise RuntimeError(
                    f"Previous Pub/Sub publish failed: "
                    f"{self._publish_error}"
                )

        event_id = event.get("event_id")
        symbol = event.get("symbol")
        schema_version = event.get("schema_version")

        if not isinstance(event_id, str) or not event_id.strip():
            raise ValueError(
                "event['event_id'] must be a non-empty string"
            )

        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError(
                "event['symbol'] must be a non-empty string"
            )

        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version <= 0
        ):
            raise ValueError(
                "event['schema_version'] must be a positive integer"
            )

        event_id = event_id.strip()
        symbol = symbol.strip()

        payload = serialize_event(event)

        future = self.client.publish(
            self.topic_path,
            payload,
            event_id=event_id,
            symbol=symbol,
            schema_version=str(schema_version),
        )

        with self._lock:
            self._pending_futures.add(future)

        future.add_done_callback(
            self._publish_done_callback
        )

    def _publish_done_callback(self, future) -> None:
        try:
            future.result()

        except Exception as exc:
            with self._lock:
                if self._publish_error is None:
                    self._publish_error = exc

        finally:
            with self._lock:
                self._pending_futures.discard(future)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return

            self._closed = True

        self.client.stop()

        with self._lock:
            pending = list(self._pending_futures)

        first_close_error = None

        for future in pending:
            try:
                future.result(
                    timeout=self.publish_timeout_seconds
                )

            except Exception as exc:
                if first_close_error is None:
                    first_close_error = exc

            finally:
                with self._lock:
                    self._pending_futures.discard(future)

        with self._lock:
            publish_error = self._publish_error

        if publish_error is not None:
            raise RuntimeError(
                f"Pub/Sub publish failed: {publish_error}"
            )

        if first_close_error is not None:
            raise RuntimeError(
                f"Pub/Sub publish did not complete: "
                f"{first_close_error}"
            )