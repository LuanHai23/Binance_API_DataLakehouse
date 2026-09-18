import threading
from typing import Mapping


MAX_RUN_DURATION_SECONDS = 3600


def parse_run_duration_seconds(
    config: Mapping[str, object],
) -> int | None:
    """
    Parse RUN_DURATION_SECONDS from configuration

    Returns None when the setting is not configured
    Raises ValueError when the setting is invalid
    """
    key = "RUN_DURATION_SECONDS"

    if key not in config:
        return None

    value = config[key]

    if not isinstance(value, str):
        raise ValueError(
            "RUN_DURATION_SECONDS must be a string"
        )

    value = value.strip()

    if not value:
        raise ValueError(
            "RUN_DURATION_SECONDS must not be empty"
        )

    try:
        duration = int(value)
    except ValueError as exc:
        raise ValueError(
            "RUN_DURATION_SECONDS must be an integer"
        ) from exc

    if duration <= 0:
        raise ValueError(
            "RUN_DURATION_SECONDS must be greater than 0"
        )

    if duration > MAX_RUN_DURATION_SECONDS:
        raise ValueError(
            f"RUN_DURATION_SECONDS must be <= "
            f"{MAX_RUN_DURATION_SECONDS}"
        )

    return duration


class RuntimeController:
    """
    Coordinates runtime stop requests, optional duration deadlines and the currently active WebSocket
    """

    def __init__(
        self,
        run_duration_seconds: int | None,
    ) -> None:
        if run_duration_seconds is not None:
            if (
                isinstance(run_duration_seconds, bool)
                or not isinstance(run_duration_seconds, int)
                or run_duration_seconds <= 0
                or run_duration_seconds > MAX_RUN_DURATION_SECONDS
            ):
                raise ValueError(
                    "run_duration_seconds must be None or "
                    "a positive integer <= "
                    f"{MAX_RUN_DURATION_SECONDS}"
                )

        self.run_duration_seconds = run_duration_seconds

        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._active_websocket = None
        self._timer = None

    @property
    def stop_requested(self) -> bool:
        return self._stop_event.is_set()

    def start(self) -> None:
        with self._lock:
            if self._timer is not None:
                return

            if self._stop_event.is_set():
                return

            if self.run_duration_seconds is None:
                return

            timer = threading.Timer(
                self.run_duration_seconds,
                self.request_stop,
            )
            timer.daemon = True

            self._timer = timer
            timer.start()

    def attach_websocket(self, websocket_app) -> None:
        should_close = False

        with self._lock:
            if self._stop_event.is_set():
                should_close = True
            else:
                self._active_websocket = websocket_app

        if should_close:
            websocket_app.close()

    def detach_websocket(self, websocket_app) -> None:
        with self._lock:
            if self._active_websocket is websocket_app:
                self._active_websocket = None

    def request_stop(self) -> None:
        websocket_to_close = None

        with self._lock:
            if self._stop_event.is_set():
                return

            self._stop_event.set()
            websocket_to_close = self._active_websocket
            self._active_websocket = None

        if websocket_to_close is not None:
            websocket_to_close.close()

    def wait(self, timeout_seconds: float) -> bool:
        return self._stop_event.wait(timeout_seconds)

    def close(self) -> None:
        with self._lock:
            timer = self._timer
            self._timer = None

        if timer is not None:
            timer.cancel()

        self.request_stop()