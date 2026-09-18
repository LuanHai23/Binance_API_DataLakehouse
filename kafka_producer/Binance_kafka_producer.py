import json
import os
import signal
from functools import partial

import websocket
from dotenv import load_dotenv

from kafka_producer.event_contract import build_agg_trade_event
from kafka_producer.publisher import EventPublisher
from kafka_producer.publisher_factory import create_publisher
from kafka_producer.runtime_control import (
    RuntimeController,
    parse_run_duration_seconds,
)

load_dotenv()

SYMBOLS = ["btcusdt","ethusdt","bnbusdt","solusdt","xrpusdt","adausdt","dogeusdt","shibusdt",]

stream_string = "/".join(
    f"{symbol}@aggTrade"
    for symbol in SYMBOLS
)

BINANCE_SOCKET = (
    f"wss://stream.binance.com:9443/stream?streams={stream_string}"
)

msg_count = 0

def on_message(
    ws,
    message: str,
    *,
    publisher: EventPublisher,
) -> None:
    global msg_count

    try:
        raw_msg = json.loads(message)

        if not isinstance(raw_msg, dict) or "data" not in raw_msg:
            return

        processed_data = build_agg_trade_event(
            raw_msg["data"]
        )

    except (
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as exc:
        print(f"Invalid Binance message: {exc}")
        return

    # Transport errors must not be swallowed.
    publisher.publish(processed_data)

    msg_count += 1

    if msg_count % 100 == 0:
        print(f"Published {msg_count} messages")


def on_error(ws, error) -> None:
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg) -> None:
    print(
        "WebSocket closed. "
        f"status={close_status_code}, message={close_msg}"
    )


def on_open(
    ws,
    *,
    runtime: RuntimeController,
) -> None:
    if runtime.stop_requested:
        ws.close()
        return

    print("Connected to Binance WebSocket")
    print(f"Socket: {BINANCE_SOCKET}")


def on_shutdown_signal(
    signal_number,
    frame,
    *,
    runtime: RuntimeController,
) -> None:
    print(f"Received shutdown signal: {signal_number}")
    runtime.request_stop()

def run(
    publisher: EventPublisher,
    runtime: RuntimeController,
) -> None:
    """
    Run Binance WebSocket until RuntimeController requests stop.

    The publisher remains alive across WebSocket reconnects.
    """

    while not runtime.stop_requested:
        ws_app = websocket.WebSocketApp(
            BINANCE_SOCKET,
            on_open=partial(
                on_open,
                runtime=runtime,
            ),
            on_message=partial(
                on_message,
                publisher=publisher,
            ),
            on_error=on_error,
            on_close=on_close,
        )

        runtime.attach_websocket(ws_app)

        # Stop may have been requested between construction and
        # attachment. Do not enter run_forever in that case.
        if runtime.stop_requested:
            runtime.detach_websocket(ws_app)
            break

        try:
            ws_app.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except KeyboardInterrupt:
            raise

        except Exception as exc:
            print(f"WebSocket runtime error: {exc}")

        finally:
            runtime.detach_websocket(ws_app)

        if runtime.stop_requested:
            break

        print("Reconnecting in 5 seconds...")

        if runtime.wait(5):
            break

def main() -> None:
    duration = parse_run_duration_seconds(os.environ)
    runtime = RuntimeController(duration)
    publisher = create_publisher(os.environ)

    previous_sigterm_handler = signal.getsignal(
        signal.SIGTERM
    )

    signal.signal(
        signal.SIGTERM,
        partial(
            on_shutdown_signal,
            runtime=runtime,
        ),
    )

    try:
        runtime.start()
        publisher.start()
        run(publisher, runtime)

    except KeyboardInterrupt:
        print("Shutting down...")

    finally:
        try:
            runtime.close()

        finally:
            try:
                publisher.close()

            finally:
                signal.signal(
                    signal.SIGTERM,
                    previous_sigterm_handler,
                )


if __name__ == "__main__":
    main()