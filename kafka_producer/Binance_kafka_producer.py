import json
import os
import time
from functools import partial

import websocket
from dotenv import load_dotenv

from kafka_producer.event_contract import build_agg_trade_event
from kafka_producer.kafka_publisher import KafkaPublisher
from kafka_producer.publisher import EventPublisher


load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

SYMBOLS = ["btcusdt","ethusdt","bnbusdt","solusdt","xrpusdt","adausdt","dogeusdt","shibusdt",
]

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

    except (json.JSONDecodeError, TypeError, ValueError) as exc:
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


def on_open(ws) -> None:
    print("Connected to Binance WebSocket")
    print(f"Socket: {BINANCE_SOCKET}")

def run(publisher: EventPublisher) -> None:
    while True:
        ws_app = websocket.WebSocketApp(
            BINANCE_SOCKET,
            on_open=on_open,
            on_message=partial(
                on_message,
                publisher=publisher,
            ),
            on_error=on_error,
            on_close=on_close,
        )

        try:
            ws_app.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except KeyboardInterrupt:
            raise

        except Exception as exc:
            print(f"WebSocket runtime error: {exc}")

        print("Reconnecting in 5 seconds...")
        time.sleep(5)

def main() -> None:
    if (
        not isinstance(KAFKA_BOOTSTRAP_SERVERS, str)
        or not KAFKA_BOOTSTRAP_SERVERS.strip()
    ):
        raise ValueError(
            "KAFKA_BOOTSTRAP_SERVERS is not configured"
        )

    if (
        not isinstance(KAFKA_TOPIC, str)
        or not KAFKA_TOPIC.strip()
    ):
        raise ValueError(
            "KAFKA_TOPIC is not configured"
        )

    publisher = KafkaPublisher(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
    )

    try:
        publisher.ensure_topic()
        run(publisher)

    except KeyboardInterrupt:
        print("Shutting down...")

    finally:
        publisher.close()


if __name__ == "__main__":
    main()