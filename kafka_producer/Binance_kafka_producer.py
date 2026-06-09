import json
import os
import time
from datetime import datetime

import websocket
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

load_dotenv()

# Cấu hình của Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

# Danh sách các coin btc, eth, bnb, sol, xrp, ada, doge, shib
SYMBOLS = ['btcusdt', 'ethusdt', 'bnbusdt', 'solusdt', 'xrpusdt', 'adausdt', 'dogeusdt','shibusdt']
# URL Combined 
stream_string = "/".join([f"{symbol}@aggTrade" for symbol in SYMBOLS])

# Dùng endpoint '/stream?streams=' thay vì '/ws/'
BINANCE_SOCKET = f'wss://stream.binance.com:9443/stream?streams={stream_string}'

# Tạo topic nếu chưa tồn tại
def create_topic() -> None:
    admin_client = AdminClient(
        {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
    )

    metadata = admin_client.list_topics(timeout=10)

    if KAFKA_TOPIC in metadata.topics:
        print(f"Kafka topic already exists: {KAFKA_TOPIC}")
        return

    topic_list = [
        NewTopic(
            KAFKA_TOPIC,
            num_partitions=1,
            replication_factor=1
        )
    ]

    futures = admin_client.create_topics(topic_list)

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Kafka topic created: {topic}")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")

#Producer
producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "binance-aggtrade-producer",
    "queue.buffering.max.messages": 1000000,
    "queue.buffering.max.ms": 1000,
    "compression.type": "snappy",
    "acks": "all",
}

producer = Producer(producer_conf)

msg_count = 0

def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"Message delivery failed: {err}")


def on_message(ws, message) -> None:
    global msg_count

    try:
        raw_msg = json.loads(message)

        if "data" not in raw_msg:
            return

        data = raw_msg["data"]

        processed_data = {
            "source": "binance_aggTrade",
            "event_type": data.get("e"),
            "symbol": data.get("s"),
            "trade_id": data.get("a"),
            "price": float(data.get("p")),
            "quantity": float(data.get("q")),
            "first_trade_id": data.get("f"),
            "last_trade_id": data.get("l"),
            "trade_time": data.get("T"),
            "is_buyer_maker": data.get("m"),
            "ingested_at": datetime.utcnow().isoformat(),
        }

        producer.produce(
            topic=KAFKA_TOPIC,
            key=processed_data["symbol"],
            value=json.dumps(processed_data),
            callback=delivery_report,
        )

        # Trigger delivery callbacks
        producer.poll(0)

        msg_count += 1

        if msg_count % 100 == 0:
            print(
                f"Produced {msg_count} messages "
                f"to topic={KAFKA_TOPIC}"
            )

    except BufferError:
        print("Kafka producer queue is full. Flushing...")
        producer.flush()

    except Exception as e:
        print(f"Error processing message: {e}")


def on_error(ws, error) -> None:
    print(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_msg) -> None:
    print(
        f"WebSocket closed. "
        f"status={close_status_code}, message={close_msg}"
    )
    producer.flush()


def on_open(ws) -> None:
    print(f"Connected to Binance WebSocket")
    print(f"Socket: {BINANCE_SOCKET}")
    print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Kafka topic: {KAFKA_TOPIC}")


if __name__ == "__main__":
    print("Starting Binance Kafka Producer...")

    create_topic()

    while True:
        try:
            ws = websocket.WebSocketApp(
                BINANCE_SOCKET,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except KeyboardInterrupt:
            print("Interrupted by user, closing producer...")
            break

        except Exception as e:
            print(f"Error creating WebSocketApp: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)

    print("Flushing producer...")
    producer.flush()
    print("Done.")