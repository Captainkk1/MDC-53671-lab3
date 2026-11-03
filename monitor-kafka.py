"""E6: print one line for each new message"""
from datetime import datetime

from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "monitor-kafka",
    "auto.offset.reset": "latest",
    # check every 10 seconds
    "topic.metadata.refresh.interval.ms": 10000,
})


def main():
    consumer.subscribe(["^[^_].*"])
    print("Topic-name, Partition-id, offset-id, timestamp")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            _, timestamp_ms = msg.timestamp()
            timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
            timestamp = timestamp.isoformat(sep=" ", timespec="milliseconds")
            position = f"{msg.topic()}, {msg.partition()}, {msg.offset()}"
            print(f"{position}, {timestamp}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
