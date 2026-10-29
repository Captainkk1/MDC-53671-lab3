"""Exercise 6: monitor all the Kafka topics and print, for every new record,
the status of the topic in the format:
Topic-name, Partition-id, offset-id, timestamp"""
from datetime import datetime

from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "monitor-kafka",
    "auto.offset.reset": "latest",
    # look for newly created topics every 10 s (default is 5 min)
    "topic.metadata.refresh.interval.ms": 10000,
})


def main():
    # a pattern starting with "^" is a regex: every topic except the internal
    # ones such as "__consumer_offsets"
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
            print(f"{msg.topic()}, {msg.partition()}, {msg.offset()}, {timestamp}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
