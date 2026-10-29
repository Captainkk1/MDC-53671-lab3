"""Exercise 4: listen to the Kafka topic "empty-stations" and print a message
in the console as soon as a station becomes empty (with its address and city)."""
import json

from confluent_kafka import Consumer

IN_TOPIC = "empty-stations"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "alert-empty-stations",
    "auto.offset.reset": "earliest",
})


def main():
    consumer.subscribe([IN_TOPIC])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            station = json.loads(msg.value().decode("utf-8"))
            # "empty-stations" also carries "no longer empty" events: skip them
            if not station["is_empty"]:
                continue

            # some stations (e.g. in Lyon) have an empty address: use the name instead
            address = station["address"] or station["name"]
            print(f"ALERT: station empty -> address: {address}, "
                  f"city: {station['contract_name']}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
