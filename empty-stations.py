"""Exercise 3: listen to the Kafka topic "stations-status" and send a message
to the topic "empty-stations" as soon as a station becomes empty, or is no
longer empty."""
import json

from confluent_kafka import Consumer, Producer

IN_TOPIC = "stations-status"
OUT_TOPIC = "empty-stations"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "empty-stations",
    "auto.offset.reset": "earliest",
})
producer = Producer({"bootstrap.servers": "localhost:9092"})

# last known emptiness (True / False) for each station key
was_empty = {}


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

            key = msg.key().decode("utf-8")
            station = json.loads(msg.value().decode("utf-8"))
            is_empty = station["available_bikes"] == 0

            previous = was_empty.get(key)
            was_empty[key] = is_empty
            # first time we see the station: we only record its state,
            # there is no transition yet
            if previous is None or previous == is_empty:
                continue

            # the "is_empty" field tells the consumers which transition it is
            event = dict(station, is_empty=is_empty)
            producer.produce(OUT_TOPIC, key=key, value=json.dumps(event))
            producer.poll(0)

            change = "became EMPTY" if is_empty else "is no longer empty"
            print(f"{key} {change} ({station['address']}, {station['contract_name']})")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
