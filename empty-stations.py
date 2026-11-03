"""e3: read stations-status and send a message to empty-stations when a station status change"""
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

# True if the station was empty the last time we saw it
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
            raw_value = msg.value().decode("utf-8")
            station = json.loads(raw_value)
            #judge if it's empty
            is_empty = station["available_bikes"] == 0

            previous = was_empty.get(key)
            was_empty[key] = is_empty
            # status doesn't changed
            if previous is None or previous == is_empty:
                continue

            # boolean
            event = dict(station)
            event["is_empty"] = is_empty
            payload = json.dumps(event)
            producer.produce(OUT_TOPIC, key=key, value=payload)
            producer.poll(0)

            change = "became EMPTY" if is_empty else "is no longer empty"
            address = station["address"]
            city = station["contract_name"]
            print(f"{key} {change} ({address}, {city})")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
