"""Exercise 2: listen to the Kafka topic "velib-stations" and, whenever the
status of a station (number of bikes / free slots) changes, send the station
details to the topic "stations-status"."""
import json

from confluent_kafka import Consumer, Producer

IN_TOPIC = "velib-stations"
OUT_TOPIC = "stations-status"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "stations-activity",
    "auto.offset.reset": "earliest",
})
producer = Producer({"bootstrap.servers": "localhost:9092"})

# last known (available_bikes, available_bike_stands) for each station key
last_status = {}


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
            status = (station["available_bikes"], station["available_bike_stands"])

            # a station seen for the first time is also forwarded, so that the
            # downstream scripts know its initial state
            if last_status.get(key) == status:
                continue
            previous = last_status.get(key)
            last_status[key] = status

            producer.produce(OUT_TOPIC, key=key, value=json.dumps(station))
            producer.poll(0)

            if previous is not None:
                print(f"{key}: bikes {previous[0]} -> {status[0]}, "
                      f"free slots {previous[1]} -> {status[1]}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
