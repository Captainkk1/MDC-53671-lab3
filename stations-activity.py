"""e2: read velib and send to status when its bikes or free slots change."""
import json

from confluent_kafka import Consumer, Producer

IN_TOPIC = "velib-stations"
OUT_TOPIC = "stations-status"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "stations-activity",
    "auto.offset.reset": "earliest", #read from earliest message
})
producer = Producer({"bootstrap.servers": "localhost:9092"})

# (bikes, free slots) 
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
            raw_value = msg.value().decode("utf-8")
            station = json.loads(raw_value)
            bikes = station["available_bikes"]
            free_slots = station["available_bike_stands"]
            status = (bikes, free_slots)

            # see the status changes or not
            previous = last_status.get(key)
            if previous == status:
                continue
            last_status[key] = status

            producer.produce(OUT_TOPIC, key=key, value=json.dumps(station))
            producer.poll(0)

            if previous is not None:
                print(f"{key}: bikes changes: {previous[0]} -> {status[0]}, "
                      f"free slots changes: {previous[1]} -> {status[1]}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
