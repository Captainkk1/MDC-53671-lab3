"""e4: print the result from ex3"""
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
            # we only print the message when station isn't empty anymore
            if not station["is_empty"]:
                continue
            
            address = station["address"] or station["name"]
            print(f"Notice : station empty -> address: {address}, "
                  f"station's city: {station['contract_name']}")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
