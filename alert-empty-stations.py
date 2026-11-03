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

            raw_value = msg.value().decode("utf-8")
            station = json.loads(raw_value)
            # we only print the message when station is empty 
            if not station["is_empty"]:
                continue
            
            address = station["address"]
            if not address:
                address = station["name"]
            city = station["contract_name"]
            print(f"Notice : station empty -> address: {address}, "
                  f"station's city: {city}")
            
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
