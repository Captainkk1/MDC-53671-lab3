"""Test helper (not part of the submission): send a scripted sequence of fake
stations to "velib-stations", so that every case of scripts 2-4 is triggered
without waiting for a real station to become empty."""
import json
import time

from confluent_kafka import Producer

TOPIC = "velib-stations"

producer = Producer({"bootstrap.servers": "localhost:9092"})

# (available_bikes, available_bike_stands) sent one after the other, and what
# each step should trigger in the pipeline
SEQUENCE = [
    (2, 8, "first time seen   -> forwarded by script 2, script 3 only records"),
    (2, 8, "no change         -> NOT forwarded by script 2"),
    (1, 9, "change            -> forwarded by script 2"),
    (0, 10, "becomes empty     -> script 3 event + script 4 ALERT"),
    (0, 10, "still empty       -> nothing"),
    (3, 7, "no longer empty   -> script 3 event, no alert"),
]


def fake_station(bikes, stands):
    return {
        "number": 1,
        "contract_name": "TestCity",
        "name": "00001 - TEST STATION",
        "address": "1 Rue du Test",
        "available_bikes": bikes,
        "available_bike_stands": stands,
        "bike_stands": bikes + stands,
        "status": "OPEN",
        "last_update": int(time.time() * 1000),
    }


def main():
    for bikes, stands, expected in SEQUENCE:
        producer.produce(TOPIC, key="TestCity-1",
                         value=json.dumps(fake_station(bikes, stands)))
        producer.flush()
        print(f"sent bikes={bikes}, stands={stands}  expected: {expected}")
        time.sleep(2)


if __name__ == "__main__":
    main()
