"""Exercise 5: archive the input data of the Kafka topic "velib-stations"
in a text file (one JSON station per line)."""
from confluent_kafka import Consumer

IN_TOPIC = "velib-stations"
ARCHIVE_FILE = "velib-stations-archive.txt"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "archive-data",
    "auto.offset.reset": "earliest",
})


def main():
    consumer.subscribe([IN_TOPIC])
    count = 0
    try:
        # append mode, so restarting the script does not erase the archive
        with open(ARCHIVE_FILE, "a", encoding="utf-8") as archive:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                archive.write(msg.value().decode("utf-8") + "\n")
                archive.flush()
                count += 1
                if count % 1000 == 0:
                    print(f"{count} records archived in {ARCHIVE_FILE}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
