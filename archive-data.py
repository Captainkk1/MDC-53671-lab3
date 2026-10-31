"""e5: save the messages of velib in a text file
(one JSON per line)."""
from confluent_kafka import Consumer

IN_TOPIC = "velib-stations"
ARCHIVE_FILE = "velib-stations.txt"

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "archive-data",
    "auto.offset.reset": "earliest",
})


def main():
    consumer.subscribe([IN_TOPIC])
    count = 0
    try:
        # new content will be added at the end of file
        with open(ARCHIVE_FILE, "a", encoding="utf-8") as archive:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                line = msg.value().decode("utf-8")
                archive.write(line + "\n")
                archive.flush()
                count += 1
                if count % 1000 == 0:
                    print(f"{count} records saved")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
