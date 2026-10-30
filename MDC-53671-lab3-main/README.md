# Kafka Lab - Vélib

We watch bike stations (Vélib, Vélô, ...) with Kafka and Python, using the
[JCDecaux API](https://developer.jcdecaux.com). The lab subject is in [Kafka-Lab-Velib.pdf](Kafka-Lab-Velib.pdf).

## How it works

```
API -> velib-stations -> stations-status -> empty-stations -> alert in console
            |
            +-> velib-stations.txt
```

| Script | What it does |
|---|---|
| `ingest-data.py` | Gets all stations from the API every 10 seconds and sends them to `velib-stations`. |
| `stations-activity.py` | Sends a station to `stations-status` only when its bikes or free slots change. |
| `empty-stations.py` | Sends a message to `empty-stations` when a station becomes empty or is not empty anymore. |
| `alert-empty-stations.py` | Prints the address and city of a station that just became empty. |
| `archive-data.py` | Saves the messages of `velib-stations` in a text file. |
| `monitor-kafka.py` | Prints `Topic-name, Partition-id, offset-id, timestamp` for each new message. |

Notes:
- A station is empty when `available_bikes` is 0.
- The last state of each station is kept in a dictionary in memory.
- If a station has no address, the alert shows its name.

## Requirements

- Kafka 4.x (no ZooKeeper) and Java 17
- Python 3.11+ and `pip install confluent-kafka`
- A JCDecaux API key

## Run

1. Start Kafka (format only the first time). Keep the Kafka folder path free of spaces.

```bash
cd ~/kafka
bin/kafka-storage.sh format --standalone -t "$(bin/kafka-storage.sh random-uuid)" -c config/server.properties
bin/kafka-server-start.sh config/server.properties
```

2. Create the topics (new terminal):

```bash
for t in velib-stations stations-status empty-stations; do
  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic $t
done
```

3. Start each consumer in its own terminal:

```bash
python monitor-kafka.py
python stations-activity.py
python empty-stations.py
python alert-empty-stations.py
python archive-data.py
```

4. Start the ingestion:

```bash
export JCDECAUX_KEY="your-api-key"
python ingest-data.py
```

## Test

Empty stations are rare, so `testing/fake-stations.py` sends a fake station
with bikes 2, 2, 1, 0, 0, 3:

```bash
python testing/fake-stations.py
```

You should see:
- `stations-activity.py`: 3 changes (the repeated values are ignored)
- `empty-stations.py`: one "became EMPTY" and one "is no longer empty"
- `alert-empty-stations.py`: one alert for `1 Rue du Test, TestCity`
