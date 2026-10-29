# Kafka Lab – Vélib Use Case

Real-time monitoring of bike-sharing stations (Vélib, Vélô Toulouse, …) with
Apache Kafka and Python, using the [JCDecaux API](https://developer.jcdecaux.com).

The subject of the lab is in [Kafka-Lab-Velib.pdf](Kafka-Lab-Velib.pdf).

## Pipeline

```
JCDecaux API ──(1)──> velib-stations ──(2)──> stations-status ──(3)──> empty-stations ──(4)──> console alert
                            │
                            └──(5)──> velib-stations-archive.txt

(6) monitor-kafka.py prints the activity of every topic
```

| # | Script | Reads | Writes | Role |
|---|---|---|---|---|
| 1 | `ingest-data.py` | JCDecaux API | `velib-stations` | Polls the API every 10 s and publishes one message per station (key = `city-stationNumber`). |
| 2 | `stations-activity.py` | `velib-stations` | `stations-status` | Keeps the last (bikes, free slots) of each station and forwards the station only when it changes. |
| 3 | `empty-stations.py` | `stations-status` | `empty-stations` | Sends an event when a station becomes empty or is no longer empty (field `is_empty`). |
| 4 | `alert-empty-stations.py` | `empty-stations` | console | Prints the address and city of each station that just became empty. |
| 5 | `archive-data.py` | `velib-stations` | text file | Appends every record (one JSON per line) to `velib-stations-archive.txt`. |
| 6 | `monitor-kafka.py` | all topics | console | Prints `Topic-name, Partition-id, offset-id, timestamp` for every new record. |

### Design choices

- **Station state is kept in memory** (a dictionary keyed by `city-stationNumber`) in scripts 2 and 3.
- **First observation of a station**: script 2 forwards it, so that script 3 knows the initial
  state; script 3 only records it (there is no transition yet).
- **Empty station** means `available_bikes == 0`.
- **Missing addresses**: some stations (e.g. in Lyon) have an empty `address`; script 4 then
  prints the station `name` instead.
- **Monitoring all topics**: script 6 subscribes with the regex `^[^_].*`, i.e. every topic
  except Kafka internal ones such as `__consumer_offsets`, and refreshes the topic list
  every 10 s so that new topics are picked up.

## Requirements

- Apache Kafka 4.x (KRaft mode, no ZooKeeper) and Java 17
- Python 3.11+ with `confluent-kafka`:

  ```bash
  pip install confluent-kafka
  ```

- A JCDecaux API key ([sign up](https://developer.jcdecaux.com/#/signup))

## How to run

> On macOS/Linux, Kafka's scripts fail with *"Classpath is empty"* if the Kafka folder path
> contains spaces. Put Kafka in a path without spaces (e.g. `~/kafka`).

**1. Start Kafka** (the `format` step is only needed the first time):

```bash
cd ~/kafka
bin/kafka-storage.sh format --standalone -t "$(bin/kafka-storage.sh random-uuid)" -c config/server.properties
bin/kafka-server-start.sh config/server.properties     # keep this terminal open
```

**2. Create the topics** (in another terminal):

```bash
cd ~/kafka
for t in velib-stations stations-status empty-stations; do
  bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic $t
done
```

**3. Start the consumers**, one terminal each, from this folder:

```bash
python monitor-kafka.py
python stations-activity.py
python empty-stations.py
python alert-empty-stations.py
python archive-data.py
```

**4. Start the ingestion:**

```bash
export JCDECAUX_KEY="your-api-key"
python ingest-data.py
```

## Testing

Empty stations are rare, so [testing/fake-stations.py](testing/fake-stations.py) sends a scripted
sequence for a fake station (`TestCity-1`): 2 bikes → 2 (no change) → 1 → 0 → 0 → 3.

```bash
python testing/fake-stations.py
```

Expected result with the consumers of step 3 running:

- `stations-activity.py` prints 3 changes (the repeated values are not forwarded);
- `empty-stations.py` prints one `became EMPTY` and one `is no longer empty`;
- `alert-empty-stations.py` prints exactly one alert:
  `ALERT: station empty -> address: 1 Rue du Test, city: TestCity`.

With the real API, each poll sends about 2,450 stations; after the first poll (where every station
is new), `stations-activity.py` reports a few dozen to a hundred changes every 10 seconds.
