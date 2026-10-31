# Kafka Lab - Vélib

## How to start

Open one terminal for each step. Run the Python commands in this folder.

**1. Start Kafka** (keep this terminal open)

Format only the first time.

```powershell
cd C:\kafka
.\bin\windows\kafka-storage.bat format --standalone -t (.\bin\windows\kafka-storage.bat random-uuid) -c config\server.properties
$env:KAFKA_HEAP_OPTS="-Xmx1G -Xms1G"
.\bin\windows\kafka-server-start.bat config\server.properties
```

**2. Create the topics** (new terminal, in the Kafka folder)

```powershell
foreach ($t in "velib-stations","stations-status","empty-stations") {
  .\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic $t
}
```

**3. Start the 5 consumers** (one terminal each)

```
python monitor-kafka.py
python stations-activity.py
python empty-stations.py
python alert-empty-stations.py
python archive-data.py
```

**4. Start the ingestion** (last, new terminal)

Get a free API key at https://developer.jcdecaux.com/#/signup, then:

```powershell
$env:JCDECAUX_KEY="YOUR_KEY"
python ingest-data.py
```

## Test without an API key

Real stations are rarely empty, and not everyone has an API key.
So we made `testing/fake-stations.py`: it sends one fake station
with 2, 2, 1, 0, 0, 3 bikes.

Do steps 1 to 3 and wait about 10 seconds for the consumers to start.
Then, in a new terminal:

```
python testing/fake-stations.py
```

You should see:
- `stations-activity.py`: 3 changes
- `empty-stations.py`: one "became EMPTY" and one "is no longer empty"
- `alert-empty-stations.py`: one alert
