"""Exercise 1: poll the JCDecaux API every 10 seconds and publish
each station as one message to the Kafka topic "velib-stations"."""
import json
import os
import time
import urllib.request

from confluent_kafka import Producer

API_KEY = os.environ["JCDECAUX_KEY"]  
URL = f"https://api.jcdecaux.com/vls/v1/stations?apiKey={API_KEY}"
TOPIC = "velib-stations"
INTERVAL = 10  # seconds between two API calls

producer = Producer({"bootstrap.servers": "localhost:9092"})

def fetch_stations():
    # """Call the API and return the list of stations.
    with urllib.request.urlopen(URL, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def main():
    while True:
        try:
            stations = fetch_stations()
        except Exception as error:
            print(f"API request failed: {error}")
            time.sleep(INTERVAL)
            continue

        for station in stations:
            # key = city + station number, so each station is identifiable downstream
            key = f"{station['contract_name']}-{station['number']}"
            producer.produce(TOPIC, key=key, value=json.dumps(station))
            producer.poll(0)
        producer.flush()

        print(f"{time.strftime('%H:%M:%S')} sent {len(stations)} stations to {TOPIC}")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()