"""ex1: get the stations data from the API every 10 seconds and send to the topic."""

import json
import os
import time
import urllib.request

from confluent_kafka import Producer

API_KEY = os.environ["JCDECAUX_KEY"]
URL = f"https://api.jcdecaux.com/vls/v1/stations?apiKey={API_KEY}"
TOPIC = "velib-stations"
INTERVAL = 10 

producer = Producer({"bootstrap.servers": "localhost:9092"})

#get data
def fetch_stations():
    with urllib.request.urlopen(URL, timeout=20) as response:
        body = response.read().decode("utf-8")
    return json.loads(body)


def main():
    while True:
        try:
            stations = fetch_stations()
        except Exception as error:
            print(f"API error: {error}")
            time.sleep(INTERVAL)
            continue
#send info per station
        for station in stations:
            # the key is city + station number
            city = station["contract_name"]
            number = station["number"]
            key = f"{city}-{number}"
            payload = json.dumps(station)
            producer.produce(TOPIC, key=key, value=payload)
            producer.poll(0)
        producer.flush()

        now = time.strftime("%H:%M:%S")
        print(f"{now} sent {len(stations)} stations")
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()