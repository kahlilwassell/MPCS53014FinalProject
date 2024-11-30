import requests
import json
from kafka import KafkaProducer
import time

# Configuration
CTA_API_KEY = "1cac44e9a4544e79a0c2a0e1cf933925"
API_BASE_URL = "http://lapi.transitchicago.com/api/1.0/"
KAFKA_BROKERS = "wn0-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092,wn1-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092"
TRAIN_POSITIONS_TOPIC = "kjwassell_train_positions"
TRAIN_ARRIVALS_TOPIC = "kjwassell_train_arrivals"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# List of all CTA train routes
routes = ["red", "blue", "green", "orange", "brown", "purple", "pink", "yellow"]

# Predefined stations (or fetch dynamically if possible)
stations = ["40380", "40390", "40410"]  # Example station IDs, replace or fetch dynamically


def fetch_train_positions(route):
    """Fetch train positions for a specific route."""
    url = f"{API_BASE_URL}ttpositions.aspx"
    params = {
        "key": CTA_API_KEY,
        "rt": route,
        "outputType": "JSON"
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching train positions for {route}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Exception occurred while fetching train positions for {route}: {e}")
        return None


def fetch_train_arrivals(station_id):
    """Fetch train arrivals for a specific station."""
    url = f"{API_BASE_URL}ttarrivals.aspx"
    params = {
        "key": CTA_API_KEY,
        "stpid": station_id,
        "outputType": "JSON"
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching train arrivals for station {station_id}: {response.status_code} - {response.text}")
            return None
    except Exception as e:
        print(f"Exception occurred while fetching train arrivals for station {station_id}: {e}")
        return None


def publish_to_kafka(topic, data):
    """Publish data to Kafka."""
    try:
        producer.send(topic, value=data)
        print(f"Data published to {topic}: {data}")
    except Exception as e:
        print(f"Error publishing to Kafka: {e}")


def process_train_positions():
    """Fetch and process train positions."""
    for route in routes:
        print(f"Fetching train positions for route: {route}")
        data = fetch_train_positions(route)
        if data:
            relevant_data = data.get('ctatt', {}).get('route', {})
            publish_to_kafka(TRAIN_POSITIONS_TOPIC, relevant_data)
        else:
            print(f"No data available for route: {route}")


def process_train_arrivals():
    """Fetch and process train arrivals."""
    for station_id in stations:
        print(f"Fetching train arrivals for station: {station_id}")
        data = fetch_train_arrivals(station_id)
        if data:
            relevant_data = data.get('ctatt', {}).get('eta', {})
            publish_to_kafka(TRAIN_ARRIVALS_TOPIC, relevant_data)
        else:
            print(f"No data available for station: {station_id}")


def main():
    """Main producer loop."""
    while True:
        # Process train positions
        process_train_positions()

        # Process train arrivals
        process_train_arrivals()

        # Fetch data every 30 seconds
        time.sleep(10)


if __name__ == "__main__":
    main()
