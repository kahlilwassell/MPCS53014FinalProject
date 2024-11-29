import requests
import json
from kafka import KafkaProducer
import time

# Configuration
CTA_API_KEY = "1cac44e9a4544e79a0c2a0e1cf933925"
API_BASE_URL = "http://lapi.transitchicago.com/api/1.0/"
KAFKA_BROKER = "localhost:9092"  # Update with your Kafka broker address
TRAIN_POSITIONS_TOPIC = "train_positions"
TRAIN_CROWDING_TOPIC = "train_crowding"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_train_positions(route):
    """Fetch train positions for a specific route."""
    url = f"{API_BASE_URL}ttpositions.aspx"
    params = {
        "key": CTA_API_KEY,
        "rt": route,
        "outputType": "JSON"
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching train positions: {response.status_code} - {response.text}")
        return None

def publish_to_kafka(topic, data):
    """Publish data to Kafka."""
    try:
        producer.send(topic, value=data)
        print(f"Data published to {topic}: {data}")
    except Exception as e:
        print(f"Error publishing to Kafka: {e}")

def main():
    """Main producer loop."""
    # Example routes to monitor; extend as needed
    routes = ["red", "blue", "green"]
    
    while True:
        for route in routes:
            data = fetch_train_positions(route)
            if data:
                publish_to_kafka(TRAIN_POSITIONS_TOPIC, data)
        
        # Simulate data fetching interval
        time.sleep(30)  # Adjust as needed for real-time requirements

if __name__ == "__main__":
    main()
