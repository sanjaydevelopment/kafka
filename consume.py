import json
import requests
from confluent_kafka import Producer

# Define the REST API URL
api_url = "https://randomuser.me/api/?results=1"

# Kafka producer configuration
kafka_config = {
    "bootstrap.servers": "localhost:9092", 
    "client.id": "python-producer"
}

# Create a Kafka producer instance
producer = Producer(kafka_config)

def fetch_data_from_api():
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Failed to fetch data from API: {e}")
        return None

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    data = fetch_data_from_api()
    if data:
        # Serialize the data as JSON and encode it to bytes
        serialized_data = json.dumps(data).encode('utf-8')

        # Produce the serialized data to a Kafka topic
        producer.produce("my_topic", key=None, value=serialized_data, callback=delivery_report)
        
        # Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush()
