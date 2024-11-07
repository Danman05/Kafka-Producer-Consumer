# producer.py

# Generate mock data, that resembles data from a game called Factorio.
# With the generated data it can be sent out to a specific topic through the Kafka cluster

from kafka import KafkaProducer
import json
import time
import random

def generate_mock_data():
    data = {
        "tick": int(time.time()),
        "iron_plate": random.randint(50, 200),
        "copper_plate": random.randint(30, 150),
        "gear_wheel": random.randint(20, 100),
        "electronic_circuit": random.randint(10, 50),
    }
    return data

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic ='factorio-data-v2'

# Send data to Kafka at intervals
try:
    while True:
        data = generate_mock_data()
        producer.send(topic, data)
        print(f"Sent data: {data}")
        time.sleep(2) 
except KeyboardInterrupt:
    print("Stopped data production.")

finally:
    producer.close()
