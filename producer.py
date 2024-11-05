# producer.py

from kafka import KafkaProducer
import json
import time
import random

# Set up Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

topic ='factorio-data-v2'

# Mock data generator
def generate_mock_data():
    data = {
        "tick": int(time.time()),
        "iron_plate": random.randint(50, 200),
        "copper_plate": random.randint(30, 150),
        "gear_wheel": random.randint(20, 100),
        "electronic_circuit": random.randint(10, 50),
    }
    return data

# Send data to Kafka at intervals
try:
    while True:
        data = generate_mock_data()
        # producer.produce('factorio-data-v2', value=json.dumps(data))
        # producer.flush()
        producer.send(topic, data)
        print(f"Sent data: {data}")
        time.sleep(15)  # Adjust as needed for data frequency
except KeyboardInterrupt:
    print("Stopped data production.")

finally:
    producer.close()
