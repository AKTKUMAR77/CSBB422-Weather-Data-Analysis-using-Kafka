import csv
import json
import time
from kafka import KafkaProducer

print("Starting Kafka Producer...")
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'weather_stream'

with open('indian_weather_master.csv', 'r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        payload = {
            "city": row['location_name'],
            "state": row['region'],
            "temp_c": float(row['temperature_celsius']),
            "humidity": float(row['humidity']),
            "pm25": float(row['air_quality_PM2.5']) if row['air_quality_PM2.5'] else 0.0
        }
        producer.send(TOPIC_NAME, value=payload)
        print(f"[Streaming] City: {payload['city']} | Temp: {payload['temp_c']}°C | PM2.5: {payload['pm25']}")
        time.sleep(0.01)