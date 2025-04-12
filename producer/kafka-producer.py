import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'nyc_taxi_rides'

def stream_csv_to_kafka(filepath):
    with open(filepath, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            producer.send(TOPIC_NAME, row)
            print(f"Sent: {row.get('tpep_pickup_datetime', 'row')}")
            time.sleep(.5)

    producer.flush()
    producer.close()

stream_csv_to_kafka('C:/Users/daneb/Documents/taxi-data-pipeline/data/sample_2024-12.csv')
