from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:9092'})
topic = 'events-topic'

# Sample events (with event_id)
events = [
    {"event_id": "evt_1", "data": "Event 1"},
    {"event_id": "evt_1", "data": "Event 1 (Duplicate)"},
    {"event_id": "evt_2", "data": "Event 2"},
    {"event_id": "evt_3", "data": "Event 3"}]

for event in events:
    producer.produce(topic, json.dumps(event).encode('utf-8'))
    producer.flush()

print("Produced 3 events to Kafka.")