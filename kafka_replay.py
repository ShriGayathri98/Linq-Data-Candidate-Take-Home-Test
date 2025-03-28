from confluent_kafka import Consumer, TopicPartition
import json
import redis
from pydantic import BaseModel, ValidationError

# --- Configurations ---
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'events-topic'
PARTITION = 0
START_OFFSET = 0
END_OFFSET = 3

# --- Redis & Kafka Setup ---
REDIS_CLIENT = redis.Redis(host='localhost', port=6379, db=0)

consumer_conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'event-replay-consumer',
    'enable.auto.commit': False,
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(consumer_conf)

# --- Schema Validation ---
class EventSchema(BaseModel):
    event_id: str
    # Add other event fields...

# --- Replay Logic ---
tp = TopicPartition(TOPIC, PARTITION, START_OFFSET)
consumer.assign([tp])
consumer.seek(tp)

print(f"Starting replay from offset {START_OFFSET} to {END_OFFSET}...\n")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        if msg.offset() > END_OFFSET:
            break

        # Parse & Validate
        try:
            event = json.loads(msg.value().decode('utf-8'))
            validated_event = EventSchema(**event)
        except (json.JSONDecodeError, ValidationError) as e:
            print(f"Invalid event: {e}")
            continue

        # Deduplication (Atomic)
        event_id = validated_event.event_id
        redis_key = f"event:{event_id}"
        if not REDIS_CLIENT.setnx(redis_key, 1):
            print(f"Skipping duplicate: {event_id}")
            continue
        REDIS_CLIENT.expire(redis_key, 3600)

        # Process event
        print(f"Reprocessed offset {msg.offset()}: {event_id}")
        # Add your business logic here

        # Commit offset
        consumer.commit(message=msg, asynchronous=False)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
    REDIS_CLIENT.close()
    print("\nReplay complete.")