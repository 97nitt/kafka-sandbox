from confluent_kafka import Consumer

topic = "test-topic"

# create consumer
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "test-consumer-group",
    "auto.offset.reset": "earliest"
})

print(f"Kafka consumer subscribing to topic: {topic}")
consumer.subscribe([topic])

while True:
    # poll for next message
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    print(f"Consumed message: "
          f"topic={msg.topic()}, "
          f"partition={msg.partition()}, "
          f"offset={msg.offset()}, "
          f"timestamp={msg.timestamp()[1]}, "
          f"key={msg.key()}, "
          f"value={msg.value()}")

print("Closing Kafka consumer")
consumer.close()
