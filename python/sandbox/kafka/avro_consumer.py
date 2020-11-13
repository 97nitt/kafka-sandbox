from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from sandbox.kafka import avro_schemas

topic = "avro-topic"

# create Avro deserializer
deserializer = AvroDeserializer(avro_schemas.test_message, SchemaRegistryClient({
    "url": "http://localhost:8081"
}))

# create consumer
consumer = DeserializingConsumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "test-consumer-group",
    "auto.offset.reset": "earliest",
    "value.deserializer": deserializer
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
