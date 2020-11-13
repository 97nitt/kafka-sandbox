from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from sandbox.kafka import avro_schemas

# create Avro serializer
serializer = AvroSerializer(avro_schemas.test_message, SchemaRegistryClient({
    "url": "http://localhost:8081"
}))

# create producer
producer = SerializingProducer({
    'bootstrap.servers': 'localhost:9092',
    'value.serializer': serializer
})

def callback(err, msg):
    """ Called once for each message produced to indicate delivery success or failure.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message successfully delivered: "
              f"topic={msg.topic()}, "
              f"partition={msg.partition()}, "
              f"offset={msg.offset()}, "
              f"timestamp={msg.timestamp()[1]}, "
              f"key={msg.key()}, "
              f"value={msg.value()}")

# send a message
key = 'key'
value = {
    'foo': 'one',
    'bar': 'two'
}
producer.produce('avro-topic', key=key, value=value, on_delivery=callback)

# wait for all queued messages to be delivered
producer.flush()
