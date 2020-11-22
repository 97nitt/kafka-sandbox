from confluent_kafka import KafkaError, Message, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import json
import logging

logger = logging.getLogger(__name__)

def default_callback(error: KafkaError, message: Message):
    if error is not None:
        logger.error(f"Message delivery failed: {error}")
    else:
        logger.debug(f"Message successfully delivered: "
              f"topic={message.topic()}, "
              f"partition={message.partition()}, "
              f"offset={message.offset()}, "
              f"timestamp={message.timestamp()[1]}, "
              f"key={message.key()}, "
              f"value={message.value()}")

class Producer:

    def __init__(self,
                 bootstrap_servers: str,
                 topic: str,
                 value_serializer=None,
                 config=None):

        producer_config = {
            "bootstrap.servers": bootstrap_servers,
            "value.serializer": value_serializer
        }
        if config:
            producer_config.update(config)

        self.producer = SerializingProducer(producer_config)
        self.topic = topic

    def send(self, key=None, value=None, on_delivery=default_callback):
        self.producer.produce(self.topic, key=key, value=value, on_delivery=on_delivery)
        self.producer.flush()

class AvroProducer(Producer):

    def __init__(self,
                 bootstrap_servers: str,
                 topic: str,
                 schema_registry_url: str,
                 schema: str,
                 config=None):

        super().__init__(
            bootstrap_servers,
            topic,
            AvroSerializer(schema, SchemaRegistryClient({"url": schema_registry_url})),
            config)

    def send(self, key=None, value=None, on_delivery=default_callback):
        if isinstance(value, str):
            super().send(key, json.loads(value), on_delivery)
        else:
            super().send(key, value, on_delivery)
