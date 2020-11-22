from confluent_kafka import DeserializingConsumer, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from typing import Callable
import logging

logger = logging.getLogger(__name__)

class Consumer:

    def __init__(self,
                 bootstrap_servers: str,
                 topic: str,
                 group: str,
                 callback: Callable[[Message], None],
                 value_deserializer=None,
                 poll_timeout: float = 1.0,
                 config=None):

        consumer_config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group,
            "value.deserializer": value_deserializer
        }
        if config:
            consumer_config.update(config)

        self.consumer = DeserializingConsumer(consumer_config)
        self.topic = topic
        self.callback = callback
        self.poll_timeout = poll_timeout

    def start(self):
        logger.info("Starting Kafka consumer")
        self.consumer.subscribe([self.topic])

        while True:
            message = self.consumer.poll(self.poll_timeout)

            if message is None:
                continue

            if message.error():
                print(f"Consumer error: {message.error()}")
                continue

            self.callback(message)

    def close(self):
        logger.info("Closing Kafka consumer")
        self.consumer.close()

class AvroConsumer(Consumer):

    def __init__(self,
                 bootstrap_servers: str,
                 topic: str,
                 group: str,
                 callback: Callable[[Message], None],
                 schema_registry_url,
                 schema,
                 poll_timeout: float = 1.0,
                 config=None):

        super().__init__(
            bootstrap_servers,
            topic,
            group,
            callback,
            AvroDeserializer(schema, SchemaRegistryClient({"url": schema_registry_url})),
            poll_timeout,
            config)
