# Kafka Sandbox for Java

This project serves as a playground for exploring features of Kafka using Java.

## Areas of Exploration

- a [Producer](src/main/java/sandbox/kafka/producer/Producer.java) class that wraps a Kafka producer
  client and provides a simplified API for sending messages to Kafka

- a [Consumer](src/main/java/sandbox/kafka/consumer/Consumer.java) class that wraps a Kafka consumer
  client and provides a simplified API for reading messages from Kafka
  
- custom [Avro serializers](src/main/java/sandbox/kafka/serde/avro) that make it easy to serialize a
  POJO to Avro-encoded bytes, and deserialize Avro-encoded bytes to a POJO, using a Confluent schema
  registry and Avro's built-in support for generating schemas using reflection
  
- simple [Spring](src/main/java/sandbox/kafka/spring) application to demonstrate use of [spring-kafka](https://spring.io/projects/spring-kafka)

Each of the above have associated unit tests to demonstrate and validate usage.

## Integration Tests

There is a small suite of integration tests that run against a containerized Kafka infrastructure
using [Testcontainers](https://www.testcontainers.org).

- [SchemaSerdeTests](src/integrationTest/java/sandbox/kafka/test/integration/SchemaSerdeTests.java)
  demonstrates sending and receiving a schema-based message using different serialization options
  (JSON, Avro, Protocol Buffers)
