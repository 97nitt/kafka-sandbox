# Kafka Sandbox for Python

This project serves as a playground for exploring features of Kafka using the 
[Confluent Kafka Python](https://docs.confluent.io/current/clients/confluent-kafka-python/index.html) client.

## Setup virtual environment

Create a virtual environment however you see fit, and install dependencies:

    pip install -r requirements.txt

## Producer Clients

To run a simple Kafka producer:

    $ python -m sandbox.kafka.producer

To run an Avro Kafka producer:

    $ python -m sandbox.kafka.avro_producer

## Consumer Clients

To run a simple Kafka consumer:

    $ python -m sandbox.kafka.consumer

To run an Avro Kafka consumer:

    $ python -m sandbox.kafka.avro_consumer
