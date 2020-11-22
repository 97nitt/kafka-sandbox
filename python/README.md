# Kafka Sandbox for Python

This project serves as a playground for exploring features of Kafka using the 
[Confluent Kafka Python](https://docs.confluent.io/current/clients/confluent-kafka-python/index.html) client.

## Setup virtual environment

It is highly recommended to create a Python virtual environment to isolate the dependencies of this project.
```
$ python -m venv --prompt kafka-sandbox .venv
$ source .venv/bin/activate
$ pip install -e .
```
This will install the `console-producer`:
```
$ console-producer -h                                    master
usage: console-producer [-h] [--brokers BROKERS] --topic TOPIC [--avro] [--schema FILE]
                        [--registry URL]

Kafka console producer.

optional arguments:
  -h, --help         show this help message and exit
  --brokers BROKERS  Kafka Broker URLs (default: localhost:9092)
  --topic TOPIC      Kafka topic (default: None)
  --avro             Serialize messages using Avro (default: False)
  --schema FILE      Path to Avro schema file (default: None)
  --registry URL     Schema Registry URL (default: http://localhost:8081)

```
And the `console-consumer`:
```
$ console-consumer -h                                    master
usage: console-consumer [-h] [--brokers BROKERS] --topic TOPIC [--group GROUP] [--from-earliest]
                        [--avro] [--schema FILE] [--registry URL]

Kafka console consumer.

optional arguments:
  -h, --help         show this help message and exit
  --brokers BROKERS  Kafka Broker URLs (default: localhost:9092)
  --topic TOPIC      Kafka topic (default: None)
  --group GROUP      Kafka consumer group (default: None)
  --from-earliest    If no committed offsets are available, start from earliest (default: False)
  --avro             Serialize messages using Avro (default: False)
  --schema FILE      Path to Avro schema file (default: None)
  --registry URL     Scham Registry URL (default: http://localhost:8081)

```

## Running Kafka locally via Docker

This project contains a [Docker Compose](https://docs.docker.com/compose) file that can be used to run a local 
single-node Kafka cluster and Confluent Schema Registry.
```
$ docker-compose -f ../docker-compose.yaml up -d
```

## Console Producer/Consumer

To start a Kafka producer and send some messages:
```
$ console-producer --topic demo
Starting producer (brokers=localhost:9092, topic=demo).
Enter message text below. Press enter/return to send message, Ctrl-C to exit.
one
two
three
^C
Total messages sent: 3
Total messages delivered: 3
Total messages failed: 0
```

To start a Kafka consumer:
```
$ console-consumer --topic demo --from-earliest
Starting consumer (brokers=localhost:9092, topic=demo, group=console-26aab655-fda6-406e-b5cf-3a622e881d50).
Use Ctrl-C to exit.
key=None, value=b'one', timestamp=1606056091516, partition=0, offset=0
key=None, value=b'two', timestamp=1606056093007, partition=0, offset=1
key=None, value=b'three', timestamp=1606056093820, partition=0, offset=2
^C
Total messages received: 3
```

## Avro Producer/Consumer

To send [Avro](https://avro.apache.org)-encoded messages:
```
$ console-producer --topic demo-avro --avro --schema avro/test_message.avsc
Starting producer (brokers=localhost:9092, topic=demo-avro).
Enter message text below. Press enter/return to send message, Ctrl-C to exit.
{"foo":"one", "bar":"two"}
{"foo":"three", "bar":"four"}
^C
Total messages sent: 2
Total messages delivered: 2
Total messages failed: 0
```

To consume [Avro](https://avro.apache.org)-encoded messages:
```
$ console-consumer --topic demo-avro --from-earliest --avro --schema avro/test_message.avsc
Starting consumer (brokers=localhost:9092, topic=demo-avro, group=console-9b6c97c2-5e5f-454f-962d-640c97d01fbe).
Use Ctrl-C to exit.
key=None, value={'foo': 'one', 'bar': 'two'}, timestamp=1606056488593, partition=0, offset=0
key=None, value={'foo': 'three', 'bar': 'four'}, timestamp=1606056498396, partition=0, offset=1
^C
Total messages received: 2
```
