# Backward Compatibility

Backward compatibility means that consumers with the latest schema will be able to read data produced with older versions of that schema.

## Allowed Changes

The following schema changes are allowed with backward compatibility:

- Any field may be deleted (does not need to be optional)
- Only optional fields may be added

Schema changes should be rolled out by updating _Consumers_ first, then _Producers_.

## Demo

Use [Docker Compose](https://docs.docker.com/compose) to stand up a single-node Kafka cluster and a Confluent Schema Registry. The default compatibility strategy for the registry is _backward_.
```
$ docker-compose -f ../docker-compose.yaml up -d
```

Given the following schemas:

- [v1](schemas/backward/v1.avsc): defines a schema with two fields, `field1` and `field2`
- [v2](schemas/backward/v2.avsc): removes `field1` and adds an optional `field3`

Start a `console-producer` using `v1` and send some messages:
```
$ console-producer --avro --schema schemas/backward/v1.avsc --topic backward
Starting producer (brokers=localhost:9092, topic=backward).
Enter message text below. Press enter/return to send message, Ctrl-C to exit.
{"field1":"one", "field2":"two"}
{"field1":"uno", "field2":"dos"}
^C
Total messages sent: 2
Total messages delivered: 2
Total messages failed: 0
```

Start a `console-producer` using `v2` and send some messages:
```
$ console-producer --avro --schema schemas/backward/v2.avsc --topic backward
Starting producer (brokers=localhost:9092, topic=backward).
Enter message text below. Press enter/return to send message, Ctrl-C to exit.
{"field2":"two", "field3":"three"}
{"field2":"dos", "field3":null}
^C
Total messages sent: 2
Total messages delivered: 2
Total messages failed: 0
```

Start a `console-consumer` using `v1` and verify that the `v2` messages _cannot_ be read:
```
% console-consumer --avro --schema schemas/backward/v1.avsc --topic backward --from-earliest
Starting consumer (brokers=localhost:9092, topic=backward, group=console-9f7f18e3-c3bc-43c6-83ee-de2216c31c54).
Use Ctrl-C to exit.
key=None, value={'field1': 'one', 'field2': 'two'}, timestamp=1606092809298, partition=0, offset=0
key=None, value={'field1': 'uno', 'field2': 'dos'}, timestamp=1606092825707, partition=0, offset=1
Traceback (most recent call last):
  File "/Users/corey/Dev/97nitt/kafka-sandbox/python/.venv/lib/python3.8/site-packages/confluent_kafka/deserializing_consumer.py", line 137, in poll
    value = self._value_deserializer(value, ctx)
  File "/Users/corey/Dev/97nitt/kafka-sandbox/python/.venv/lib/python3.8/site-packages/confluent_kafka/schema_registry/avro.py", line 321, in __call__
    obj_dict = schemaless_reader(payload,
  File "fastavro/_read.pyx", line 958, in fastavro._read.schemaless_reader
  File "fastavro/_read.pyx", line 970, in fastavro._read.schemaless_reader
  File "fastavro/_read.pyx", line 653, in fastavro._read._read_data
  File "fastavro/_read.pyx", line 558, in fastavro._read.read_record
fastavro._read_common.SchemaResolutionError: No default value for field1
```
Notice that the first message encountered that was serialized using `v2` has failed because `field1` is defined as a required field without a default value in `v1`, and the serialized data does not contain this field.

Start a `console-consumer` using `v2` and verify that all messages can be read:
```
% console-consumer --avro --schema schemas/backward/v2.avsc --topic backward --from-earliest
Starting consumer (brokers=localhost:9092, topic=backward, group=console-2cefbda8-08d8-4a21-a7c0-c99b8ca3d260).
Use Ctrl-C to exit.
key=None, value={'field2': 'two', 'field3': None}, timestamp=1606092809298, partition=0, offset=0
key=None, value={'field2': 'dos', 'field3': None}, timestamp=1606092825707, partition=0, offset=1
key=None, value={'field2': 'two', 'field3': 'three'}, timestamp=1606092962531, partition=0, offset=2
key=None, value={'field2': 'dos', 'field3': None}, timestamp=1606092990119, partition=0, offset=3
^C
Total messages received: 4
```
Notice that for the first two messages that were serialized using `v1`, the consumer has ignored `field1` and added a default `null` value for `field3`.
