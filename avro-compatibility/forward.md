# Forward Compatibility

Forward compatibility means that consumers with older versions of a schema will be able to read data produced with the latest version of that schema.

## Allowed Changes

The following schema changes are allowed with forward compatibility:

- Only optional fields may be deleted
- Fields may be added (they do not need to be optional)

Schema changes should be rolled out by updating _Producers_ first, then _Consumers_.

## Demo

Use [Docker Compose](https://docs.docker.com/compose) to stand up a single-node Kafka cluster and a Confluent Schema Registry. Set the compatibility strategy for the registry using the `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL` environment variable.
```
$ SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=forward docker-compose -f ../docker-compose.yaml up -d
```

Given the following schemas:

- [v1](schemas/forward/v1.avsc): defines a schema with two fields, `field1` and optional `field2`
- [v2](schemas/forward/v2.avsc): removes `field2` and adds a required `field3`

Start a `console-producer` using `v1` and send some messages:
```
$ console-producer --avro --schema schemas/forward/v1.avsc --topic forward
Starting producer (brokers=localhost:9092, topic=forward).
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
$ console-producer --avro --schema schemas/forward/v2.avsc --topic forward
Starting producer (brokers=localhost:9092, topic=forward).
Enter message text below. Press enter/return to send message, Ctrl-C to exit.
{"field1":"one", "field3":"three"}
{"field1":"uno", "field3":"tres"}
^C
Total messages sent: 2
Total messages delivered: 2
Total messages failed: 0
```

Start a `console-consumer` using `v1` and verify that all messages can be read:
```
% console-consumer --avro --schema schemas/forward/v1.avsc --topic forward --from-earliest
Starting consumer (brokers=localhost:9092, topic=forward, group=console-16fd2452-cf03-49ed-819b-3c1a91f2e83f).
Use Ctrl-C to exit.
key=None, value={'field1': 'one', 'field2': 'two'}, timestamp=1606095205370, partition=0, offset=0
key=None, value={'field1': 'uno', 'field2': 'dos'}, timestamp=1606095212518, partition=0, offset=1
key=None, value={'field1': 'one', 'field2': None}, timestamp=1606095268186, partition=0, offset=2
key=None, value={'field1': 'uno', 'field2': None}, timestamp=1606095275262, partition=0, offset=3
^C
Total messages received: 4
```
Notice that for the last two messages that were serialized using `v2`, the consumer has ignored `field3` and added a default `null` value for `field2`.

Start a `console-consumer` using `v2` and verify that the `v1` messages _cannot_ be read:
```
% console-consumer --avro --schema schemas/forward/v2.avsc --topic forward --from-earliest
Starting consumer (brokers=localhost:9092, topic=forward, group=console-732cfbd6-9583-4b80-a759-796f3bd5ae54).
Use Ctrl-C to exit.
Traceback (most recent call last):
  File "/Users/corey/Dev/97nitt/kafka-sandbox/python/.venv/lib/python3.8/site-packages/confluent_kafka/deserializing_consumer.py", line 137, in poll
    value = self._value_deserializer(value, ctx)
  File "/Users/corey/Dev/97nitt/kafka-sandbox/python/.venv/lib/python3.8/site-packages/confluent_kafka/schema_registry/avro.py", line 321, in __call__
    obj_dict = schemaless_reader(payload,
  File "fastavro/_read.pyx", line 958, in fastavro._read.schemaless_reader
  File "fastavro/_read.pyx", line 970, in fastavro._read.schemaless_reader
  File "fastavro/_read.pyx", line 653, in fastavro._read._read_data
  File "fastavro/_read.pyx", line 558, in fastavro._read.read_record
fastavro._read_common.SchemaResolutionError: No default value for field3
```
Notice that the first message encountered that was serialized using `v1` has failed because `field3` is defined as a required field without a default value in `v2`, and the serialized data does not contain this field.
