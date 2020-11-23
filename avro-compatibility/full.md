# Full Compatibility

Full compatibility means schemas are both backward _and_ forward compatible: "old" data can be read with newer schemas, and "new" data can be read with older schemas.

This strategy would make sense if you need to read historical data written with older schema versions and you cannot use _backward_ compatibility because you cannot rely on keeping consumer schemas updated.

## Allowed Changes

The following schema changes are allowed with full compatibility:

- Only optional fields may be deleted
- Only optional fields may be added

Schema changes can be rolled out to producers and consumers in any order.

## Demo

Use [Docker Compose](https://docs.docker.com/compose) to stand up a single-node Kafka cluster and a Confluent Schema Registry. Set the compatibility strategy for the registry using the `SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL` environment variable.
```
$ SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=full docker-compose -f ../docker-compose.yaml up -d
```

Given the following schemas:

- [v1](schemas/full/v1.avsc): defines a schema with two fields, `field1` and optional `field2`
- [v2](schemas/full/v2.avsc): removes `field2` and adds a optional `field3`

Start a `console-producer` using `v1` and send some messages:
```
$ console-producer --avro --schema schemas/full/v1.avsc --topic full
Starting producer (brokers=localhost:9092, topic=full).
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
$ console-producer --avro --schema schemas/full/v2.avsc --topic full
Starting producer (brokers=localhost:9092, topic=full).
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
% console-consumer --avro --schema schemas/full/v1.avsc --topic full --from-earliest
Starting consumer (brokers=localhost:9092, topic=full, group=console-cbc6df1d-110c-4c8c-b199-82786c69c547).
Use Ctrl-C to exit.
key=None, value={'field1': 'one', 'field2': 'two'}, timestamp=1606097437396, partition=0, offset=0
key=None, value={'field1': 'uno', 'field2': 'dos'}, timestamp=1606097443492, partition=0, offset=1
key=None, value={'field1': 'one', 'field2': None}, timestamp=1606097504449, partition=0, offset=2
key=None, value={'field1': 'uno', 'field2': None}, timestamp=1606097511090, partition=0, offset=3
^C
Total messages received: 4
```
Notice that for the last two messages that were serialized using `v2`, the consumer has ignored `field3` and added a default `null` value for `field2`.

Start a `console-consumer` using `v2` and verify that all messages can be read:
```
% console-consumer --avro --schema schemas/full/v2.avsc --topic full --from-earliest
Starting consumer (brokers=localhost:9092, topic=full, group=console-983c28eb-82a1-4399-982c-46b5b9e46373).
Use Ctrl-C to exit.
key=None, value={'field1': 'one', 'field3': None}, timestamp=1606097437396, partition=0, offset=0
key=None, value={'field1': 'uno', 'field3': None}, timestamp=1606097443492, partition=0, offset=1
key=None, value={'field1': 'one', 'field3': 'three'}, timestamp=1606097504449, partition=0, offset=2
key=None, value={'field1': 'uno', 'field3': 'tres'}, timestamp=1606097511090, partition=0, offset=3
^C
Total messages received: 4
```
Notice that for the first two messages that were serialized using `v1`, the consumer has ignored `field2` and added a default `null` value for `field3`.
