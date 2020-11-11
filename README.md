# Kafka Sandbox [![Build Status](https://travis-ci.com/97nitt/serde-sandbox.svg?token=9wZj3TUqNCufm9GNxgso&branch=master)](https://travis-ci.com/97nitt/serde-sandbox)

This repo serves as a playground for exploring features of Kafka.

It contains the following subprojects:

- [dotnet](dotnet): demonstrates use of .Net clients
- [java](java): demonstrates use of Java clients

## Dockerized Kafka

This repo includes a [Docker Compose](https://docs.docker.com/compose/) file that will standup containerized Docker infrastructure that can be used for local development and testing.

    docker-compose up -d

This will standup the following containers with ports mapped to your local machine:

| Container       | Host Port | Description                                                                               |
|-----------------|-----------|-------------------------------------------------------------------------------------------|
| zookeeper       |      2181 | Dependency of Kafka, used for cluster coordination                                        |
| kafka           |      9092 | Single-node Kafka cluster                                                                 |
| schema-registry |      8081 | [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) |
