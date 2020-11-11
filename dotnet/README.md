# Kafka Sandbox for .Net

This project serves as a playground for exporing features of Kafka using .Net.

## Producer Clients

To run a simple Kafka producer:

    $ dotnet clean
    $ dotnet build /p:StartupObject=Sandbox.Kafka.Producer
    $ dotnet run

To run a Generic Avro Kafka producer:

    $ dotnet clean
    $ dotnet build /p:StartupObject=Sandbox.Kafka.Avro.GenericAvroProducer
    $ dotnet run

## Consumer Clients

To run a simple Kafka consumer:

    $ dotnet clean
    $ dotnet build /p:StartupObject=Sandbox.Kafka.Consumer
    $ dotnet run

To run a Generic Avro Kafka consumer:

    $ dotnet clean
    $ dotnet build /p:StartupObject=Sandbox.Kafka.Avro.GenericAvroConsumer
    $ dotnet run
