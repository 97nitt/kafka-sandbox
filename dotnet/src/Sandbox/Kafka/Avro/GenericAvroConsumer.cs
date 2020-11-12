using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Threading;

namespace Sandbox.Kafka.Avro
{
    class GenericAvroConsumer
    {
        public static void Main(string[] args)
        {
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "http://localhost:8081"
            };

            var consumerConfig = new ConsumerConfig
            { 
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer = 
                new ConsumerBuilder<string, GenericRecord>(consumerConfig)
                    .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
                    .Build())
            {
                Console.WriteLine($"Kafka consumer subscribing to topic: {GenericAvroProducer.Topic}");
                consumer.Subscribe(GenericAvroProducer.Topic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    Console.WriteLine("Canceling Kafka consumer polling");
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var result = consumer.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{result.Message.Value}' from: topic={result.Topic}, partition={result.Partition.Value}, offset={result.Offset.Value}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Ensure the consumer leaves the group cleanly and final offsets are committed.
                    Console.WriteLine("Closing Kafka consumer");
                    consumer.Close();
                }
            }
        }
    }
}