using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Threading.Tasks;

namespace Sandbox.Kafka.Avro
{
    class GenericAvroProducer
    {
        public const string Topic = "avro-topic";

        public static RecordSchema Schema = (RecordSchema) RecordSchema.Parse(@"{
                ""namespace"": ""Sandbox.Kafka.Schema"",
                ""name"": ""TestMessage"",
                ""type"": ""record"",
                ""fields"": [{
                    ""name"": ""foo"",
                    ""type"": ""string""
                }, {
                    ""name"": ""bar"",
                    ""type"": ""string""
                }]
            }");

        public static async Task Main(string[] args)
        {
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "http://localhost:8081"
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092" 
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer = 
                new ProducerBuilder<string, GenericRecord>(producerConfig)
                    .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
                    .Build())
            {
                var record = new GenericRecord(Schema);
                record.Add("foo", "one");
                record.Add("bar", "two");

                var message = new Message<string, GenericRecord>
                {
                    Key = "key",
                    Value = record
                };

                try
                {
                    var result = await producer.ProduceAsync(Topic, message);
                    Console.WriteLine($"Delivered '{result.Value}' to: topic={result.Topic}, partition={result.Partition.Value}, offset={result.Offset.Value}");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }
    }
}
