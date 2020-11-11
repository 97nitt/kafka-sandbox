using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Sandbox.Kafka
{
    class Producer
    {
        public const string Topic = "test-topic";

        public static async Task Main(string[] args)
        {
            var config = new ProducerConfig 
            { 
                BootstrapServers = "localhost:9092" 
            };

            // If serializers are not specified, default serializers from
            // `Confluent.Kafka.Serializers` will be automatically used where
            // available. Note: by default strings are encoded as UTF8.
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var message = new Message<Null, string>
                    {
                        Value = "test"
                    };

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
