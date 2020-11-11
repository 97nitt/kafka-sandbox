using System;
using System.Threading;
using Confluent.Kafka;

namespace Sandbox.Kafka
{
    class Consumer
    {
        public static void Main(string[] args)
        {
            var conf = new ConsumerConfig
            { 
                GroupId = "test-consumer-group",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(conf).Build())
            {
                Console.WriteLine($"Kafka consumer subscribing to topic: {Producer.Topic}");
                consumer.Subscribe(Producer.Topic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    Console.WriteLine("Cancelling Kafka consumer polling");
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