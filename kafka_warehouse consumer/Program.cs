using Confluent.Kafka;
using System;
using System.Threading;
using Newtonsoft.Json;

namespace warehouse_consumer
{
    class Program
    {
        public static void Main(string[] args)
        {
            // Kafka consumer configuration
            string bootstrapServers = "localhost:9092";
            string topicName = "warehouse_events";
            string groupId = "my-consumer-group";

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(topicName);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(CancellationToken.None);
                            Console.WriteLine($"Received message: {cr.Message.Value}");

                            var eventData = JsonConvert.DeserializeObject<EventData>(cr.Message.Value);
                            Console.WriteLine($"Event type: {eventData.EventType}");
                            Console.WriteLine($"Shipment ID: {eventData.Data.ShipmentId}");
                            Console.WriteLine("Items:");
                            foreach (var item in eventData.Data.Items)
                            {
                                Console.WriteLine($"  Item ID: {item.ItemId}, Quantity: {item.Quantity}");
                            }

                            consumer.Commit(cr);
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occurred: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }
        }
    }

    public class EventData
    {
        public string EventType { get; set; }
        public ShipmentData Data { get; set; }
    }

    public class ShipmentData
    {
        public int ShipmentId { get; set; }
        public Item[] Items { get; set; }
    }

    public class Item
    {
        public int ItemId { get; set; }
        public int Quantity { get; set; }
    }
}


//working code that consumes data from python producer using docker as well
