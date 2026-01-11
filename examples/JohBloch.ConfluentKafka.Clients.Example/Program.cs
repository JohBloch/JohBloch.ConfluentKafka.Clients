using System.Text.Json;
using JohBloch.ConfluentKafka.Clients;
using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// 1. Setup DI and Configuration
var services = new ServiceCollection();

services.AddLogging(builder => 
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

services.AddKafkaClients(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.SchemaRegistryUrl = "http://localhost:8081";
    
    // Sample Producer Config
    options.Producers.Add("default", new KafkaProducerOptions 
    { 
        Topic = "example-topic" 
    });
    
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "acks", "all" },
        { "enable.idempotence", "true" }
    };
    
    // Sample Consumer Config
    options.Consumer.GroupId = "example-consumer-group";
    options.Consumer.Topic = "example-topic";
    options.Consumer.AutoOffsetReset = "earliest";
});

var serviceProvider = services.BuildServiceProvider();

// 2. Get Clients
var producer = serviceProvider.GetRequiredService<IKafkaProducerClient>();
var consumer = serviceProvider.GetRequiredService<IKafkaConsumerClient>();

Console.WriteLine("Kafka Client Example Started");
Console.WriteLine("----------------------------");

try
{
    // 3. Define a message
    var message = new TestMessage 
    { 
        Id = Guid.NewGuid().ToString(), 
        Content = "Hello Kafka!", 
        Timestamp = DateTime.UtcNow 
    };
    
    // 4. Produce Message
    Console.WriteLine($" Producing message: {message.Id}");
    var result = await producer.SendMessageWithSchemaAsync(
        message: message,
        key: message.Id,
        producerKey: "default",
        schemaType: SchemaType.Json
    );
    
    Console.WriteLine($" Message produced to: {result.Topic}"); 

    // 5. Consume Message
    Console.WriteLine(" Starting consumer...");
    
    consumer.Subscribe(new[] { "example-topic" });

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
    
    try 
    {
        while (!cts.Token.IsCancellationRequested)
        {
            var consumedMsg = await consumer.ConsumeAsync<TestMessage>(cts.Token);
            
            if (consumedMsg != null)
            {
                Console.WriteLine($" Consumed message: {consumedMsg.Message.Value.Content} (ID: {consumedMsg.Message.Value.Id})");
                consumer.Commit(consumedMsg);
                
                if (consumedMsg.Message.Value.Id == message.Id)
                {
                    break;
                }
            }
            else
            {
                 // Small delay if null returned
                 await Task.Delay(100, cts.Token);
            }
        }
    }
    catch (OperationCanceledException)
    {
        Console.WriteLine(" Consumer timed out (no message received).");
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
}

public class TestMessage
{
    public string Id { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
}
