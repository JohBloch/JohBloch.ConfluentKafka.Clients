using Confluent.Kafka;
using JohBloch.ConfluentKafka.Clients;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// 1. Setup DI and Configuration
ServiceCollection services = new ServiceCollection();

IConfiguration configuration = LocalSettingsConfiguration.Build();

services.AddLogging(builder => 
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

services.AddSchemaRegistryCache(configuration);

services.AddKafkaClients(options =>
{
    options
        .ApplySchemaRegistrySection(configuration)
        .ApplyKafkaSection(configuration)
        .ApplyConsumerSection(configuration)
        .ApplyProducerSection(configuration);
});

ServiceProvider serviceProvider = services.BuildServiceProvider();

// 2. Get Clients
IKafkaProducerClient producerClient = serviceProvider.GetRequiredService<IKafkaProducerClient>();
IKafkaConsumerClient consumerClient = serviceProvider.GetRequiredService<IKafkaConsumerClient>();

Console.WriteLine("Kafka Client Example Started");
Console.WriteLine("----------------------------");

IReadOnlyList<string> producerKeys = KafkaClientOptionsConfigurationExtensions.GetProducerKeys(configuration);
IReadOnlyList<string> consumerTopics = KafkaClientOptionsConfigurationExtensions.GetConsumerTopics(configuration);

if (consumerTopics.Count > 0)
{
    consumerClient.Subscribe(consumerTopics);
    Console.WriteLine($" Subscribed to topics: {string.Join(", ", consumerTopics)}");
}

try
{
    // 3. Produce one message per configured producer key
    List<string> producedMessageIds = new List<string>(producerKeys.Count);
    foreach (string key in producerKeys)
    {
        TestMessage message = new TestMessage
        {
            Id = Guid.NewGuid().ToString(),
            Content = $"Hello Kafka from producer '{key}'",
            Timestamp = DateTime.UtcNow
        };

        producedMessageIds.Add(message.Id);

        Console.WriteLine($" Producing message: {message.Id} via '{key}'");
        KafkaResult result = await producerClient.SendMessageWithSchemaAsync(
            message: message,
            key: message.Id,
            producerKey: key,
            schemaType: SchemaType.Json
        );

        Console.WriteLine($" Message produced to: {result.Topic}");
    }

    // 5. Consume Message
    Console.WriteLine(" Starting consumer...");

    using CancellationTokenSource cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
    
    try 
    {
        while (!cts.Token.IsCancellationRequested)
        {
            ConsumeResult<string, TestMessage>? consumedMsg = await consumerClient.ConsumeAsync<TestMessage>(cts.Token);
            
            if (consumedMsg != null)
            {
                Console.WriteLine($" Consumed message: {consumedMsg.Message.Value.Content} (ID: {consumedMsg.Message.Value.Id})");
                consumerClient.Commit(consumedMsg);
                
                if (producedMessageIds.Contains(consumedMsg.Message.Value.Id, StringComparer.Ordinal))
                {
                    producedMessageIds.Remove(consumedMsg.Message.Value.Id);
                    if (producedMessageIds.Count == 0)
                    {
                        break;
                    }
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
