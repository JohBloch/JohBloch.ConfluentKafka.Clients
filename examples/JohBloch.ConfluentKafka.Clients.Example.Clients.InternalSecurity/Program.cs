using Confluent.Kafka;
using JohBloch.ConfluentKafka.Clients;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

const int ConsumeTimeoutSeconds = 15;

// 1. Setup DI and configuration
var services = new ServiceCollection();
IConfiguration configuration = BuildConfiguration();

services.AddLogging(ConfigureLogging);

services.AddKafkaClients(options =>
    options
        .ApplySchemaRegistrySection(configuration)
        .ApplyKafkaSection(configuration)
        .ApplyConsumerSection(configuration)
        .ApplyProducerSection(configuration));

using ServiceProvider serviceProvider = services.BuildServiceProvider();

// 2. Get clients
IKafkaProducerClient producerClient = serviceProvider.GetRequiredService<IKafkaProducerClient>();
IKafkaConsumerClient consumerClient = serviceProvider.GetRequiredService<IKafkaConsumerClient>();

Console.WriteLine("Kafka Client Example Started");
Console.WriteLine("----------------------------");

IReadOnlyList<string> producerKeys = KafkaClientOptionsConfigurationExtensions.GetProducerKeys(configuration);
IReadOnlyList<string> consumerTopics = KafkaClientOptionsConfigurationExtensions.GetConsumerTopics(configuration);

if (consumerTopics.Count > 0)
{
    Console.WriteLine($" Configured topics: {string.Join(", ", consumerTopics)}");
}

try
{
    // 3. Produce one message per configured producer key
    var producedMessageIds = new List<string>(producerKeys.Count);
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

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(ConsumeTimeoutSeconds));
    
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

static IConfiguration BuildConfiguration()
{
    IConfigurationRoot fileConfig = new ConfigurationBuilder()
        .AddJsonFile(Path.Combine(AppContext.BaseDirectory, "local.settings.sample.json"), optional: true, reloadOnChange: false)
        .AddJsonFile(Path.Combine(AppContext.BaseDirectory, "local.settings.json"), optional: true, reloadOnChange: false)
        .Build();

    Dictionary<string, string?> functionsValues = ExtractFunctionsValues(fileConfig);

    return new ConfigurationBuilder()
        .AddJsonFile(Path.Combine(AppContext.BaseDirectory, "local.settings.sample.json"), optional: true, reloadOnChange: false)
        .AddJsonFile(Path.Combine(AppContext.BaseDirectory, "local.settings.json"), optional: true, reloadOnChange: false)
        .AddInMemoryCollection(functionsValues)
        .AddEnvironmentVariables()
        .Build();
}

static Dictionary<string, string?> ExtractFunctionsValues(IConfiguration configuration)
{
    IConfigurationSection values = configuration.GetSection("Values");
    if (!values.Exists())
    {
        return new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
    }

    return values
        .GetChildren()
        .Where(c => c.Value is not null)
        .ToDictionary(
            c => c.Key.Replace("__", ":", StringComparison.Ordinal),
            c => (string?)c.Value,
            StringComparer.OrdinalIgnoreCase);
}

static void ConfigureLogging(ILoggingBuilder builder)
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
}

public class TestMessage
{
    public string Id { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
}
