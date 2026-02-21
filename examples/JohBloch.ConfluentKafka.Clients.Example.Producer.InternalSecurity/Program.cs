using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Producer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

// 1. Setup DI and configuration
var services = new ServiceCollection();
IConfiguration configuration = BuildConfiguration();

services.AddLogging(ConfigureLogging);

services.AddKafkaProducerClient(options =>
    options
        .ApplySchemaRegistrySection(configuration)
        .ApplyKafkaSection(configuration)
        .ApplyProducerSection(configuration));

using ServiceProvider serviceProvider = services.BuildServiceProvider();

// 2. Get client
IKafkaProducerClient producerClient = serviceProvider.GetRequiredService<IKafkaProducerClient>();

Console.WriteLine("Kafka Producer Example Started");
Console.WriteLine("----------------------------");

IReadOnlyList<string> producerKeys = KafkaClientOptionsConfigurationExtensions.GetProducerKeys(configuration);

try
{
    // 3. Produce one message per configured producer key
    foreach (string key in producerKeys)
    {
        TestMessage message = new TestMessage
        {
            Id = Guid.NewGuid().ToString(),
            Content = $"Hello Kafka from producer '{key}'",
            Timestamp = DateTime.UtcNow
        };

        Console.WriteLine($" Producing message: {message.Id} via '{key}'");
        KafkaResult result = await producerClient.SendMessageWithSchemaAsync(
            message: message,
            key: message.Id,
            producerKey: key,
            schemaType: SchemaType.Json
        );

        Console.WriteLine($" Message produced to: {result.Topic}");
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
