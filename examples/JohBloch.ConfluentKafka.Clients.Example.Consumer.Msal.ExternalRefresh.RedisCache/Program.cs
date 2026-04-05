using Confluent.Kafka;
using JohBloch.ConfluentKafka.Clients;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Consumer;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using JohBloch.ConfluentKafka.Clients.Security;
using JohBloch.ConfluentKafka.Clients.Example.Consumer.Msal.ExternalRefresh.RedisCache;

const int ConsumeTimeoutSeconds = 15;

// 1. Setup DI and configuration
var services = new ServiceCollection();
IConfiguration configuration = BuildConfiguration();

services.AddLogging(ConfigureLogging);

services.AddSchemaRegistryCache(configuration);

// MSAL-based security token provider (external refresh login).
// Register this BEFORE AddKafkaConsumerClient so the built-in OAuth provider (TryAdd) is skipped.
services.Configure<MsalTokenProviderOptions>(configuration.GetSection("Msal"));
services.AddSingleton<ISecurityTokenProvider, MsalDeviceCodeSecurityTokenProvider>();

services.AddKafkaConsumerClient(options =>
    options
        .ApplySchemaRegistrySection(configuration)
        .ApplyKafkaSection(configuration)
        .ApplyConsumerSection(configuration));

using ServiceProvider serviceProvider = services.BuildServiceProvider();

// 2. Get Client
IKafkaConsumerClient consumerClient = serviceProvider.GetRequiredService<IKafkaConsumerClient>();

Console.WriteLine("Kafka Consumer MSAL Example Started");
Console.WriteLine("----------------------------");
IReadOnlyList<string> consumerTopics = KafkaClientOptionsConfigurationExtensions.GetConsumerTopics(configuration);

if (consumerTopics.Count > 0)
{
    Console.WriteLine($" Configured topics: {string.Join(", ", consumerTopics)}");
}

try
{
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
