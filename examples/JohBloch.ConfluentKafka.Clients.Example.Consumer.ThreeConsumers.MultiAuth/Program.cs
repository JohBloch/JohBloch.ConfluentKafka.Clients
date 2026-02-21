using Confluent.Kafka;
using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Consumer;
using JohBloch.ConfluentKafka.Clients.Example.Consumer.ThreeConsumers.MultiAuth.Extensions;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

const int ConsumeNullDelayMs = 100;

IConfiguration configuration = BuildConfiguration();
IConfigurationSection consumersRoot = configuration.GetSection("Consumers");

if (!consumersRoot.Exists())
{
    Console.WriteLine("Missing configuration section: Consumers");
    Console.WriteLine("Provide local.settings.json or local.settings.sample.json");
    return;
}

ServiceProvider spOAuthA = BuildServiceProvider(consumersRoot.GetSection("OAuthA"), consumerName: "OAuthA");
ServiceProvider spOAuthB = BuildServiceProvider(consumersRoot.GetSection("OAuthB"), consumerName: "OAuthB");
ServiceProvider spApiKey = BuildServiceProvider(consumersRoot.GetSection("ApiKey"), consumerName: "ApiKey");

try
{
    IKafkaConsumerClient c1 = spOAuthA.GetRequiredService<IKafkaConsumerClient>();
    IKafkaConsumerClient c2 = spOAuthB.GetRequiredService<IKafkaConsumerClient>();
    IKafkaConsumerClient c3 = spApiKey.GetRequiredService<IKafkaConsumerClient>();

    using CancellationTokenSource cts = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) =>
    {
        e.Cancel = true;
        cts.Cancel();
    };

    Console.WriteLine("Kafka 3-consumer example started. Press Ctrl+C to stop.");

    Task t1 = ConsumeLoopAsync("OAuthA", c1, ConsumeNullDelayMs, cts.Token);
    Task t2 = ConsumeLoopAsync("OAuthB", c2, ConsumeNullDelayMs, cts.Token);
    Task t3 = ConsumeLoopAsync("ApiKey", c3, ConsumeNullDelayMs, cts.Token);

    await Task.WhenAll(t1, t2, t3);
}
finally
{
    spOAuthA.Dispose();
    spOAuthB.Dispose();
    spApiKey.Dispose();
}

static async Task ConsumeLoopAsync(
    string name,
    IKafkaConsumerClient consumer,
    int nullDelayMs,
    CancellationToken ct)
{
    while (!ct.IsCancellationRequested)
    {
        ConsumeResult<string, string>? result = null;

        try
        {
            result = await consumer.ConsumeAsync<string>(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return;
        }

        if (result is null)
        {
            await Task.Delay(nullDelayMs, ct);
            continue;
        }

        string value = result.Message?.Value ?? "<null>";
        Console.WriteLine($"[{name}] {result.TopicPartitionOffset}: {value}");

        try
        {
            consumer.Commit(result);
        }
        catch (KafkaException ex)
        {
            Console.WriteLine($"[{name}] Commit failed: {ex.Error.Reason}");
        }
    }
}

static ServiceProvider BuildServiceProvider(IConfigurationSection consumerSection, string consumerName)
{
    if (!consumerSection.Exists())
    {
        throw new InvalidOperationException($"Missing consumer config section: Consumers:{consumerName}");
    }

    ServiceCollection services = new ServiceCollection();

    services.AddLogging(builder =>
    {
        builder.AddConsole();
        builder.SetMinimumLevel(LogLevel.Information);
    });

    services.AddKafkaConsumerClient((KafkaClientOptions options) =>
    {
        options
            .ApplySchemaRegistrySection(consumerSection)
            .ApplyKafkaSection(consumerSection)
            .ApplyConsumerSection(consumerSection);
    });

    return services.BuildServiceProvider();
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
