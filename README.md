# JohBloch.ConfluentKafka.Clients

[![Build Status](https://img.shields.io/github/actions/workflow/status/JohBloch/JohBloch.ConfluentKafka.Clients/build.yml?branch=main)](https://github.com/JohBloch/JohBloch.ConfluentKafka.Clients/actions)
[![NuGet](https://img.shields.io/nuget/v/JohBloch.ConfluentKafka.Clients.svg)](https://www.nuget.org/packages/JohBloch.ConfluentKafka.Clients/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%2010.0-purple)](https://dotnet.microsoft.com/)

A modern, feature-rich .NET client library for Apache Kafka with Schema Registry support, Dead Letter Queue functionality, and multiple serialization formats.

## Features

✨ **Multiple Schema Types**
- Avro (Chr.Avro with POCO support)
- JSON (System.Text.Json)
- Protobuf (protobuf-net for POCOs)

🔐 **Security**
- OAuth Bearer authentication (SASL OAUTHBEARER)
- SSL/TLS support
- Confluent Schema Registry integration

📬 **Dead Letter Queue (DLQ)**
- Automatic DLQ routing for failed messages
- Configurable topic patterns (per-topic or shared)
- JSON serialization optimized for Grafana/Loki
- Rich metadata and error context

⚡ **Performance**
- Batch message production with optimization
- Configurable compression (gzip, snappy, lz4, zstd)
- Async/await throughout
- Efficient memory usage

🛠️ **Developer Experience**
- Strongly typed configuration
- Comprehensive XML documentation
- Factory patterns for easy DI integration
- Extensive unit test coverage

## Installation

```bash
dotnet add package JohBloch.ConfluentKafka.Clients
```

## Quick Start

### Azure Functions (Isolated) - Configuration + DI (Recommended)

This example shows how to keep *all Kafka setup isolated in your consuming app* (not in the NuGet package code), and wire everything up from `Program.cs`.

#### `local.settings.json` (example)

```json
{
    "IsEncrypted": false,
    "Values": {
        "AzureWebJobsStorage": "UseDevelopmentStorage=true",
        "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",

        "Kafka__BootstrapServers": "YOUR_BOOTSTRAP_SERVERS",
        "Kafka__GroupId": "my-function-consumer",

        "Kafka__OAuthTokenEndpoint": "https://YOUR_IDP/oauth/token",
        "Kafka__OAuthClientId": "YOUR_CLIENT_ID",
        "Kafka__OAuthClientSecret": "YOUR_CLIENT_SECRET",
        "Kafka__OAuthScope": "YOUR_SCOPE",

        "Kafka__OAuthLogicalCluster": "lkc-...",
        "Kafka__OAuthIdentityPoolId": "pool-...",

        "Kafka__Consumer__Topic": "orders",
        "Kafka__Consumer__EnableAutoCommit": "false",
        "Kafka__Consumer__AutoOffsetReset": "Earliest",

        "Kafka__SchemaRegistry__Url": "https://YOUR_SCHEMA_REGISTRY",
        "Kafka__SchemaRegistry__TokenEndpointUrl": "https://YOUR_IDP/oauth/token",
        "Kafka__SchemaRegistry__ClientId": "YOUR_SR_CLIENT_ID",
        "Kafka__SchemaRegistry__ClientSecret": "YOUR_SR_CLIENT_SECRET",
        "Kafka__SchemaRegistry__Scope": "YOUR_SR_SCOPE",
        "Kafka__SchemaRegistry__LogicalCluster": "YOUR_SR_LOGICAL_CLUSTER",
        "Kafka__SchemaRegistry__IdentityPoolId": "YOUR_SR_IDENTITY_POOL_ID"
    }
}
```

#### `Program.cs` (Azure Functions isolated)

```csharp
using JohBloch.ConfluentKafka.Clients;
using JohBloch.ConfluentKafka.Clients.Models;
using Microsoft.Azure.Functions.Worker.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;

var builder = FunctionsApplication.CreateBuilder(args);

builder.ConfigureFunctionsWebApplication();

// Bind all options from configuration and register the library services.
builder.Services.AddKafkaClients(options => builder.Configuration.GetSection("Kafka").Bind(options));

// Optional: bind Schema Registry OAuth settings (Url + OAuth fields).
// The library maps SchemaRegistryUrl by default; this lets you provide the full OAuth configuration.
builder.Services.PostConfigure<SchemaRegistryOptions>(sr => builder.Configuration.GetSection("Kafka:SchemaRegistry").Bind(sr));

var app = builder.Build();
app.Run();
```

#### `KafkaTimer` function (poll every 5 minutes)

```csharp
using System.Text.Json;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

public sealed class KafkaTimer
{
    private readonly IKafkaConsumerClient _consumer;
    private readonly ILogger<KafkaTimer> _logger;

    public KafkaTimer(IKafkaConsumerClient consumer, ILogger<KafkaTimer> logger)
    {
        _consumer = consumer;
        _logger = logger;
    }

    [Function(nameof(KafkaTimer))]
    public async Task Run([TimerTrigger("0 */5 * * * *")] TimerInfo timer, CancellationToken ct)
    {
        _logger.LogInformation("KafkaTimer fired at {UtcNow}", DateTimeOffset.UtcNow);

        const int maxMessages = 100;
        const int timeoutMs = 4000;

        // Use JsonElement for a generic "just log it" approach.
        // If you have a POCO, replace JsonElement with your type.
        var batch = await _consumer.ConsumeBatchAsync<JsonElement>(maxMessages, timeoutMs, ct);

        if (batch.Count == 0)
        {
            _logger.LogInformation("No messages received.");
            return;
        }

        foreach (var record in batch)
        {
            var value = record.Message?.Value;
            _logger.LogInformation(
                "Received. Topic={Topic} Partition={Partition} Offset={Offset} Key={Key} Value={Value}",
                record.Topic,
                record.Partition.Value,
                record.Offset.Value,
                record.Message?.Key,
                value.ValueKind == JsonValueKind.Undefined ? "<undefined>" : value.GetRawText());

            // Simple demo: commit each message.
            _consumer.Commit(record);
        }

        _logger.LogInformation("Processed and committed {Count} messages.", batch.Count);
    }
}
```

### Producer Example

```csharp
using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;

public sealed class OrderPublisher
{
    private readonly IKafkaProducerClient _producer;

    public OrderPublisher(IKafkaProducerClient producer)
    {
        _producer = producer;
    }

    public Task PublishAsync(Order order, CancellationToken ct)
    {
        // The producerKey must exist in Kafka:Producers config
        return _producer.SendMessageWithSchemaAsync(
            message: order,
            key: order.OrderId,
            producerKey: "default",
            schemaType: SchemaType.Json,
            ct: ct);
    }
}
```

### Consumer Example

```csharp
using JohBloch.ConfluentKafka.Clients.Interfaces;

public sealed class OrderWorker
{
    private readonly IKafkaConsumerClient _consumer;

    public OrderWorker(IKafkaConsumerClient consumer)
    {
        _consumer = consumer;
    }

    public async Task PollOnceAsync(CancellationToken ct)
    {
        // Optional if you already set Kafka:Consumer:Topic
        _consumer.Subscribe(new[] { "orders" });

        var record = await _consumer.ConsumeAsync<Order>(ct);
        if (record is null) return;

        await ProcessOrderAsync(record.Message.Value);
        _consumer.Commit(record);
    }
}
```

### Dead Letter Queue Example

```csharp
try
{
    await ProcessMessageAsync(message);
}
catch (Exception ex)
{
    // Automatically send to DLQ
    await producer.SendToDeadLetterQueueAsync(
        originalMessage: consumeResult,
        exception: ex,
        retryCount: 3);
}
```

## Configuration

### Producer Options

```csharp
public class KafkaProducerOptions
{
    public string BootstrapServers { get; set; }
    public string Topic { get; set; }
    public string ApplicationId { get; set; }
    public int BatchSizeKB { get; set; } = 32;
    public int LingerMS { get; set; } = 100;
    public int QueueBufferMaxMessages { get; set; } = 50000;
    public string CompressionType { get; set; } = "none";
    public int CompressionLevel { get; set; } = 0;
    public string DeadLetterQueueTopicPattern { get; set; } = "dlq-{topic}";
    public bool IncludeStackTraceInDlq { get; set; } = false;
    public bool AutoDlqOnDeliveryFailure { get; set; } = false;
}
```

### Consumer Options

```csharp
public class KafkaConsumerOptions
{
    public string BootstrapServers { get; set; }
    public string GroupId { get; set; }
    public string Topic { get; set; }
    public int SessionTimeoutMs { get; set; } = 45000;
    public int HeartbeatIntervalMs { get; set; } = 3000;
    public string AutoOffsetReset { get; set; } = "earliest";
    public bool EnableAutoCommit { get; set; } = true;
    public SchemaType DefaultSchemaType { get; set; } = SchemaType.Avro;
    public bool AutoDetectSchemaType { get; set; } = true;
    public Dictionary<string, SchemaType> TopicSchemaTypes { get; set; } = new();
}
```

## Documentation

### Getting Started Guides

- 📘 [Avro Serialization Example](docs/AvroExample.md) - Complete guide for Avro with schema evolution
- 📗 [JSON Serialization Example](docs/JsonExample.md) - JSON serialization with System.Text.Json
- 📕 [Protobuf POCO Support](docs/ProtobufNetExample.md) - Using protobuf-net with POCOs

### Authentication & Security

- 🔐 [OAuth Authentication Example](docs/OAuthExample.md) - OAuth/OIDC for Azure AD, Okta, Keycloak
- 🔑 [API Key Authentication Example](docs/ApiKeyExample.md) - API keys for Confluent Cloud, AWS MSK, Azure Event Hubs

### Advanced Patterns

- 🔄 [Multi-Topic Example](docs/MultiTopicExample.md) - Order processing with retry logic and DLQ
- ⚡ [Batch Processing Example](docs/BatchExample.md) - High-throughput batch producer and consumer
- 📬 [Dead Letter Queue Guide](docs/DeadLetterQueue.md) - DLQ patterns and best practices

### Project Documentation

- [Contributing Guidelines](CONTRIBUTING.md)
- [Security Policy](SECURITY.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Changelog](CHANGELOG.md)
- [API Stability](docs/ApiStability.md)

## Requirements

- To consume this package: .NET 8.0 or .NET 10.0
- To build this repo from source: .NET 10.0 SDK (see `global.json`)

## Local Development (Docker)

This repo includes a minimal local stack for running the example app:

- Kafka broker: `localhost:9092` (PLAINTEXT)
- Schema Registry: `http://localhost:8081`

Start the stack:

```bash
docker compose up -d
```

Run the example:

```bash
dotnet run --project examples/JohBloch.ConfluentKafka.Clients.Example/JohBloch.ConfluentKafka.Clients.Example.csproj
```

Stop the stack:

```bash
docker compose down -v
```
- Apache Kafka 2.0+
- Confluent Schema Registry (optional, for schema support)

## Dependencies

- Confluent.Kafka 2.13.0
- Confluent.SchemaRegistry 2.13.0
- Chr.Avro.Confluent 10.12.0
- protobuf-net 3.2.56
- Microsoft.Extensions.Logging 9.0.0

## Building from Source

```bash
git clone https://github.com/JohBloch/JohBloch.ConfluentKafka.Clients.git
cd JohBloch.ConfluentKafka.Clients
dotnet build
```

## Running Tests

```bash
dotnet test
```

All tests should pass in approximately 50 seconds.

## Project Structure

```
├── src/
│   └── JohBloch.ConfluentKafka.Clients/
│       ├── Interfaces/              # Public interfaces
│       ├── Models/                  # Data models
│       ├── Services/                # Core implementations
│       │   ├── Serialization/       # Organized by schema type
│       │   │   ├── Avro/
│       │   │   ├── Json/
│       │   │   └── Protobuf/
│       │   ├── KafkaProducerClient.cs
│       │   └── KafkaConsumerClient.cs
│       └── Security/                # Security providers
├── tests/                           # Unit tests
├── docs/                            # Documentation
└── JohBloch.ConfluentKafka.Clients.sln
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- 📫 Report bugs via [GitHub Issues](https://github.com/JohBloch/JohBloch.ConfluentKafka.Clients/issues)
- 💬 Ask questions in [Discussions](https://github.com/JohBloch/JohBloch.ConfluentKafka.Clients/discussions)
- 🔒 Report security vulnerabilities via [Security Policy](SECURITY.md)

## Acknowledgments

- Built on top of [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet)
- Uses [Chr.Avro](https://github.com/ch-robinson/dotnet-avro) for Avro serialization
- Uses [protobuf-net](https://github.com/protobuf-net/protobuf-net) for Protobuf support

## Roadmap

- [ ] Add integration test suite with Testcontainers
- [ ] Support for Kafka transactions
- [ ] Metrics and telemetry integration
- [ ] Additional authentication mechanisms
- [ ] Performance benchmarks

---

Made with ❤️ by JohBloch