# JohBloch.ConfluentKafka.Clients

[![Build Status](https://img.shields.io/github/actions/workflow/status/JohBloch/JohBloch.ConfluentKafka.Clients/build.yml?branch=main)](https://github.com/JohBloch/JohBloch.ConfluentKafka.Clients/actions)
[![NuGet](https://img.shields.io/nuget/v/JohBloch.ConfluentKafka.Clients.svg)](https://www.nuget.org/packages/JohBloch.ConfluentKafka.Clients/)
[![NuGet](https://img.shields.io/nuget/v/JohBloch.ConfluentKafka.Clients.Core.svg)](https://www.nuget.org/packages/JohBloch.ConfluentKafka.Clients.Core/)
[![NuGet](https://img.shields.io/nuget/v/JohBloch.ConfluentKafka.Clients.Consumer.svg)](https://www.nuget.org/packages/JohBloch.ConfluentKafka.Clients.Consumer/)
[![NuGet](https://img.shields.io/nuget/v/JohBloch.ConfluentKafka.Clients.Producer.svg)](https://www.nuget.org/packages/JohBloch.ConfluentKafka.Clients.Producer/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![.NET](https://img.shields.io/badge/.NET-8.0%20%7C%2010.0-purple)](https://dotnet.microsoft.com/)

A modern, feature-rich .NET client library for Apache Kafka with Schema Registry support, Dead Letter Queue functionality, and multiple serialization formats.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Build & CI](#build--ci)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Documentation](#documentation)
- [Requirements](#requirements)
- [Local Development (Docker)](#local-development-docker)
- [Dependencies](#dependencies)
- [Building from Source](#building-from-source)
- [Running Tests](#running-tests)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)
- [Support](#support)
- [Acknowledgments](#acknowledgments)
- [Roadmap](#roadmap)

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

Choose either the convenience package (everything included) or pick only the components you need.

### Convenience package (recommended)

```bash
dotnet add package JohBloch.ConfluentKafka.Clients
```

### Granular packages

```bash
dotnet add package JohBloch.ConfluentKafka.Clients.Core
dotnet add package JohBloch.ConfluentKafka.Clients.Consumer
dotnet add package JohBloch.ConfluentKafka.Clients.Producer
```

## Build & CI

- The .NET SDK version is pinned via `global.json`. GitHub Actions uses that file to select the SDK.
- To enable Snyk scanning in CI, add a repository secret named `SNYK_TOKEN`.

## Quick Start

### Repo example app (Console)

This repo includes runnable console examples under [examples/](examples/):

- [examples/JohBloch.ConfluentKafka.Clients.Example.Clients.InternalSecurity](examples/JohBloch.ConfluentKafka.Clients.Example.Clients.InternalSecurity) (meta package: consumer + producer)
- [examples/JohBloch.ConfluentKafka.Clients.Example.Producer.InternalSecurity](examples/JohBloch.ConfluentKafka.Clients.Example.Producer.InternalSecurity) (producer-only)
- [examples/JohBloch.ConfluentKafka.Clients.Example.Consumer.InternalSecurity](examples/JohBloch.ConfluentKafka.Clients.Example.Consumer.InternalSecurity) (consumer-only)
- [examples/JohBloch.ConfluentKafka.Clients.Example.Consumer.Msal.ExternalRefresh.RedisCache](examples/JohBloch.ConfluentKafka.Clients.Example.Consumer.Msal.ExternalRefresh.RedisCache) (consumer-only, MSAL token provider + Redis schema cache)

- Each example has a committed `local.settings.sample.json` template.
- For local development, copy it to `local.settings.json` and put secrets there (this file is ignored by git).
- PowerShell (example): `Copy-Item .\examples\JohBloch.ConfluentKafka.Clients.Example.Clients.InternalSecurity\local.settings.sample.json .\examples\JohBloch.ConfluentKafka.Clients.Example.Clients.InternalSecurity\local.settings.json`

Notes about configuration in the console example:

- The file uses an Azure Functions-style `Values` object.
- Keys use `__` as a separator for nested options.
- The console example binds `Kafka`, `SchemaRegistry`, `Consumer`, and `Producer` from separate root sections (see the JSON below).

If you're wiring this library into your own app and binding `KafkaClientOptions` directly from configuration, see **Minimal app configuration (recommended)** below.

Multi-topic + multi-producer is configured via:

- `Consumer__Topics`
- `Producer__Producers__*`

#### Schema cache (default in-memory, optional Redis)

- Default behavior is in-memory schema caching (no extra config needed).
- To override to Redis, configure the example app with:
    - `SchemaRegistry__Cache__Provider`: `Redis`
    - `SchemaRegistry__Cache__Redis__ConnectionString`: e.g. `localhost:6379`
    - Optional: `SchemaRegistry__Cache__Redis__KeyPrefix` and `SchemaRegistry__Cache__Redis__DefaultTtlSeconds`

Start Redis locally:

```bash
docker run --rm -p 6379:6379 redis:7-alpine
```

Example `local.settings.json` (for the repo console example app):

```json
{
    "IsEncrypted": false,
    "Values": {
        "Kafka__BootstrapServers": "localhost:9092",

        "SchemaRegistry__Url": "http://localhost:8081",

        "SchemaRegistry__Cache__Provider": "Redis",
        "SchemaRegistry__Cache__Redis__ConnectionString": "localhost:6379",
        "SchemaRegistry__Cache__Redis__KeyPrefix": "schema-registry-cache:",
        "SchemaRegistry__Cache__Redis__DefaultTtlSeconds": "3600",

        "Consumer__GroupId": "example-consumer-group",
        "Consumer__Topics": "topic-a,topic-b",
        "Consumer__AutoOffsetReset": "earliest",

        "Producer__Config__acks": "all",
        "Producer__Config__enable.idempotence": "true",

        "Producer__Producers__orders__Topic": "topic-a",
        "Producer__Producers__orders__AutoDlqOnDeliveryFailure": "true",
        "Producer__Producers__orders__DeadLetterQueueTopicPattern": "dlq-{topic}",

        "Producer__Producers__audit__Topic": "topic-b",
        "Producer__Producers__audit__AutoDlqOnDeliveryFailure": "true",
        "Producer__Producers__audit__DeadLetterQueueTopicPattern": "dlq-{topic}"
    }
}
```

### Minimal app configuration (recommended)

For most real applications, prefer binding `KafkaClientOptions` from a single `Kafka` root section:

- `Kafka:*` binds to `KafkaClientOptions`
- Optional: `Kafka:SchemaRegistry:*` binds to `SchemaRegistryOptions` when you want Schema Registry OAuth credentials that differ from Kafka OAuth.

Minimal example (JSON-form keys shown; environment variables use `__`):

```json
{
    "Kafka": {
        "BootstrapServers": "localhost:9092",
        "GroupId": "my-consumer-group",
        "SchemaRegistryUrl": "http://localhost:8081",
        "Consumer": {
            "Topic": "orders",
            "AutoOffsetReset": "Earliest"
        },
        "Producers": {
            "default": {
                "Topic": "orders"
            }
        }
    }
}
```

### Azure Functions (Isolated) - Configuration + DI (Recommended)

This example shows how to keep *all Kafka setup isolated in your consuming app* (not in the NuGet package code), and wire everything up from `Program.cs`.

#### `local.settings.json` (examples)

There are two common ways to configure Schema Registry:

- **Option A (simplest):** bind everything from `Kafka` using `KafkaClientOptions`.
- **Option B (most explicit):** bind Schema Registry settings separately using `SchemaRegistryOptions` under `Kafka__SchemaRegistry__*`.

Option A is a great default when you want a single options object (`KafkaClientOptions`). If Kafka and Schema Registry share the same OAuth settings, set both the `KafkaOauth*` and `SchemaRegistryOauth*` values to the same values.

##### Option A: Kafka OAuth + Schema Registry OAuth + `Kafka__SchemaRegistryUrl`

```json
{
    "IsEncrypted": false,
    "Values": {
        "AzureWebJobsStorage": "UseDevelopmentStorage=true",
        "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",

        "Kafka__BootstrapServers": "YOUR_BOOTSTRAP_SERVERS",
        "Kafka__GroupId": "my-function-consumer",

        "Kafka__KafkaOauthTokenEndpoint": "https://YOUR_IDP/oauth/token",
        "Kafka__KafkaOauthClientId": "YOUR_CLIENT_ID",
        "Kafka__KafkaOauthClientSecret": "YOUR_CLIENT_SECRET",
        "Kafka__KafkaOauthScope": "YOUR_SCOPE",

        "Kafka__KafkaOauthLogicalCluster": "lkc-...",
        "Kafka__KafkaOauthIdentityPoolId": "pool-...",

        "Kafka__SchemaRegistryOauthTokenEndpoint": "https://YOUR_IDP/oauth/token",
        "Kafka__SchemaRegistryOauthClientId": "YOUR_CLIENT_ID",
        "Kafka__SchemaRegistryOauthClientSecret": "YOUR_CLIENT_SECRET",
        "Kafka__SchemaRegistryOauthScope": "YOUR_SCOPE",

        "Kafka__SchemaRegistryOauthLogicalCluster": "lsrc-...",
        "Kafka__SchemaRegistryOauthIdentityPoolId": "pool-...",

        "Kafka__Consumer__Topic": "orders",
        "Kafka__Consumer__EnableAutoCommit": "false",
        "Kafka__Consumer__AutoOffsetReset": "Earliest",

        "Kafka__SchemaRegistryUrl": "https://YOUR_SCHEMA_REGISTRY"
    }
}
```

##### Option B: Schema Registry-specific OAuth (`Kafka__SchemaRegistry__*`)

```json
{
    "IsEncrypted": false,
    "Values": {
        "AzureWebJobsStorage": "UseDevelopmentStorage=true",
        "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",

        "Kafka__BootstrapServers": "YOUR_BOOTSTRAP_SERVERS",
        "Kafka__GroupId": "my-function-consumer",

        "Kafka__KafkaOauthTokenEndpoint": "https://YOUR_IDP/oauth/token",
        "Kafka__KafkaOauthClientId": "YOUR_CLIENT_ID",
        "Kafka__KafkaOauthClientSecret": "YOUR_CLIENT_SECRET",
        "Kafka__KafkaOauthScope": "YOUR_SCOPE",

        "Kafka__KafkaOauthLogicalCluster": "lkc-...",
        "Kafka__KafkaOauthIdentityPoolId": "pool-...",

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

### Schema Registry OAuth configuration (clarified)

You can configure Schema Registry OAuth either via `SchemaRegistryOptions` (bound separately) or via `KafkaClientOptions` (`SchemaRegistryOauth*`).

Precedence (highest → lowest):

1. `Kafka:SchemaRegistry:*` (explicit schema-registry settings via `SchemaRegistryOptions`)
2. `Kafka:SchemaRegistryOauth*` (defaults for `SchemaRegistryOptions` when you bind `KafkaClientOptions`)
3. Missing values will cause validation errors when Schema Registry OAuth is enabled

Schema Registry URL can be provided either as:

- `Kafka:SchemaRegistry:Url` (when you bind `SchemaRegistryOptions`), or
- `Kafka:SchemaRegistryUrl` (when you bind `KafkaClientOptions`)

Exact configuration keys (JSON form shown; equivalent environment variable names use `__`):

- Schema Registry specific (preferred):

```json
{
    "Kafka": {
        "SchemaRegistry": {
            "Url": "https://YOUR_SCHEMA_REGISTRY",
            "TokenEndpointUrl": "https://YOUR_IDP/oauth/token",
            "ClientId": "YOUR_SR_CLIENT_ID",
            "ClientSecret": "YOUR_SR_CLIENT_SECRET",
            "Scope": "YOUR_SR_SCOPE",
            "LogicalCluster": "lsrc-...",
            "IdentityPoolId": "YOUR_SR_IDENTITY_POOL_ID"
        }
    }
}
```

- `KafkaClientOptions` fallback (Schema Registry OAuth via `SchemaRegistryOauth*`):

```json
{
    "Kafka": {
        "SchemaRegistryOauthTokenEndpoint": "https://YOUR_IDP/oauth/token",
        "SchemaRegistryOauthClientId": "YOUR_SR_CLIENT_ID",
        "SchemaRegistryOauthClientSecret": "YOUR_SR_CLIENT_SECRET",
        "SchemaRegistryOauthScope": "YOUR_SR_SCOPE",
        "SchemaRegistryOauthLogicalCluster": "lsrc-...",
        "SchemaRegistryOauthIdentityPoolId": "YOUR_SR_IDENTITY_POOL_ID"
    }
}
```

How the library uses these values:

- The DI mapping prefers `Kafka:SchemaRegistry:*` values; when any of those are not set the library will fall back to the corresponding `Kafka:SchemaRegistryOauth*` value (when you bind `KafkaClientOptions`).
- You can also explicitly `PostConfigure<SchemaRegistryOptions>` (see `Program.cs`) to override either source.
- If Schema Registry OAuth appears enabled (token endpoint, client id or secret present) but required fields are missing, the provider will throw an informative validation error.

Custom token provider (e.g., MSAL):

- You can replace the default `ISecurityTokenProvider` by registering your own implementation in DI **before** calling `AddKafkaClients` / `AddKafkaProducerClient` / `AddKafkaConsumerClient`.
- The library registers `OAuthSecurityTokenProvider` using `TryAddSingleton`, so your custom provider will automatically win.
- Schema Registry token refresh for `SchemaRegistryExtClient` is wired up when a security provider is available; with a custom provider, refresh does not require `SchemaRegistryOptions.TokenEndpointUrl` to be set.

Environment variables equivalent (examples):

- `Kafka__SchemaRegistry__ClientId` → `SchemaRegistryOptions.ClientId`
- `Kafka__SchemaRegistry__ClientSecret` → `SchemaRegistryOptions.ClientSecret`
- `Kafka__SchemaRegistry__TokenEndpointUrl` → `SchemaRegistryOptions.TokenEndpointUrl`
- `Kafka__KafkaOauthClientId` → `KafkaClientOptions.KafkaOauthClientId` (Kafka brokers)
- `Kafka__SchemaRegistryOauthClientId` → `KafkaClientOptions.SchemaRegistryOauthClientId` (Schema Registry)


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

// Option A (simplest): configure Schema Registry URL via KafkaClientOptions (Kafka__SchemaRegistryUrl).
// No additional binding is needed.

// Option B (most explicit): if you use Kafka__SchemaRegistry__* keys, bind SchemaRegistryOptions too.
// This is useful when Schema Registry has different OAuth credentials than Kafka.
// builder.Services.PostConfigure<SchemaRegistryOptions>(
//     sr => builder.Configuration.GetSection("Kafka:SchemaRegistry").Bind(sr));

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
- Runtime dependencies:
    - Kafka broker (required for running the example / using the client)
    - Confluent Schema Registry (optional, only needed for schema-based serializers)

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
dotnet run --project examples/JohBloch.ConfluentKafka.Clients.Example.Clients.InternalSecurity/JohBloch.ConfluentKafka.Clients.Example.Clients.InternalSecurity.csproj
```

Stop the stack:

```bash
docker compose down -v
```

## Dependencies

Dependencies are split per NuGet package to keep consumers lightweight.

- **JohBloch.ConfluentKafka.Clients** (convenience package)
    - References `JohBloch.ConfluentKafka.Clients.Core`, `.Consumer`, and `.Producer` (brings their dependencies transitively).

- **JohBloch.ConfluentKafka.Clients.Core**
    - Confluent.Kafka `2.13.0`
    - Confluent.SchemaRegistry `2.13.0`
    - JohBloch.ConfluentKafka.SchemaRegistryExtClient `1.1.0`
    - Microsoft.Extensions.* (version depends on target framework)
        - net8.0: Http `9.0.0`, Logging/Options `10.0.2`
        - net10.0: Http/Logging/Options `10.0.1`

- **JohBloch.ConfluentKafka.Clients.Consumer**
    - Chr.Avro.Confluent `10.12.0`
    - protobuf-net `3.2.56`

- **JohBloch.ConfluentKafka.Clients.Producer**
    - Chr.Avro.Confluent `10.12.0`
    - protobuf-net `3.2.56`

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

All tests should pass in under a minute on a typical dev machine.

## Project Structure

```
├── src/
│   ├── JohBloch.ConfluentKafka.Clients/            # Convenience package (Core + Consumer + Producer)
│   ├── JohBloch.ConfluentKafka.Clients.Core/       # Options, interfaces, models, shared helpers, security
│   ├── JohBloch.ConfluentKafka.Clients.Consumer/   # Consumer client + deserialization implementations
│   └── JohBloch.ConfluentKafka.Clients.Producer/   # Producer client + serialization implementations
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