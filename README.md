# JohBloch.ConfluentKafka.Clients

[![Build Status](https://img.shields.io/github/actions/workflow/status/JohBloch/JohBloch.ConfluentKafka.Clients/build.yml?branch=main)](https://github.com/JohBloch/JohBloch.ConfluentKafka.Clients/actions)
[![NuGet](https://img.shields.io/nuget/v/JohBloch.ConfluentKafka.Clients.svg)](https://www.nuget.org/packages/JohBloch.ConfluentKafka.Clients/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![.NET](https://img.shields.io/badge/.NET-8.0-purple)](https://dotnet.microsoft.com/)

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
- Extensive unit test coverage (106 tests)

## Installation

```bash
dotnet add package JohBloch.ConfluentKafka.Clients
```

## Quick Start

### Producer Example

```csharp
using JohBloch.ConfluentKafka.Clients.Services;
using JohBloch.ConfluentKafka.Clients.Models;

// Configure producer
var producerOptions = new Dictionary<string, KafkaProducerOptions>
{
    ["default"] = new KafkaProducerOptions
    {
        BootstrapServers = "localhost:9092",
        Topic = "orders",
        ApplicationId = "order-service",
        BatchSizeKB = 32,
        CompressionType = "gzip"
    }
};

// Create producer client
var producer = new KafkaProducerClient(
    producerOptions,
    securityTokenProvider,
    schemaRegistryFactory,
    logger);

// Send single message
var order = new Order { OrderId = "123", Amount = 99.99m };
await producer.SendMessageWithSchemaAsync(
    message: order,
    key: "123",
    producerKey: "default",
    schemaType: SchemaType.Json);

// Send batch
var orders = new List<Order> { /* ... */ };
await producer.SendBatchAsync(
    messages: orders,
    keySelector: o => o.OrderId,
    producerKey: "default");
```

### Consumer Example

```csharp
// Configure consumer
var consumerOptions = new KafkaConsumerOptions
{
    BootstrapServers = "localhost:9092",
    GroupId = "order-processor",
    Topics = new[] { "orders" }
};

// Create consumer client
var consumer = new KafkaConsumerClient(
    consumerOptions,
    schemaRegistryOptions,
    securityTokenProvider,
    schemaRegistryFactory,
    logger);

// Subscribe and consume
consumer.Subscribe(new[] { "orders" });

var result = await consumer.ConsumeAsync<Order>(
    topics: new[] { "orders" },
    deserializer: deserializer,
    cancellationToken: cancellationToken);

foreach (var message in result.Messages)
{
    await ProcessOrderAsync(message.Message.Value);
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
    public int QueueBufferMaxMessages { get; set; } = 100000;
    public string CompressionType { get; set; } = "none";
    public string DeadLetterQueueTopicPattern { get; set; } = "dlq-{topic}";
    public bool IncludeStackTraceInDlq { get; set; } = false;
}
```

### Consumer Options

```csharp
public class KafkaConsumerOptions
{
    public string BootstrapServers { get; set; }
    public string GroupId { get; set; }
    public string[] Topics { get; set; }
    public string AutoOffsetReset { get; set; } = "earliest";
    public bool EnableAutoCommit { get; set; } = false;
    public int MaxPollRecords { get; set; } = 500;
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

- .NET 8.0 or later

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
- protobuf-net 3.2.30
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

All 106 tests should pass in approximately 50 seconds.

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
├── tests/                           # Unit tests (106 tests)
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