# API Key Authentication Example

This guide demonstrates how to configure API key authentication for Kafka clients, commonly used with Confluent Cloud and managed Kafka services.

## Prerequisites

- Kafka cluster with SASL authentication enabled
- API key and secret from your Kafka provider
- Schema Registry API key (if using Schema Registry)

## Basic API Key Configuration

```csharp
using Microsoft.Extensions.DependencyInjection;
using JohBloch.ConfluentKafka.Clients.Configuration;

var services = new ServiceCollection();

services.AddKafkaClients(options =>
{
    options.BootstrapServers = "your-kafka-broker:9092";
    options.SchemaRegistryUrl = "https://your-schema-registry:8081";
    options.GroupId = "api-key-consumer-group";
    
    // API Key authentication for Kafka
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "PLAIN" },
        { "sasl.username", "your-api-key" },
        { "sasl.password", "your-api-secret" }
    };
    
    options.ConsumerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "PLAIN" },
        { "sasl.username", "your-api-key" },
        { "sasl.password", "your-api-secret" }
    };
});

var serviceProvider = services.BuildServiceProvider();
```

## Confluent Cloud Configuration

```csharp
services.AddKafkaClients(options =>
{
    // Confluent Cloud bootstrap servers
    options.BootstrapServers = "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092";
    options.SchemaRegistryUrl = "https://psrc-xxxxx.us-east-1.aws.confluent.cloud";
    options.GroupId = "confluent-cloud-group";
    
    // Kafka API key authentication
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "PLAIN" },
        { "sasl.username", "your-kafka-api-key" },
        { "sasl.password", "your-kafka-api-secret" },
        { "acks", "all" }
    };
    
    options.ConsumerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "PLAIN" },
        { "sasl.username", "your-kafka-api-key" },
        { "sasl.password", "your-kafka-api-secret" },
        { "auto.offset.reset", "earliest" }
    };
    
    // Schema Registry API key authentication
    // Note: Schema Registry auth is configured separately in the library
    // The library uses BasicAuth with the Schema Registry URL
});
```

## Schema Registry API Key Configuration

For Schema Registry with API key authentication:

```csharp
services.AddKafkaClients(options =>
{
    options.BootstrapServers = "your-kafka-broker:9092";
    options.SchemaRegistryUrl = "https://your-schema-registry:8081";
    options.GroupId = "my-consumer-group";
    
    // Kafka authentication
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "PLAIN" },
        { "sasl.username", "kafka-api-key" },
        { "sasl.password", "kafka-api-secret" }
    };
    
    // Schema Registry authentication (configure via Schema Registry client config)
    // The library's SchemaRegistryFactory handles this internally
    // For custom configuration, you can inject ISchemaRegistryClient
});
```

If you need custom Schema Registry authentication:

```csharp
using Confluent.SchemaRegistry;

// Manual Schema Registry client configuration (advanced)
var schemaRegistryConfig = new SchemaRegistryConfig
{
    Url = "https://your-schema-registry:8081",
    BasicAuthUserInfo = "sr-api-key:sr-api-secret" // Format: "key:secret"
};

var schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);

// Register in DI if needed
services.AddSingleton<ISchemaRegistryClient>(schemaRegistryClient);
```

## Environment-Based Configuration

**Recommended approach** for production:

```csharp
using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json")
    .AddEnvironmentVariables()
    .AddUserSecrets<Program>() // For development
    .Build();

services.AddKafkaClients(options =>
{
    options.BootstrapServers = configuration["Kafka:BootstrapServers"]!;
    options.SchemaRegistryUrl = configuration["Kafka:SchemaRegistryUrl"]!;
    options.GroupId = configuration["Kafka:GroupId"]!;
    
    // API keys from configuration
    var apiKey = configuration["Kafka:ApiKey"]!;
    var apiSecret = configuration["Kafka:ApiSecret"]!;
    
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "PLAIN" },
        { "sasl.username", apiKey },
        { "sasl.password", apiSecret }
    };
    
    options.ConsumerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "PLAIN" },
        { "sasl.username", apiKey },
        { "sasl.password", apiSecret }
    };
});
```

**appsettings.json** (don't commit secrets!):
```json
{
  "Kafka": {
    "BootstrapServers": "pkc-xxxxx.region.provider.confluent.cloud:9092",
    "SchemaRegistryUrl": "https://psrc-xxxxx.region.provider.confluent.cloud",
    "GroupId": "my-consumer-group"
  }
}
```

**User Secrets** (for development):
```bash
dotnet user-secrets set "Kafka:ApiKey" "your-kafka-api-key"
dotnet user-secrets set "Kafka:ApiSecret" "your-kafka-api-secret"
```

**Environment Variables** (for production):
```bash
export Kafka__ApiKey="your-kafka-api-key"
export Kafka__ApiSecret="your-kafka-api-secret"
```

## AWS MSK with SASL/SCRAM

For Amazon MSK with SASL/SCRAM authentication:

```csharp
services.AddKafkaClients(options =>
{
    options.BootstrapServers = "b-1.your-cluster.kafka.region.amazonaws.com:9096";
    options.SchemaRegistryUrl = "https://your-schema-registry:8081";
    options.GroupId = "msk-consumer-group";
    
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "SCRAM-SHA-512" }, // or SCRAM-SHA-256
        { "sasl.username", "your-msk-username" },
        { "sasl.password", "your-msk-password" }
    };
    
    options.ConsumerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "SCRAM-SHA-512" },
        { "sasl.username", "your-msk-username" },
        { "sasl.password", "your-msk-password" }
    };
});
```

## Azure Event Hubs for Kafka

Azure Event Hubs uses connection string-based authentication:

```csharp
services.AddKafkaClients(options =>
{
    options.BootstrapServers = "your-namespace.servicebus.windows.net:9093";
    options.SchemaRegistryUrl = "https://your-schema-registry:8081"; // If using external SR
    options.GroupId = "eventhubs-consumer-group";
    
    var connectionString = configuration["EventHubs:ConnectionString"]!;
    
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "PLAIN" },
        { "sasl.username", "$ConnectionString" },
        { "sasl.password", connectionString }
    };
    
    options.ConsumerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "PLAIN" },
        { "sasl.username", "$ConnectionString" },
        { "sasl.password", connectionString }
    };
});
```

## Per-Producer API Keys

Configure different API keys for different producers:

```csharp
services.AddKafkaClients(options =>
{
    options.BootstrapServers = "your-kafka-broker:9092";
    options.SchemaRegistryUrl = "https://your-schema-registry:8081";
    options.GroupId = "multi-key-group";
    
    // Default/global producer config
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "PLAIN" },
        { "sasl.username", "default-api-key" },
        { "sasl.password", "default-api-secret" }
    };
    
    // Per-topic producer config (overrides global)
    options.PerProducerConfigs = new Dictionary<string, Dictionary<string, string>>
    {
        ["sensitive-topic"] = new Dictionary<string, string>
        {
            { "sasl.username", "sensitive-topic-api-key" },
            { "sasl.password", "sensitive-topic-api-secret" }
        },
        ["analytics-topic"] = new Dictionary<string, string>
        {
            { "sasl.username", "analytics-api-key" },
            { "sasl.password", "analytics-api-secret" }
        }
    };
});
```

## Testing API Key Configuration

```csharp
using JohBloch.ConfluentKafka.Clients.Services;

var producerClient = serviceProvider.GetRequiredService<IKafkaProducerClient>();

try
{
    var result = await producerClient.ProduceAsync(
        topic: "test-topic",
        key: "test-key",
        value: new { message = "API key test" },
        serializationType: SerializationType.Json
    );
    
    Console.WriteLine("✅ API key authentication successful!");
    Console.WriteLine($"Message delivered to {result.TopicPartitionOffset}");
}
catch (Exception ex)
{
    Console.WriteLine("❌ API key authentication failed:");
    Console.WriteLine(ex.Message);
    
    // Common errors:
    // - "Authentication failed": Invalid API key or secret
    // - "Not authorized": API key lacks required permissions
    // - "Connection refused": Incorrect bootstrap servers
}
```

## Best Practices

1. **Never Commit Secrets**: Use environment variables, user secrets, or secret managers
   ```bash
   # ❌ DON'T do this
   options.GlobalProducerConfig["sasl.password"] = "hardcoded-secret";
   
   # ✅ DO this
   options.GlobalProducerConfig["sasl.password"] = configuration["Kafka:ApiSecret"]!;
   ```

2. **Rotate API Keys Regularly**: Implement key rotation strategy
   
3. **Principle of Least Privilege**: Create API keys with minimum required permissions
   
4. **Separate Keys for Environments**: Use different API keys for dev/staging/production
   
5. **Monitor Authentication Failures**: Set up alerts for authentication errors

6. **Use SSL/TLS**: Always use `SASL_SSL` in production (not `SASL_PLAINTEXT`)

7. **Validate Configuration**: Test authentication before deploying

## Security Considerations

```csharp
// ✅ Good: Secure configuration
options.GlobalProducerConfig = new Dictionary<string, string>
{
    { "security.protocol", "SASL_SSL" },      // SSL encryption
    { "sasl.mechanism", "PLAIN" },
    { "sasl.username", apiKey },
    { "sasl.password", apiSecret },
    { "ssl.endpoint.identification.algorithm", "https" } // Verify broker hostname
};

// ❌ Bad: Insecure configuration (development only)
options.GlobalProducerConfig = new Dictionary<string, string>
{
    { "security.protocol", "SASL_PLAINTEXT" }, // No encryption!
    { "sasl.mechanism", "PLAIN" },
    { "sasl.username", "admin" },              // Hardcoded!
    { "sasl.password", "password123" }         // Hardcoded!
};
```

## Troubleshooting

### Authentication Failed
```
Error: Authentication failed: SASL authentication failed
```
**Solution**: Verify API key and secret are correct

### Broker Connection Failed
```
Error: Failed to connect to broker
```
**Solution**: Check bootstrap servers address and port (usually 9092 or 9093 for SSL)

### Not Authorized
```
Error: Not authorized to access topics
```
**Solution**: Verify API key has appropriate ACLs/permissions for the topic

### SSL Handshake Failed
```
Error: SSL handshake failed
```
**Solution**: Ensure using correct security protocol (`SASL_SSL` not `SASL_PLAINTEXT`)

### Schema Registry Authentication Failed
```
Error: Schema Registry authentication failed
```
**Solution**: Verify Schema Registry credentials are configured correctly

## See Also

- [OAuth Authentication](OAuthExample.md)
- [Avro Example](AvroExample.md)
- [JSON Example](JsonExample.md)
- [Security Best Practices](../SECURITY.md)
