# Avro Example

This guide demonstrates how to produce and consume Avro-serialized messages using the Kafka client library.

## Prerequisites

- Kafka cluster with Schema Registry
- NuGet package: `JohBloch.ConfluentKafka.Clients`
- NuGet package: `Chr.Avro.Confluent` (automatically included)

## Define Your Model

```csharp
public class UserProfile
{
    public string UserId { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
}
```

## Configuration

```csharp
using Microsoft.Extensions.DependencyInjection;
using JohBloch.ConfluentKafka.Clients.Configuration;

var services = new ServiceCollection();

services.AddKafkaClients(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.SchemaRegistryUrl = "http://localhost:8081";
    options.GroupId = "avro-consumer-group";
    
    // Global config applies to all producers
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "acks", "all" },
        { "retries", "3" }
    };
    
    // Per-producer config (optional)
    options.PerProducerConfigs = new Dictionary<string, Dictionary<string, string>>
    {
        ["user-profiles"] = new Dictionary<string, string>
        {
            { "compression.type", "snappy" }
        }
    };
});

var serviceProvider = services.BuildServiceProvider();
```

## Producing Avro Messages

```csharp
using JohBloch.ConfluentKafka.Clients.Services;
using Confluent.Kafka;

// Get the producer client from DI
var producerClient = serviceProvider.GetRequiredService<IKafkaProducerClient>();

// Create your message
var userProfile = new UserProfile
{
    UserId = "12345",
    Username = "johndoe",
    Email = "john.doe@example.com",
    CreatedAt = DateTime.UtcNow
};

// Produce with Avro serialization (schema auto-registered)
var result = await producerClient.ProduceAsync(
    topic: "user-profiles",
    key: userProfile.UserId,
    value: userProfile,
    serializationType: SerializationType.Avro
);

Console.WriteLine($"Message delivered to {result.TopicPartitionOffset}");
```

## Consuming Avro Messages

```csharp
using JohBloch.ConfluentKafka.Clients.Services;

// Get the consumer client from DI
var consumerClient = serviceProvider.GetRequiredService<IKafkaConsumerClient>();

// Initialize consumer (subscribes to topic)
await consumerClient.InitializeConsumer(
    topics: new[] { "user-profiles" },
    serializationType: SerializationType.Avro
);

// Start consuming
await foreach (var message in consumerClient.ConsumeAsync<UserProfile>())
{
    try
    {
        Console.WriteLine($"Consumed user: {message.Value.Username} ({message.Value.Email})");
        Console.WriteLine($"Key: {message.Key}");
        Console.WriteLine($"Partition: {message.Partition}");
        Console.WriteLine($"Offset: {message.Offset}");
        
        // Process your message here...
        
        // Commit offset after successful processing
        consumerClient.Commit(message);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Error processing message: {ex.Message}");
        
        // Optionally send to Dead Letter Queue
        await producerClient.SendToDeadLetterQueueAsync(
            message: message,
            exception: ex,
            reason: "Processing failed"
        );
    }
}
```

## Advanced: Schema Evolution

Avro supports schema evolution. You can add optional fields without breaking consumers:

```csharp
public class UserProfileV2
{
    public string UserId { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    
    // New optional field
    public string? PhoneNumber { get; set; }
}

// Old consumers can still read messages produced with new schema
// New consumers can read messages produced with old schema
```

## Best Practices

1. **Schema Design**: Use nullable types for optional fields to support schema evolution
2. **Error Handling**: Always wrap message processing in try-catch and use DLQ for failed messages
3. **Commit Strategy**: Commit offsets after successful processing to avoid reprocessing
4. **Schema Registry**: Ensure Schema Registry is accessible and properly configured
5. **Testing**: Test schema evolution scenarios before deploying to production

## Troubleshooting

### Schema Registry Connection Error
```
Error: Failed to connect to Schema Registry at http://localhost:8081
```
**Solution**: Verify Schema Registry URL and network connectivity

### Schema Compatibility Error
```
Error: Schema being registered is incompatible with an earlier schema
```
**Solution**: Check Schema Registry compatibility mode and adjust schema evolution strategy

### Deserialization Error
```
Error: Failed to deserialize Avro message
```
**Solution**: Ensure consumer model matches the schema registered in Schema Registry

## See Also

- [Dead Letter Queue Documentation](DeadLetterQueue.md)
- [JSON Example](JsonExample.md)
- [OAuth Authentication](OAuthExample.md)
