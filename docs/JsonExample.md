# JSON Example

This guide demonstrates how to produce and consume JSON-serialized messages using the Kafka client library.

## Prerequisites

- Kafka cluster with Schema Registry
- NuGet package: `JohBloch.ConfluentKafka.Clients`
- System.Text.Json (included in .NET)

## Define Your Model

```csharp
using System.Text.Json.Serialization;

public class OrderEvent
{
    [JsonPropertyName("order_id")]
    public string OrderId { get; set; } = string.Empty;
    
    [JsonPropertyName("customer_id")]
    public string CustomerId { get; set; } = string.Empty;
    
    [JsonPropertyName("total_amount")]
    public decimal TotalAmount { get; set; }
    
    [JsonPropertyName("items")]
    public List<OrderItem> Items { get; set; } = new();
    
    [JsonPropertyName("created_at")]
    public DateTime CreatedAt { get; set; }
}

public class OrderItem
{
    [JsonPropertyName("product_id")]
    public string ProductId { get; set; } = string.Empty;
    
    [JsonPropertyName("quantity")]
    public int Quantity { get; set; }
    
    [JsonPropertyName("price")]
    public decimal Price { get; set; }
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
    options.GroupId = "json-consumer-group";
    
    // Configure producer for optimal JSON performance
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "acks", "all" },
        { "compression.type", "gzip" }, // Good compression for JSON
        { "batch.size", "16384" }
    };
});

var serviceProvider = services.BuildServiceProvider();
```

## Producing JSON Messages

```csharp
using JohBloch.ConfluentKafka.Clients.Services;
using Confluent.Kafka;

var producerClient = serviceProvider.GetRequiredService<IKafkaProducerClient>();

// Create your order event
var orderEvent = new OrderEvent
{
    OrderId = "ORD-2024-001",
    CustomerId = "CUST-12345",
    TotalAmount = 149.99m,
    Items = new List<OrderItem>
    {
        new() { ProductId = "PROD-001", Quantity = 2, Price = 49.99m },
        new() { ProductId = "PROD-002", Quantity = 1, Price = 50.01m }
    },
    CreatedAt = DateTime.UtcNow
};

// Produce with JSON serialization
var result = await producerClient.ProduceAsync(
    topic: "orders",
    key: orderEvent.OrderId,
    value: orderEvent,
    serializationType: SerializationType.Json
);

Console.WriteLine($"Order {orderEvent.OrderId} published to offset {result.Offset}");
```

## Consuming JSON Messages

```csharp
using JohBloch.ConfluentKafka.Clients.Services;

var consumerClient = serviceProvider.GetRequiredService<IKafkaConsumerClient>();

// Initialize consumer
await consumerClient.InitializeConsumer(
    topics: new[] { "orders" },
    serializationType: SerializationType.Json
);

// Start consuming
await foreach (var message in consumerClient.ConsumeAsync<OrderEvent>())
{
    try
    {
        var order = message.Value;
        
        Console.WriteLine($"Processing order {order.OrderId}");
        Console.WriteLine($"Customer: {order.CustomerId}");
        Console.WriteLine($"Total: ${order.TotalAmount:F2}");
        Console.WriteLine($"Items: {order.Items.Count}");
        
        // Process order...
        await ProcessOrderAsync(order);
        
        // Commit after successful processing
        consumerClient.Commit(message);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Failed to process order: {ex.Message}");
        
        // Send to DLQ with context
        await producerClient.SendToDeadLetterQueueAsync(
            message: message,
            exception: ex,
            reason: $"Order processing failed: {ex.Message}"
        );
    }
}
```

## Custom JSON Serialization Options

```csharp
using System.Text.Json;

// If you need custom JSON serialization options, configure them in your model:
var options = new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    WriteIndented = false, // Compact JSON for better performance
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
};

// The library uses System.Text.Json with sensible defaults
```

## JSON Schema Validation

JSON messages are validated against schemas in the Schema Registry:

```csharp
// The library automatically:
// 1. Registers JSON schema on first produce
// 2. Validates messages against registered schema
// 3. Includes schema ID in message header (wire format: magic byte + schema ID + payload)

// You can verify schema registration:
// curl http://localhost:8081/subjects/orders-value/versions/latest
```

## Handling Nested Objects

```csharp
public class ComplexEvent
{
    public string EventId { get; set; } = string.Empty;
    
    // Nested object
    public Address ShippingAddress { get; set; } = new();
    
    // Collection of nested objects
    public List<Payment> Payments { get; set; } = new();
    
    // Dictionary
    public Dictionary<string, string> Metadata { get; set; } = new();
}

public class Address
{
    public string Street { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string PostalCode { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
}

public class Payment
{
    public string PaymentMethod { get; set; } = string.Empty;
    public decimal Amount { get; set; }
}

// All nested structures serialize/deserialize automatically
```

## Performance Optimization

```csharp
// For high-throughput scenarios:
options.GlobalProducerConfig = new Dictionary<string, string>
{
    { "compression.type", "gzip" },    // Reduces message size
    { "batch.size", "65536" },         // Larger batches
    { "linger.ms", "10" },             // Wait 10ms to batch messages
    { "buffer.memory", "67108864" }    // 64MB buffer
};

// For low-latency scenarios:
options.GlobalProducerConfig = new Dictionary<string, string>
{
    { "linger.ms", "0" },              // Send immediately
    { "compression.type", "none" },    // No compression overhead
    { "acks", "1" }                    // Leader acknowledgment only
};
```

## Best Practices

1. **Property Names**: Use `[JsonPropertyName]` for consistent naming (snake_case or camelCase)
2. **Null Handling**: Use nullable types (`string?`, `int?`) for optional fields
3. **Date/Time**: Always use UTC (`DateTime.UtcNow`) for timestamps
4. **Validation**: Validate models before producing to avoid schema errors
5. **Compression**: Use gzip compression for JSON to reduce network usage
6. **Schema Evolution**: Add new optional fields at the end, never remove required fields

## Troubleshooting

### Serialization Error
```
Error: JsonException during serialization
```
**Solution**: Ensure all properties have getters/setters and model is serializable

### Schema Registry Error
```
Error: Schema registration failed
```
**Solution**: Check Schema Registry connectivity and schema compatibility settings

### Deserialization Error
```
Error: Failed to deserialize JSON message
```
**Solution**: Verify consumer model matches producer schema (same property names and types)

## See Also

- [Avro Example](AvroExample.md)
- [Dead Letter Queue Documentation](DeadLetterQueue.md)
- [API Key Authentication](ApiKeyExample.md)
