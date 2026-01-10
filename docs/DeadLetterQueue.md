# Dead Letter Queue (DLQ) Support

The library has dedicated DLQ functionality for handling failed messages with automatic JSON serialization for Grafana/Loki.

## Features

✅ **Automatic DLQ topic naming** - Configurable pattern (`dlq-{topic}` default)  
✅ **JSON schema** - Optimized for Grafana/Loki readability  
✅ **Metadata enrichment** - Automatic partition, offset, timestamp, hostname  
✅ **Flexible API** - Send direct DLQ message or auto-build from ConsumeResult  
✅ **Per-topic or shared DLQ** - Configurable via pattern  

## Configuration

```csharp
var producerOptions = new Dictionary<string, KafkaProducerOptions>
{
    ["default"] = new KafkaProducerOptions
    {
        BootstrapServers = "localhost:9092",
        Topic = "orders",
        ApplicationId = "order-service",
        
        // DLQ configuration
        DeadLetterQueueTopicPattern = "dlq-{topic}",  // Default: one DLQ per topic
        IncludeStackTraceInDlq = false                 // Default: false (can be large)
    }
};
```

### DLQ Topic Patterns

```csharp
// One DLQ per topic (recommended)
DeadLetterQueueTopicPattern = "dlq-{topic}"
// Result: orders → dlq-orders, customers → dlq-customers

// Shared DLQ for all topics
DeadLetterQueueTopicPattern = "dlq-myapp"
// Result: all errors go to dlq-myapp

// With environment
DeadLetterQueueTopicPattern = "dlq-prod-{topic}"
// Result: orders → dlq-prod-orders
```

## Usage: Automatic DLQ from ConsumeResult

The simplest way - let the library build the DLQ message automatically:

```csharp
var consumer = new KafkaConsumerClient(consumerOptions, schemaRegistryOptions, loggerFactory);
var producer = new KafkaProducerClient(producerOptions, securityTokenProvider, schemaRegistryFactory, logger);

try
{
    var result = await consumer.ConsumeAsync<OrderMessage>(
        topics: new[] { "orders" },
        deserializer: deserializer,
        cancellationToken: cancellationToken
    );

    foreach (var consumeResult in result.Messages)
    {
        try
        {
            // Process message
            await ProcessOrderAsync(consumeResult.Message.Value);
        }
        catch (Exception ex)
        {
            // Send automatically to DLQ
            await producer.SendToDeadLetterQueueAsync(
                originalMessage: consumeResult,
                exception: ex,
                retryCount: 0,
                producerKey: "default"
            );
        }
    }
}
catch (Exception ex)
{
    _logger.LogError(ex, "Failed to consume messages");
}
```

## Usage: Manual DLQ message

For more control, build the DLQ message yourself:

```csharp
var dlqMessage = new DeadLetterMessage
{
    OriginalTopic = "orders",
    Partition = 2,
    Offset = 12345,
    FailedAt = DateTime.UtcNow,
    ErrorMessage = "Invalid order format",
    ErrorType = "ValidationException",
    Severity = "error",
    RetryCount = 3,
    OriginalKey = "order-123",
    OriginalValueBase64 = Convert.ToBase64String(originalBytes),
    ApplicationName = "order-service",
    Metadata = new Dictionary<string, string>
    {
        ["validation_field"] = "customer_id",
        ["trace_id"] = "abc-123"
    }
};

await producer.SendToDeadLetterQueueAsync(
    dlqMessage: dlqMessage,
    key: "order-123",
    producerKey: "default"
);
```

## DLQ Message Model

```csharp
public class DeadLetterMessage
{
    public string OriginalTopic { get; set; }        // Topic where error occurred
    public int Partition { get; set; }                // Partition number
    public long Offset { get; set; }                  // Offset
    public DateTime FailedAt { get; set; }            // Timestamp of failure
    public string ErrorMessage { get; set; }          // Error message
    public string ErrorType { get; set; }             // Exception type
    public string? StackTrace { get; set; }           // Stack trace (optional)
    public int RetryCount { get; set; }               // Number of attempts
    public string Severity { get; set; }              // error/warning/critical
    public string? OriginalKey { get; set; }          // Original message key
    public string? OriginalValueBase64 { get; set; }  // Original message (base64)
    public Dictionary<string, string> Headers { get; set; }   // Kafka headers
    public Dictionary<string, string> Metadata { get; set; }  // Additional metadata
    public string? ApplicationName { get; set; }      // App that sent to DLQ
    public string? Hostname { get; set; }             // Host where error occurred
}
```

## Grafana Loki Integration

DLQ messages are in JSON format optimized for Grafana Loki:

### Loki Configuration

```yaml
# promtail-config.yaml
scrape_configs:
  - job_name: kafka-dlq
    kafka:
      brokers:
        - localhost:9092
      topics:
        - dlq-.*  # Match alle DLQ topics
      labels:
        job: kafka-dlq
```

### LogQL Queries

```logql
# All DLQ messages
{job="kafka-dlq"}

# Filter by original topic
{job="kafka-dlq"} | json | original_topic = "orders"

# Filter by error type
{job="kafka-dlq"} | json | error_type = "ValidationException"

# Show error rate over time
sum(rate({job="kafka-dlq"}[5m])) by (original_topic)

# Critical errors
{job="kafka-dlq"} | json | severity = "critical"

# Retry count over 3
{job="kafka-dlq"} | json | retry_count > 3
```

### Grafana Dashboard Example

```json
{
  "panels": [
    {
      "title": "DLQ Error Rate by Topic",
      "targets": [
        {
          "expr": "sum(rate({job=\"kafka-dlq\"}[5m])) by (original_topic)"
        }
      ]
    },
    {
      "title": "Recent DLQ Errors",
      "targets": [
        {
          "expr": "{job=\"kafka-dlq\"} | json | line_format \"{{.original_topic}}: {{.error_message}}\""
        }
      ]
    }
  ]
}
```

## Best Practices

### ✅ One DLQ per topic
```csharp
DeadLetterQueueTopicPattern = "dlq-{topic}"
```
- Better isolation
- Easier reprocessing
- Better partitioning strategy

### ✅ Include trace IDs
```csharp
Metadata = new Dictionary<string, string>
{
    ["trace_id"] = Activity.Current?.Id,
    ["span_id"] = Activity.Current?.SpanId.ToString()
}
```

### ✅ Severity levels
```csharp
Severity = error.GetType().Name switch
{
    "ValidationException" => "warning",
    "TimeoutException" => "error",
    "SecurityException" => "critical",
    _ => "error"
}
```

### ✅ Monitor DLQ metrics
- Alert når DLQ får beskeder
- Track error types
- Monitor retry counts

### ⚠️ Stack traces
```csharp
// Only in development - can be large
IncludeStackTraceInDlq = environment.IsDevelopment()
```

## Reprocessing from DLQ

```csharp
// Consumer for DLQ reprocessing
var dlqConsumer = new KafkaConsumerClient(dlqConsumerOptions, ...);
var dlqDeserializer = deserializerFactory.Create<DeadLetterMessage>(SchemaType.Json);

var result = await dlqConsumer.ConsumeAsync<DeadLetterMessage>(
    topics: new[] { "dlq-orders" },
    deserializer: dlqDeserializer,
    cancellationToken: ct
);

foreach (var msg in result.Messages)
{
    var dlqMessage = msg.Message.Value;
    
    // Decode original message
    var originalBytes = Convert.FromBase64String(dlqMessage.OriginalValueBase64);
    var originalMessage = JsonSerializer.Deserialize<OrderMessage>(originalBytes);
    
    // Retry processing
    try
    {
        await ProcessOrderAsync(originalMessage);
        // Success - can delete from DLQ
    }
    catch (Exception ex)
    {
        // Still failing - increment retry count or give up
    }
}
```

## See also

- [KafkaProducerClient documentation](../README.md)
- [Schema Registry Integration](./SchemaRegistry.md)
- [Grafana Loki documentation](https://grafana.com/docs/loki/)
