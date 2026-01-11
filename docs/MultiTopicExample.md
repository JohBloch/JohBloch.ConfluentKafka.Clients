# Multi-Topic Example

This guide demonstrates working with multiple topics in a coordinated workflow, including order processing, retry logic, and Dead Letter Queue (DLQ) patterns.

## Prerequisites

- Kafka cluster with Schema Registry
- NuGet package: `JohBloch.ConfluentKafka.Clients`
- Understanding of [Dead Letter Queue concepts](DeadLetterQueue.md)

## Scenario Overview

A typical order processing workflow with:
- **orders** topic: Primary order events
- **orders-retry** topic: Failed orders for retry processing
- **orders-dlq** topic: Permanently failed orders (Dead Letter Queue)

## Define Your Models

```csharp
public class Order
{
    public string OrderId { get; set; } = string.Empty;
    public string CustomerId { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public List<OrderItem> Items { get; set; } = new();
    public DateTime CreatedAt { get; set; }
    public int RetryCount { get; set; } = 0;
}

public class OrderItem
{
    public string ProductId { get; set; } = string.Empty;
    public int Quantity { get; set; }
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
    options.GroupId = "order-processing-group";
    
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "acks", "all" },
        { "retries", "3" },
        { "max.in.flight.requests.per.connection", "5" },
        { "enable.idempotence", "true" }
    };
    
    // Per-topic producer configurations
    options.PerProducerConfigs = new Dictionary<string, Dictionary<string, string>>
    {
        ["orders"] = new Dictionary<string, string>
        {
            { "compression.type", "snappy" },
            { "batch.size", "16384" }
        },
        ["orders-retry"] = new Dictionary<string, string>
        {
            { "compression.type", "gzip" }
        }
    };
});

var serviceProvider = services.BuildServiceProvider();
```

## Producing to Multiple Topics

### Initial Order Creation

```csharp
using JohBloch.ConfluentKafka.Clients.Services;
using Confluent.Kafka;

var producerClient = serviceProvider.GetRequiredService<IKafkaProducerClient>();

// Create and publish order to primary topic
var order = new Order
{
    OrderId = $"ORD-{Guid.NewGuid()}",
    CustomerId = "CUST-12345",
    TotalAmount = 299.99m,
    Items = new List<OrderItem>
    {
        new() { ProductId = "PROD-001", Quantity = 2, Price = 99.99m },
        new() { ProductId = "PROD-002", Quantity = 1, Price = 100.01m }
    },
    CreatedAt = DateTime.UtcNow,
    RetryCount = 0
};

var result = await producerClient.ProduceAsync(
    topic: "orders",
    key: order.OrderId,
    value: order,
    serializationType: SerializationType.Json
);

Console.WriteLine($"Order {order.OrderId} published to offset {result.Offset}");
```

### Sending to Retry Topic

```csharp
// After failed processing, send to retry topic
async Task SendToRetryAsync(Order order, Exception exception)
{
    order.RetryCount++;
    
    var headers = new Headers
    {
        { "retry-count", Encoding.UTF8.GetBytes(order.RetryCount.ToString()) },
        { "original-topic", Encoding.UTF8.GetBytes("orders") },
        { "error-reason", Encoding.UTF8.GetBytes(exception.Message) },
        { "retry-timestamp", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("o")) }
    };
    
    await producerClient.ProduceAsync(
        topic: "orders-retry",
        key: order.OrderId,
        value: order,
        serializationType: SerializationType.Json,
        headers: headers
    );
    
    Console.WriteLine($"Order {order.OrderId} sent to retry (attempt {order.RetryCount})");
}
```

### Sending to Dead Letter Queue

```csharp
// After max retries exceeded, send to DLQ
async Task SendToDlqAsync(ConsumeResult<string, Order> message, Exception exception)
{
    await producerClient.SendToDeadLetterQueueAsync(
        message: message,
        exception: exception,
        reason: $"Max retries ({message.Value.RetryCount}) exceeded"
    );
    
    Console.WriteLine($"Order {message.Value.OrderId} sent to DLQ after {message.Value.RetryCount} retries");
}
```

## Consuming from Multiple Topics

### Primary Order Consumer

```csharp
using JohBloch.ConfluentKafka.Clients.Services;
using Microsoft.Extensions.Logging;

public class OrderProcessor
{
    private readonly IKafkaConsumerClient _consumerClient;
    private readonly IKafkaProducerClient _producerClient;
    private readonly ILogger<OrderProcessor> _logger;
    private const int MaxRetries = 3;

    public OrderProcessor(
        IKafkaConsumerClient consumerClient,
        IKafkaProducerClient producerClient,
        ILogger<OrderProcessor> logger)
    {
        _consumerClient = consumerClient;
        _producerClient = producerClient;
        _logger = logger;
    }

    public async Task ProcessOrdersAsync(CancellationToken cancellationToken)
    {
        await _consumerClient.InitializeConsumer(
            topics: new[] { "orders" },
            serializationType: SerializationType.Json
        );

        await foreach (var message in _consumerClient.ConsumeAsync<Order>(cancellationToken))
        {
            try
            {
                _logger.LogInformation("Processing order {OrderId}", message.Value.OrderId);
                
                // Validate order
                ValidateOrder(message.Value);
                
                // Process payment
                await ProcessPaymentAsync(message.Value);
                
                // Update inventory
                await UpdateInventoryAsync(message.Value);
                
                // Send confirmation
                await SendConfirmationAsync(message.Value);
                
                // Commit after successful processing
                _consumerClient.Commit(message);
                
                _logger.LogInformation("Order {OrderId} processed successfully", message.Value.OrderId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process order {OrderId}", message.Value.OrderId);
                
                if (message.Value.RetryCount < MaxRetries)
                {
                    // Send to retry topic
                    await SendToRetryAsync(message.Value, ex);
                }
                else
                {
                    // Max retries exceeded, send to DLQ
                    await SendToDlqAsync(message, ex);
                }
                
                // Commit to prevent reprocessing by this consumer
                _consumerClient.Commit(message);
            }
        }
    }

    private void ValidateOrder(Order order)
    {
        if (order.TotalAmount <= 0)
            throw new InvalidOperationException("Order total must be positive");
        
        if (order.Items.Count == 0)
            throw new InvalidOperationException("Order must contain at least one item");
    }

    private async Task ProcessPaymentAsync(Order order)
    {
        // Simulate payment processing
        await Task.Delay(100);
        
        // Simulate occasional payment failures
        if (order.TotalAmount > 1000 && Random.Shared.Next(10) < 2)
            throw new InvalidOperationException("Payment gateway timeout");
    }

    private async Task UpdateInventoryAsync(Order order)
    {
        // Simulate inventory update
        await Task.Delay(50);
    }

    private async Task SendConfirmationAsync(Order order)
    {
        // Simulate sending confirmation email
        await Task.Delay(30);
    }

    private async Task SendToRetryAsync(Order order, Exception exception)
    {
        order.RetryCount++;
        
        var headers = new Headers
        {
            { "retry-count", Encoding.UTF8.GetBytes(order.RetryCount.ToString()) },
            { "original-topic", Encoding.UTF8.GetBytes("orders") },
            { "error-reason", Encoding.UTF8.GetBytes(exception.Message) },
            { "retry-timestamp", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("o")) }
        };
        
        await _producerClient.ProduceAsync(
            topic: "orders-retry",
            key: order.OrderId,
            value: order,
            serializationType: SerializationType.Json,
            headers: headers
        );
    }

    private async Task SendToDlqAsync(ConsumeResult<string, Order> message, Exception exception)
    {
        await _producerClient.SendToDeadLetterQueueAsync(
            message: message,
            exception: exception,
            reason: $"Max retries ({message.Value.RetryCount}) exceeded"
        );
    }
}
```

### Retry Topic Consumer with Delay

```csharp
public class RetryProcessor
{
    private readonly IKafkaConsumerClient _consumerClient;
    private readonly IKafkaProducerClient _producerClient;
    private readonly ILogger<RetryProcessor> _logger;
    private const int MaxRetries = 3;
    private static readonly TimeSpan[] RetryDelays = 
    {
        TimeSpan.FromSeconds(30),   // First retry: 30 seconds
        TimeSpan.FromMinutes(5),    // Second retry: 5 minutes
        TimeSpan.FromMinutes(30)    // Third retry: 30 minutes
    };

    public RetryProcessor(
        IKafkaConsumerClient consumerClient,
        IKafkaProducerClient producerClient,
        ILogger<RetryProcessor> logger)
    {
        _consumerClient = consumerClient;
        _producerClient = producerClient;
        _logger = logger;
    }

    public async Task ProcessRetriesAsync(CancellationToken cancellationToken)
    {
        // Use different consumer group for retry processing
        await _consumerClient.InitializeConsumer(
            topics: new[] { "orders-retry" },
            serializationType: SerializationType.Json
        );

        await foreach (var message in _consumerClient.ConsumeAsync<Order>(cancellationToken))
        {
            try
            {
                // Check if enough time has passed for retry
                if (ShouldDelayRetry(message))
                {
                    _logger.LogInformation("Delaying retry for order {OrderId}", message.Value.OrderId);
                    continue; // Don't commit, will reprocess later
                }
                
                _logger.LogInformation("Retrying order {OrderId} (attempt {RetryCount})", 
                    message.Value.OrderId, message.Value.RetryCount);
                
                // Retry order processing
                await ProcessOrderAsync(message.Value);
                
                // Success! Commit the retry message
                _consumerClient.Commit(message);
                
                _logger.LogInformation("Order {OrderId} retry successful", message.Value.OrderId);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Retry failed for order {OrderId}", message.Value.OrderId);
                
                if (message.Value.RetryCount < MaxRetries)
                {
                    // Increment retry count and send back to retry topic
                    message.Value.RetryCount++;
                    await SendToRetryAsync(message.Value, ex);
                }
                else
                {
                    // Max retries exceeded, send to DLQ
                    await SendToDlqAsync(message, ex);
                }
                
                _consumerClient.Commit(message);
            }
        }
    }

    private bool ShouldDelayRetry(ConsumeResult<string, Order> message)
    {
        // Get retry timestamp from headers
        var timestampHeader = message.Message.Headers
            .FirstOrDefault(h => h.Key == "retry-timestamp");
        
        if (timestampHeader == null)
            return false;
        
        var retryTimestamp = DateTime.Parse(Encoding.UTF8.GetString(timestampHeader.GetValueBytes()));
        var retryDelay = RetryDelays[message.Value.RetryCount - 1];
        
        return DateTime.UtcNow < retryTimestamp.Add(retryDelay);
    }

    private async Task ProcessOrderAsync(Order order)
    {
        // Same processing logic as OrderProcessor
        await Task.Delay(100); // Simulate processing
    }

    private async Task SendToRetryAsync(Order order, Exception exception)
    {
        var headers = new Headers
        {
            { "retry-count", Encoding.UTF8.GetBytes(order.RetryCount.ToString()) },
            { "original-topic", Encoding.UTF8.GetBytes("orders") },
            { "error-reason", Encoding.UTF8.GetBytes(exception.Message) },
            { "retry-timestamp", Encoding.UTF8.GetBytes(DateTime.UtcNow.ToString("o")) }
        };
        
        await _producerClient.ProduceAsync(
            topic: "orders-retry",
            key: order.OrderId,
            value: order,
            serializationType: SerializationType.Json,
            headers: headers
        );
    }

    private async Task SendToDlqAsync(ConsumeResult<string, Order> message, Exception exception)
    {
        await _producerClient.SendToDeadLetterQueueAsync(
            message: message,
            exception: exception,
            reason: $"Max retries ({message.Value.RetryCount}) exceeded"
        );
    }
}
```

### DLQ Consumer for Monitoring

```csharp
public class DlqMonitor
{
    private readonly IKafkaConsumerClient _consumerClient;
    private readonly ILogger<DlqMonitor> _logger;

    public DlqMonitor(
        IKafkaConsumerClient consumerClient,
        ILogger<DlqMonitor> logger)
    {
        _consumerClient = consumerClient;
        _logger = logger;
    }

    public async Task MonitorDlqAsync(CancellationToken cancellationToken)
    {
        await _consumerClient.InitializeConsumer(
            topics: new[] { "orders-dlq" },
            serializationType: SerializationType.Json
        );

        await foreach (var message in _consumerClient.ConsumeAsync<DeadLetterMessage>(cancellationToken))
        {
            _logger.LogError(
                "DLQ Message: Topic={Topic}, Key={Key}, Reason={Reason}, Error={Error}",
                message.Value.OriginalTopic,
                message.Key,
                message.Value.Reason,
                message.Value.ErrorMessage
            );
            
            // Send alert to monitoring system (e.g., Slack, PagerDuty)
            await SendAlertAsync(message.Value);
            
            // Store in database for investigation
            await StoreForInvestigationAsync(message.Value);
            
            _consumerClient.Commit(message);
        }
    }

    private async Task SendAlertAsync(DeadLetterMessage dlqMessage)
    {
        // Send alert to monitoring system
        await Task.CompletedTask;
    }

    private async Task StoreForInvestigationAsync(DeadLetterMessage dlqMessage)
    {
        // Store in database for manual investigation
        await Task.CompletedTask;
    }
}
```

## Orchestrating Multiple Consumers

```csharp
using Microsoft.Extensions.Hosting;

public class OrderProcessingService : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<OrderProcessingService> _logger;

    public OrderProcessingService(
        IServiceProvider serviceProvider,
        ILogger<OrderProcessingService> logger)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Start multiple consumers in parallel
        var tasks = new[]
        {
            Task.Run(async () =>
            {
                using var scope = _serviceProvider.CreateScope();
                var processor = scope.ServiceProvider.GetRequiredService<OrderProcessor>();
                await processor.ProcessOrdersAsync(stoppingToken);
            }, stoppingToken),
            
            Task.Run(async () =>
            {
                using var scope = _serviceProvider.CreateScope();
                var retryProcessor = scope.ServiceProvider.GetRequiredService<RetryProcessor>();
                await retryProcessor.ProcessRetriesAsync(stoppingToken);
            }, stoppingToken),
            
            Task.Run(async () =>
            {
                using var scope = _serviceProvider.CreateScope();
                var dlqMonitor = scope.ServiceProvider.GetRequiredService<DlqMonitor>();
                await dlqMonitor.MonitorDlqAsync(stoppingToken);
            }, stoppingToken)
        };

        await Task.WhenAll(tasks);
    }
}

// Register in Program.cs
services.AddScoped<OrderProcessor>();
services.AddScoped<RetryProcessor>();
services.AddScoped<DlqMonitor>();
services.AddHostedService<OrderProcessingService>();
```

## Topic Naming Convention

```csharp
public static class TopicNames
{
    public const string Orders = "orders";
    public const string OrdersRetry = "orders-retry";
    public const string OrdersDlq = "orders-dlq";
    
    public static string GetRetryTopic(string baseTopic) => $"{baseTopic}-retry";
    public static string GetDlqTopic(string baseTopic) => $"{baseTopic}-dlq";
}
```

## Best Practices

1. **Separate Consumer Groups**: Use different consumer groups for primary, retry, and DLQ consumers
2. **Exponential Backoff**: Increase delay between retries (30s → 5m → 30m)
3. **Max Retry Limit**: Set a reasonable maximum (typically 3-5 retries)
4. **DLQ Monitoring**: Always monitor DLQ for critical failures
5. **Idempotency**: Design processing logic to be idempotent (safe to retry)
6. **Header Metadata**: Include rich context in message headers (retry count, timestamps, error reasons)
7. **Graceful Shutdown**: Handle cancellation tokens properly to avoid message loss

## Monitoring and Observability

```csharp
public class OrderMetrics
{
    private static readonly Counter OrdersProcessed = Metrics.CreateCounter(
        "orders_processed_total",
        "Total orders processed",
        new CounterConfiguration { LabelNames = new[] { "status" } }
    );
    
    private static readonly Histogram ProcessingDuration = Metrics.CreateHistogram(
        "order_processing_duration_seconds",
        "Order processing duration"
    );

    public static void RecordSuccess()
    {
        OrdersProcessed.WithLabels("success").Inc();
    }

    public static void RecordRetry()
    {
        OrdersProcessed.WithLabels("retry").Inc();
    }

    public static void RecordDlq()
    {
        OrdersProcessed.WithLabels("dlq").Inc();
    }

    public static IDisposable MeasureProcessing()
    {
        return ProcessingDuration.NewTimer();
    }
}
```

## Testing

```csharp
[Fact]
public async Task Should_Retry_Failed_Order()
{
    // Arrange
    var order = new Order { OrderId = "TEST-001", RetryCount = 0 };
    
    // Act
    await producerClient.ProduceAsync("orders", order.OrderId, order, SerializationType.Json);
    
    // Simulate failure and retry
    order.RetryCount++;
    await producerClient.ProduceAsync("orders-retry", order.OrderId, order, SerializationType.Json);
    
    // Assert
    Assert.Equal(1, order.RetryCount);
}
```

## See Also

- [Dead Letter Queue Documentation](DeadLetterQueue.md)
- [Batch Processing Example](BatchExample.md)
- [JSON Example](JsonExample.md)
- [Avro Example](AvroExample.md)
