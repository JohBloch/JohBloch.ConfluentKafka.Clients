# Batch Processing Example

This guide demonstrates efficient batch processing for both producers and consumers to achieve high throughput.

## Prerequisites

- Kafka cluster with Schema Registry
- NuGet package: `JohBloch.ConfluentKafka.Clients`
- Understanding of Kafka batching concepts

## Why Batch Processing?

Batch processing provides significant performance benefits:
- **Higher Throughput**: Process multiple messages in one operation
- **Lower Latency**: Reduced network round trips
- **Better Resource Utilization**: Amortize overhead across multiple messages
- **Cost Efficiency**: Fewer API calls and network operations

## Models

```csharp
public class Event
{
    public string EventId { get; set; } = string.Empty;
    public string EventType { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; }
    public Dictionary<string, object> Data { get; set; } = new();
}

public class BatchResult
{
    public int SuccessCount { get; set; }
    public int FailureCount { get; set; }
    public List<string> FailedEventIds { get; set; } = new();
    public TimeSpan ProcessingTime { get; set; }
}
```

## Configuration for High Throughput

```csharp
using Microsoft.Extensions.DependencyInjection;
using JohBloch.ConfluentKafka.Clients.Configuration;

var services = new ServiceCollection();

services.AddKafkaClients(options =>
{
    options.BootstrapServers = "localhost:9092";
    options.SchemaRegistryUrl = "http://localhost:8081";
    options.GroupId = "batch-processing-group";
    
    // Optimized producer config for batching
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        // Batching settings
        { "batch.size", "65536" },              // 64 KB batch size
        { "linger.ms", "10" },                  // Wait 10ms to fill batch
        { "compression.type", "snappy" },       // Fast compression
        { "buffer.memory", "67108864" },        // 64 MB buffer
        
        // Performance settings
        { "acks", "1" },                        // Leader ack only for speed
        { "max.in.flight.requests.per.connection", "5" },
        { "retries", "3" },
        
        // Idempotence for exactly-once semantics
        { "enable.idempotence", "true" }
    };
    
    // Optimized consumer config for batching
    options.ConsumerConfig = new Dictionary<string, string>
    {
        // Fetch settings for batching
        { "fetch.min.bytes", "10240" },         // Wait for 10 KB minimum
        { "fetch.wait.max.ms", "500" },         // Max wait 500ms
        { "max.partition.fetch.bytes", "1048576" }, // 1 MB per partition
        
        // Disable auto-commit for manual batch commits
        { "enable.auto.commit", "false" },
        
        // Session settings
        { "session.timeout.ms", "30000" },
        { "max.poll.interval.ms", "300000" }    // 5 minutes for batch processing
    };
});

var serviceProvider = services.BuildServiceProvider();
```

## Batch Producer

### Basic Batch Produce

```csharp
using JohBloch.ConfluentKafka.Clients.Services;
using System.Diagnostics;

public class BatchProducer
{
    private readonly IKafkaProducerClient _producerClient;
    private readonly ILogger<BatchProducer> _logger;

    public BatchProducer(
        IKafkaProducerClient producerClient,
        ILogger<BatchProducer> logger)
    {
        _producerClient = producerClient;
        _logger = logger;
    }

    public async Task<BatchResult> ProduceBatchAsync(
        string topic,
        IEnumerable<Event> events)
    {
        var stopwatch = Stopwatch.StartNew();
        var result = new BatchResult();
        var tasks = new List<Task<DeliveryResult<string, Event>>>();

        foreach (var evt in events)
        {
            try
            {
                // Queue messages asynchronously
                var task = _producerClient.ProduceAsync(
                    topic: topic,
                    key: evt.EventId,
                    value: evt,
                    serializationType: SerializationType.Json
                );
                
                tasks.Add(task);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to queue event {EventId}", evt.EventId);
                result.FailureCount++;
                result.FailedEventIds.Add(evt.EventId);
            }
        }

        // Wait for all messages to be acknowledged
        var deliveryResults = await Task.WhenAll(tasks);
        
        result.SuccessCount = deliveryResults.Length;
        result.ProcessingTime = stopwatch.Elapsed;

        _logger.LogInformation(
            "Batch produced {Success} messages in {Duration}ms ({Throughput} msg/s)",
            result.SuccessCount,
            result.ProcessingTime.TotalMilliseconds,
            result.SuccessCount / result.ProcessingTime.TotalSeconds
        );

        return result;
    }
}
```

### Advanced Batch Producer with Partitioning

```csharp
public class AdvancedBatchProducer
{
    private readonly IKafkaProducerClient _producerClient;
    private readonly ILogger<AdvancedBatchProducer> _logger;
    private const int MaxBatchSize = 1000;
    private const int MaxConcurrentBatches = 5;

    public AdvancedBatchProducer(
        IKafkaProducerClient producerClient,
        ILogger<AdvancedBatchProducer> logger)
    {
        _producerClient = producerClient;
        _logger = logger;
    }

    public async Task<BatchResult> ProduceLargeBatchAsync(
        string topic,
        IAsyncEnumerable<Event> events)
    {
        var stopwatch = Stopwatch.StartNew();
        var overallResult = new BatchResult();
        var semaphore = new SemaphoreSlim(MaxConcurrentBatches);
        var batchTasks = new List<Task<BatchResult>>();

        await foreach (var batch in CreateBatchesAsync(events, MaxBatchSize))
        {
            await semaphore.WaitAsync();

            var batchTask = Task.Run(async () =>
            {
                try
                {
                    return await ProduceBatchAsync(topic, batch);
                }
                finally
                {
                    semaphore.Release();
                }
            });

            batchTasks.Add(batchTask);
        }

        var batchResults = await Task.WhenAll(batchTasks);

        // Aggregate results
        foreach (var batchResult in batchResults)
        {
            overallResult.SuccessCount += batchResult.SuccessCount;
            overallResult.FailureCount += batchResult.FailureCount;
            overallResult.FailedEventIds.AddRange(batchResult.FailedEventIds);
        }

        overallResult.ProcessingTime = stopwatch.Elapsed;

        _logger.LogInformation(
            "Produced {Success} messages, {Failures} failures in {Duration}s ({Throughput} msg/s)",
            overallResult.SuccessCount,
            overallResult.FailureCount,
            overallResult.ProcessingTime.TotalSeconds,
            overallResult.SuccessCount / overallResult.ProcessingTime.TotalSeconds
        );

        return overallResult;
    }

    private async Task<BatchResult> ProduceBatchAsync(string topic, List<Event> events)
    {
        var result = new BatchResult();
        var tasks = new List<Task>();

        foreach (var evt in events)
        {
            try
            {
                var task = _producerClient.ProduceAsync(
                    topic: topic,
                    key: evt.EventId,
                    value: evt,
                    serializationType: SerializationType.Json
                );

                tasks.Add(task);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to queue event {EventId}", evt.EventId);
                result.FailureCount++;
                result.FailedEventIds.Add(evt.EventId);
            }
        }

        await Task.WhenAll(tasks);
        result.SuccessCount = tasks.Count;

        return result;
    }

    private async IAsyncEnumerable<List<Event>> CreateBatchesAsync(
        IAsyncEnumerable<Event> events,
        int batchSize)
    {
        var batch = new List<Event>(batchSize);

        await foreach (var evt in events)
        {
            batch.Add(evt);

            if (batch.Count >= batchSize)
            {
                yield return batch;
                batch = new List<Event>(batchSize);
            }
        }

        if (batch.Count > 0)
        {
            yield return batch;
        }
    }
}
```

## Batch Consumer

### Basic Batch Consumer

```csharp
public class BatchConsumer
{
    private readonly IKafkaConsumerClient _consumerClient;
    private readonly ILogger<BatchConsumer> _logger;
    private const int BatchSize = 100;
    private const int BatchTimeoutMs = 5000;

    public BatchConsumer(
        IKafkaConsumerClient consumerClient,
        ILogger<BatchConsumer> logger)
    {
        _consumerClient = consumerClient;
        _logger = logger;
    }

    public async Task ConsumeBatchesAsync(
        string topic,
        CancellationToken cancellationToken)
    {
        await _consumerClient.InitializeConsumer(
            topics: new[] { topic },
            serializationType: SerializationType.Json
        );

        var batch = new List<ConsumeResult<string, Event>>();
        var batchStartTime = DateTime.UtcNow;

        await foreach (var message in _consumerClient.ConsumeAsync<Event>(cancellationToken))
        {
            batch.Add(message);

            var batchDuration = DateTime.UtcNow - batchStartTime;
            var shouldProcess = batch.Count >= BatchSize || 
                               batchDuration.TotalMilliseconds >= BatchTimeoutMs;

            if (shouldProcess)
            {
                await ProcessBatchAsync(batch);
                
                // Commit last offset in batch
                _consumerClient.Commit(batch.Last());
                
                // Reset batch
                batch.Clear();
                batchStartTime = DateTime.UtcNow;
            }
        }
    }

    private async Task ProcessBatchAsync(List<ConsumeResult<string, Event>> batch)
    {
        var stopwatch = Stopwatch.StartNew();
        
        _logger.LogInformation("Processing batch of {Count} messages", batch.Count);

        try
        {
            // Process all messages in batch
            var processingTasks = batch.Select(msg => ProcessMessageAsync(msg.Value));
            await Task.WhenAll(processingTasks);

            _logger.LogInformation(
                "Batch processed {Count} messages in {Duration}ms ({Throughput} msg/s)",
                batch.Count,
                stopwatch.Elapsed.TotalMilliseconds,
                batch.Count / stopwatch.Elapsed.TotalSeconds
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Batch processing failed");
            throw;
        }
    }

    private async Task ProcessMessageAsync(Event evt)
    {
        // Your processing logic here
        await Task.Delay(10); // Simulate processing
    }
}
```

### Advanced Batch Consumer with Parallel Processing

```csharp
public class ParallelBatchConsumer
{
    private readonly IKafkaConsumerClient _consumerClient;
    private readonly ILogger<ParallelBatchConsumer> _logger;
    private const int BatchSize = 500;
    private const int BatchTimeoutMs = 5000;
    private const int MaxDegreeOfParallelism = 10;

    public ParallelBatchConsumer(
        IKafkaConsumerClient consumerClient,
        ILogger<ParallelBatchConsumer> logger)
    {
        _consumerClient = consumerClient;
        _logger = logger;
    }

    public async Task ConsumeBatchesAsync(
        string topic,
        CancellationToken cancellationToken)
    {
        await _consumerClient.InitializeConsumer(
            topics: new[] { topic },
            serializationType: SerializationType.Json
        );

        var batch = new List<ConsumeResult<string, Event>>();
        var batchStartTime = DateTime.UtcNow;

        await foreach (var message in _consumerClient.ConsumeAsync<Event>(cancellationToken))
        {
            batch.Add(message);

            var batchDuration = DateTime.UtcNow - batchStartTime;
            var shouldProcess = batch.Count >= BatchSize || 
                               batchDuration.TotalMilliseconds >= BatchTimeoutMs;

            if (shouldProcess)
            {
                await ProcessBatchInParallelAsync(batch);
                
                // Commit last offset
                _consumerClient.Commit(batch.Last());
                
                // Reset batch
                batch.Clear();
                batchStartTime = DateTime.UtcNow;
            }
        }
    }

    private async Task ProcessBatchInParallelAsync(
        List<ConsumeResult<string, Event>> batch)
    {
        var stopwatch = Stopwatch.StartNew();
        
        _logger.LogInformation("Processing batch of {Count} messages in parallel", batch.Count);

        var options = new ParallelOptions
        {
            MaxDegreeOfParallelism = MaxDegreeOfParallelism,
            CancellationToken = CancellationToken.None
        };

        try
        {
            await Parallel.ForEachAsync(batch, options, async (message, ct) =>
            {
                try
                {
                    await ProcessMessageAsync(message.Value);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process message {Key}", message.Key);
                    throw;
                }
            });

            _logger.LogInformation(
                "Batch processed {Count} messages in {Duration}ms ({Throughput} msg/s)",
                batch.Count,
                stopwatch.Elapsed.TotalMilliseconds,
                batch.Count / stopwatch.Elapsed.TotalSeconds
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Batch processing failed");
            throw;
        }
    }

    private async Task ProcessMessageAsync(Event evt)
    {
        // Your processing logic here
        await Task.Delay(10); // Simulate processing
    }
}
```

### Batch Consumer with Database Bulk Insert

```csharp
using System.Data;
using Dapper;

public class DatabaseBatchConsumer
{
    private readonly IKafkaConsumerClient _consumerClient;
    private readonly IDbConnection _dbConnection;
    private readonly ILogger<DatabaseBatchConsumer> _logger;
    private const int BatchSize = 1000;

    public DatabaseBatchConsumer(
        IKafkaConsumerClient consumerClient,
        IDbConnection dbConnection,
        ILogger<DatabaseBatchConsumer> logger)
    {
        _consumerClient = consumerClient;
        _dbConnection = dbConnection;
        _logger = logger;
    }

    public async Task ConsumeBatchesAsync(
        string topic,
        CancellationToken cancellationToken)
    {
        await _consumerClient.InitializeConsumer(
            topics: new[] { topic },
            serializationType: SerializationType.Json
        );

        var batch = new List<ConsumeResult<string, Event>>();

        await foreach (var message in _consumerClient.ConsumeAsync<Event>(cancellationToken))
        {
            batch.Add(message);

            if (batch.Count >= BatchSize)
            {
                await BulkInsertAsync(batch);
                
                // Commit after successful DB insert
                _consumerClient.Commit(batch.Last());
                
                batch.Clear();
            }
        }
    }

    private async Task BulkInsertAsync(List<ConsumeResult<string, Event>> batch)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            using var transaction = _dbConnection.BeginTransaction();

            var events = batch.Select(m => m.Value).ToList();

            // Bulk insert using Dapper
            await _dbConnection.ExecuteAsync(
                @"INSERT INTO Events (EventId, EventType, Timestamp, Data)
                  VALUES (@EventId, @EventType, @Timestamp, @Data)",
                events,
                transaction
            );

            transaction.Commit();

            _logger.LogInformation(
                "Bulk inserted {Count} events in {Duration}ms ({Throughput} msg/s)",
                batch.Count,
                stopwatch.Elapsed.TotalMilliseconds,
                batch.Count / stopwatch.Elapsed.TotalSeconds
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Bulk insert failed");
            throw;
        }
    }
}
```

## Performance Optimization

### Producer Optimization

```csharp
// High throughput configuration
options.GlobalProducerConfig = new Dictionary<string, string>
{
    { "batch.size", "131072" },             // 128 KB batches
    { "linger.ms", "20" },                  // Wait 20ms to accumulate
    { "compression.type", "lz4" },          // Fast compression
    { "buffer.memory", "134217728" },       // 128 MB buffer
    { "max.in.flight.requests.per.connection", "5" },
    { "acks", "1" }                         // Leader ack only
};

// Low latency configuration
options.GlobalProducerConfig = new Dictionary<string, string>
{
    { "batch.size", "16384" },              // 16 KB batches
    { "linger.ms", "0" },                   // Send immediately
    { "compression.type", "none" },         // No compression
    { "acks", "1" }
};
```

### Consumer Optimization

```csharp
// High throughput configuration
options.ConsumerConfig = new Dictionary<string, string>
{
    { "fetch.min.bytes", "102400" },        // 100 KB minimum
    { "fetch.wait.max.ms", "500" },         // Wait up to 500ms
    { "max.partition.fetch.bytes", "2097152" }, // 2 MB per partition
    { "enable.auto.commit", "false" }       // Manual commit for batching
};
```

## Monitoring and Metrics

```csharp
public class BatchMetrics
{
    private static readonly Histogram BatchSize = Metrics.CreateHistogram(
        "batch_size",
        "Size of processed batches"
    );

    private static readonly Histogram BatchProcessingDuration = Metrics.CreateHistogram(
        "batch_processing_duration_seconds",
        "Batch processing duration"
    );

    private static readonly Counter BatchesProcessed = Metrics.CreateCounter(
        "batches_processed_total",
        "Total batches processed",
        new CounterConfiguration { LabelNames = new[] { "status" } }
    );

    public static void RecordBatchSize(int size)
    {
        BatchSize.Observe(size);
    }

    public static void RecordProcessingDuration(TimeSpan duration)
    {
        BatchProcessingDuration.Observe(duration.TotalSeconds);
    }

    public static void RecordSuccess()
    {
        BatchesProcessed.WithLabels("success").Inc();
    }

    public static void RecordFailure()
    {
        BatchesProcessed.WithLabels("failure").Inc();
    }
}
```

## Best Practices

1. **Batch Size**: Balance between throughput and latency (100-1000 messages)
2. **Timeout**: Set batch timeout to avoid indefinite waiting
3. **Parallel Processing**: Use parallel processing for CPU-intensive tasks
4. **Error Handling**: Handle failures gracefully, consider partial batch failures
5. **Commit Strategy**: Commit after successful batch processing
6. **Memory Management**: Monitor memory usage with large batches
7. **Backpressure**: Implement backpressure when processing is slower than consumption
8. **Monitoring**: Track batch sizes, processing times, and throughput

## Performance Comparison

```
Single Message Processing:
- Throughput: ~1,000 msg/s
- Latency: 50-100ms per message

Batch Processing (100 messages):
- Throughput: ~10,000 msg/s (10x improvement)
- Latency: 500ms per batch (5ms per message)

Batch Processing (1000 messages):
- Throughput: ~50,000 msg/s (50x improvement)
- Latency: 2000ms per batch (2ms per message)
```

## Testing

```csharp
[Fact]
public async Task Should_Process_Batch_Successfully()
{
    // Arrange
    var events = Enumerable.Range(1, 100)
        .Select(i => new Event { EventId = $"EVT-{i}", EventType = "Test" })
        .ToList();

    // Act
    var result = await batchProducer.ProduceBatchAsync("test-topic", events);

    // Assert
    Assert.Equal(100, result.SuccessCount);
    Assert.Equal(0, result.FailureCount);
}

[Fact]
public async Task Should_Process_Large_Batch_In_Parallel()
{
    // Arrange
    var events = Enumerable.Range(1, 10000)
        .Select(i => new Event { EventId = $"EVT-{i}", EventType = "Test" });

    // Act
    var stopwatch = Stopwatch.StartNew();
    await batchProducer.ProduceLargeBatchAsync("test-topic", events.ToAsyncEnumerable());
    stopwatch.Stop();

    // Assert
    Assert.True(stopwatch.Elapsed.TotalSeconds < 5, "Should complete in less than 5 seconds");
}
```

## See Also

- [Multi-Topic Example](MultiTopicExample.md)
- [JSON Example](JsonExample.md)
- [Avro Example](AvroExample.md)
- [Dead Letter Queue Documentation](DeadLetterQueue.md)
