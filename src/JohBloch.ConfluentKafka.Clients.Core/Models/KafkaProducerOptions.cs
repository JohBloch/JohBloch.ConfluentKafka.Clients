namespace JohBloch.ConfluentKafka.Clients.Models;

/// <summary>
/// Configuration for Kafka producers.
/// </summary>
public sealed class KafkaProducerOptions
{
    /// <summary>Bootstrap servers for Kafka cluster.</summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>Topic to produce to.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>Application/client identifier used by producers.</summary>
    public string ApplicationId { get; set; } = string.Empty;

    /// <summary>Dead letter queue topic naming pattern. Use {topic} placeholder for original topic name.</summary>
    public string DeadLetterQueueTopicPattern { get; set; } = "dlq-{topic}";

    /// <summary>Whether to include stack trace in DLQ messages (can be large).</summary>
    public bool IncludeStackTraceInDlq { get; set; } = false;

    /// <summary>
    /// When enabled, delivery failures will automatically produce a Dead Letter message to the configured DLQ topic.
    /// The original send will still be reported as a failure, but the returned result will include DLQ status.
    /// </summary>
    public bool AutoDlqOnDeliveryFailure { get; set; } = false;

    /// <summary>Batch size in KB.</summary>
    public int BatchSizeKB { get; set; } = 32;

    /// <summary>Linger time in milliseconds.</summary>
    public int LingerMS { get; set; } = 100;

    /// <summary>Queue buffering max messages.</summary>
    public int QueueBufferMaxMessages { get; set; } = 50000;

    /// <summary>Compression type name (e.g., gzip, snappy).</summary>
    public string CompressionType { get; set; } = "none";

    /// <summary>Compression level if supported.</summary>
    public int CompressionLevel { get; set; } = 0;
}
