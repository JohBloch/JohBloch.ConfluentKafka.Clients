namespace JohBloch.ConfluentKafka.Clients.Models;

/// <summary>
/// Configuration for Kafka consumers.
/// </summary>
public sealed class KafkaConsumerOptions
{
    /// <summary>Bootstrap servers for Kafka cluster.</summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>Consumer group id.</summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>Topic to consume from.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>Enable auto commit of offsets.</summary>
    public bool EnableAutoCommit { get; set; } = true;

    /// <summary>Auto offset reset policy.</summary>
    public string AutoOffsetReset { get; set; } = "earliest";

    /// <summary>Session timeout in milliseconds.</summary>
    public int SessionTimeoutMs { get; set; } = 45000;

    /// <summary>Heartbeat interval in milliseconds.</summary>
    public int HeartbeatIntervalMs { get; set; } = 3000;

    /// <summary>Default schema type to use for deserialization when auto-detection is disabled.</summary>
    public SchemaType DefaultSchemaType { get; set; } = SchemaType.Avro;

    /// <summary>Whether to automatically detect schema type from Schema Registry.</summary>
    public bool AutoDetectSchemaType { get; set; } = true;

    /// <summary>Per-topic schema type overrides. Key is topic name, value is schema type.</summary>
    public Dictionary<string, SchemaType> TopicSchemaTypes { get; set; } = new();
}
