namespace JohBloch.ConfluentKafka.Clients.Models;

/// <summary>
/// Configuration for Kafka consumers.
/// </summary>
public sealed class KafkaConsumerOptions
{
    /// <summary>
    /// Security mode for this consumer.
    /// - <see cref="KafkaConsumerSecurityMode.OAuth"/> uses the global Kafka OAuth settings (same as producer)
    ///   via <see cref="JohBloch.ConfluentKafka.Clients.Security.ISecurityTokenProvider"/>.
    /// - <see cref="KafkaConsumerSecurityMode.ApiKeySecret"/> uses SASL/PLAIN with api_key/api_secret.
    /// </summary>
    public KafkaConsumerSecurityMode SecurityMode { get; set; } = KafkaConsumerSecurityMode.Auto;

    /// <summary>
    /// Kafka API key for this consumer (used when <see cref="SecurityMode"/> is ApiKeySecret).
    /// Maps to SASL username.
    /// </summary>
    public string? ApiKey { get; set; }

    /// <summary>
    /// Kafka API secret for this consumer (used when <see cref="SecurityMode"/> is ApiKeySecret).
    /// Maps to SASL password.
    /// </summary>
    public string? ApiSecret { get; set; }

    /// <summary>Bootstrap servers for Kafka cluster.</summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>Consumer group id.</summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>Topic to consume from.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>
    /// Topics to consume from. If provided (non-empty), this takes precedence over <see cref="Topic"/>.
    /// </summary>
    public List<string> Topics { get; set; } = new();

    /// <summary>
    /// Returns the effective topic list for this consumer.
    /// If <see cref="Topics"/> contains one or more non-empty entries, it is used; otherwise <see cref="Topic"/> is used.
    /// </summary>
    public IReadOnlyList<string> GetTopics()
    {
        if (Topics is { Count: > 0 })
        {
            List<string> cleaned = Topics
                .Where(t => !string.IsNullOrWhiteSpace(t))
                .Select(t => t.Trim())
                .Distinct(StringComparer.Ordinal)
                .ToList();

            if (cleaned.Count > 0)
            {
                return cleaned;
            }
        }

        if (!string.IsNullOrWhiteSpace(Topic))
        {
            return new[] { Topic.Trim() };
        }

        return Array.Empty<string>();
    }

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

/// <summary>
/// Security configuration mode for a Kafka consumer.
/// </summary>
public enum KafkaConsumerSecurityMode
{
    /// <summary>
    /// Automatically select security:
    /// - Use ApiKeySecret when <see cref="KafkaConsumerOptions.ApiKey"/> and <see cref="KafkaConsumerOptions.ApiSecret"/> are set.
    /// - Otherwise, use OAuth when the registered security provider returns SASL settings.
    /// - Otherwise, no security.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// No SASL/OAuth security (plaintext).
    /// </summary>
    None = 1,

    /// <summary>
    /// Use OAuth2 (OAUTHBEARER) via the registered security provider.
    /// Uses the same global Kafka OAuth settings as producer.
    /// </summary>
    OAuth = 2,

    /// <summary>
    /// Use api_key/api_secret via SASL/PLAIN over SSL.
    /// </summary>
    ApiKeySecret = 3
}
