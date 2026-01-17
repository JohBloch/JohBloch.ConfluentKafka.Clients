using JohBloch.ConfluentKafka.Clients.Models;

namespace JohBloch.ConfluentKafka.Clients.Configuration;

/// <summary>
/// Unified configuration options for Kafka clients setup.
/// </summary>
public class KafkaClientOptions
{
    /// <summary>
    /// Consumer configuration options.
    /// </summary>
    public KafkaConsumerOptions Consumer { get; set; } = new();

    /// <summary>
    /// Bootstrap servers for the Kafka cluster (comma-separated).
    /// </summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// URL for the Confluent Schema Registry.
    /// </summary>
    public string SchemaRegistryUrl { get; set; } = string.Empty;

    /// <summary>
    /// Consumer Group ID.
    /// </summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>
    /// OAuth Token Endpoint URL (if using OAuth).
    /// </summary>
    public string? OAuthTokenEndpoint { get; set; }

    /// <summary>
    /// OAuth Client ID (if using OAuth).
    /// </summary>
    public string? OAuthClientId { get; set; }

    /// <summary>
    /// OAuth Client Secret (if using OAuth).
    /// </summary>
    public string? OAuthClientSecret { get; set; }

    /// <summary>
    /// OAuth Scope (optional, if using OAuth).
    /// </summary>
    public string? OAuthScope { get; set; }

    /// <summary>
    /// Optional OAuth bearer token extension: logical cluster id (e.g. Confluent Cloud 'lkc-...').
    /// When provided, this will be sent as the OAuth OAUTHBEARER token extension key 'logicalCluster'.
    /// </summary>
    public string? OAuthLogicalCluster { get; set; }

    /// <summary>
    /// Optional OAuth bearer token extension: identity pool id (e.g. Confluent Cloud identity pool).
    /// When provided, this will be sent as the OAuth OAUTHBEARER token extension key 'identityPoolId'.
    /// </summary>
    public string? OAuthIdentityPoolId { get; set; }

    /// <summary>
    /// Configuration for specific logged producers (Topic, BatchSize, DLQ settings).
    /// Key is the producer name (e.g., "orders", "audit").
    /// </summary>
    public Dictionary<string, KafkaProducerOptions> Producers { get; set; } = new();

    /// <summary>
    /// Default global configuration for producers (librdkafka properties).
    /// </summary>
    public Dictionary<string, string> GlobalProducerConfig { get; set; } = new();

    /// <summary>
    /// Per-producer configuration overrides (key is producer name).
    /// </summary>
    public Dictionary<string, Dictionary<string, string>> PerProducerConfigs { get; set; } = new();

    /// <summary>
    /// Configuration for consumers.
    /// </summary>
    public Dictionary<string, string> ConsumerConfig { get; set; } = new();
}
