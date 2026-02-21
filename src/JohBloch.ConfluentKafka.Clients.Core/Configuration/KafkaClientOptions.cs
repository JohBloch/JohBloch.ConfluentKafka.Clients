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
    /// Schema Registry API key (BasicAuth username).
    /// Commonly used with Confluent Cloud if Schema Registry OAuth is not configured.
    /// </summary>
    public string? SchemaRegistryApiKey { get; set; }

    /// <summary>
    /// Schema Registry API secret (BasicAuth password).
    /// Commonly used with Confluent Cloud if Schema Registry OAuth is not configured.
    /// </summary>
    public string? SchemaRegistryApiSecret { get; set; }

    /// <summary>
    /// Consumer Group ID.
    /// </summary>
    public string GroupId { get; set; } = string.Empty;

    /// <summary>
    /// Schema Registry OAuth Token Endpoint URL (if using OAuth).
    /// </summary>
    public string? SchemaRegistryOauthTokenEndpoint { get; set; }

    /// <summary>
    /// Schema Registry OAuth Client ID (if using OAuth).
    /// </summary>
    public string? SchemaRegistryOauthClientId { get; set; }

    /// <summary>
    /// Schema Registry OAuth Client Secret (if using OAuth).
    /// </summary>
    public string? SchemaRegistryOauthClientSecret { get; set; }

    /// <summary>
    /// Schema Registry OAuth Scope (optional, if using OAuth).
    /// </summary>
    public string? SchemaRegistryOauthScope { get; set; }

    /// <summary>
    /// Kafka OAuth settings using a nested configuration section.
    /// Supports configuration binding like <c>Kafka:OAuth:ClientId</c> (or env var <c>Kafka__OAuth__ClientId</c>).
    /// </summary>
    public KafkaOAuthOptions OAuth { get; set; } = new();

    /// <summary>
    /// Optional Schema Registry OAuth bearer token extension: logical cluster id (e.g. Confluent Cloud 'lsrc-...').
    /// When provided, this will be sent as the OAuth token extension key 'logicalCluster' for Schema Registry.
    /// </summary>
    public string? SchemaRegistryOauthLogicalCluster { get; set; }

    /// <summary>
    /// Optional Schema Registry OAuth bearer token extension: identity pool id (e.g. Confluent Cloud identity pool).
    /// When provided, this will be sent as the OAuth token extension key 'identityPoolId' for Schema Registry.
    /// </summary>
    public string? SchemaRegistryOauthIdentityPoolId { get; set; }

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

/// <summary>
/// Kafka OAuth options intended for configuration binding under <c>Kafka:OAuth</c>.
/// </summary>
public sealed class KafkaOAuthOptions
{
    /// <summary>
    /// Creates a new instance of <see cref="KafkaOAuthOptions"/>.
    /// </summary>
    public KafkaOAuthOptions()
    {
    }

    /// <summary>
    /// OAuth client id.
    /// </summary>
    public string? ClientId { get; set; }

    /// <summary>
    /// OAuth client secret.
    /// </summary>
    public string? ClientSecret { get; set; }

    /// <summary>
    /// OAuth scope (optional).
    /// </summary>
    public string? Scope { get; set; }

    /// <summary>
    /// OAuth token endpoint URL.
    /// Matches common naming in appsettings: <c>TokenEndpointUrl</c>.
    /// </summary>
    public string? TokenEndpointUrl { get; set; }

    /// <summary>
    /// Optional Confluent Cloud extension: logical cluster id (e.g. lkc-...).
    /// </summary>
    public string? LogicalCluster { get; set; }

    /// <summary>
    /// Optional Confluent Cloud extension: identity pool id.
    /// </summary>
    public string? IdentityPoolId { get; set; }
}
