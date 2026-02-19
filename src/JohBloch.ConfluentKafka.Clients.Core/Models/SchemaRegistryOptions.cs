namespace JohBloch.ConfluentKafka.Clients.Models;

/// <summary>
/// Configuration for Confluent Schema Registry.
/// </summary>
public sealed class SchemaRegistryOptions
{
    /// <summary>Schema Registry base URL.</summary>
    public string Url { get; set; } = string.Empty;

    /// <summary>
    /// Schema Registry API key (BasicAuth username).
    /// Used by providers like Confluent Cloud when OAuth is not used.
    /// </summary>
    public string ApiKey { get; set; } = string.Empty;

    /// <summary>
    /// Schema Registry API secret (BasicAuth password).
    /// Used by providers like Confluent Cloud when OAuth is not used.
    /// </summary>
    public string ApiSecret { get; set; } = string.Empty;

    /// <summary>OAuth client id.</summary>
    public string ClientId { get; set; } = string.Empty;

    /// <summary>OAuth client secret.</summary>
    public string ClientSecret { get; set; } = string.Empty;

    /// <summary>OAuth scope.</summary>
    public string Scope { get; set; } = string.Empty;

    /// <summary>Logical cluster id used as OAuth extension.</summary>
    public string LogicalCluster { get; set; } = string.Empty;

    /// <summary>OAuth token endpoint URL.</summary>
    public string TokenEndpointUrl { get; set; } = string.Empty;

    /// <summary>Identity pool id (optional).</summary>
    public string IdentityPoolId { get; set; } = string.Empty;
}
