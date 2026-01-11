namespace JohBloch.ConfluentKafka.Clients.Models;

/// <summary>
/// Root options for Kafka clients.
/// </summary>
public sealed class KafkaOptions
{
    /// <summary>Producer options keyed by logical producer name.</summary>
    public Dictionary<string, KafkaProducerOptions> KafkaProducerOptions { get; set; } = new();

    /// <summary>Consumer options keyed by logical consumer name.</summary>
    public Dictionary<string, KafkaConsumerOptions> KafkaConsumerOptions { get; set; } = new();

    /// <summary>Schema Registry connectivity and auth options.</summary>
    public SchemaRegistryOptions SchemaRegistryOptions { get; set; } = new();
}
