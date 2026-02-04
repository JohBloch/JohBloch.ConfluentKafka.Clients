namespace JohBloch.ConfluentKafka.Clients.Models;

/// <summary>
/// System health status information
/// </summary>
public class HealthStatus
{
    /// <summary>
    /// Whether the system is healthy overall
    /// </summary>
    public bool IsHealthy { get; set; }

    /// <summary>
    /// Whether Kafka is connected and accessible
    /// </summary>
    public bool IsKafkaConnected { get; set; }

    /// <summary>
    /// Whether Schema Registry is connected and accessible
    /// </summary>
    public bool IsSchemaRegistryConnected { get; set; }

    /// <summary>
    /// Last error message encountered during health check
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// Additional details about system health
    /// </summary>
    public Dictionary<string, string> Details { get; set; } = new();
}
