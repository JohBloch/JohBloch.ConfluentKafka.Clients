namespace JohBloch.ConfluentKafka.Clients.Services;

/// <summary>
/// Shared validation for Kafka configuration.
/// Enforces only keys without librdkafka defaults.
/// </summary>
public static class KafkaConfigValidator
{
    /// <summary>
    /// Validate global config (requires bootstrap.servers).
    /// Returns null if valid; otherwise an error message.
    /// </summary>
    public static string? ValidateGlobal(string? bootstrapServers)
    {
        if (string.IsNullOrWhiteSpace(bootstrapServers))
            return "Missing Kafka.Global.Config required key: bootstrap.servers";
        return null;
    }

    /// <summary>
    /// Validate consumers (requires group.id for each named consumer).
    /// Returns null if valid; otherwise an error message.
    /// </summary>
    public static string? ValidateConsumer(string consumerName, string? groupId)
    {
        if (string.IsNullOrWhiteSpace(groupId))
            return $"Missing required consumer setting group.id for {consumerName}";
        return null;
    }
}
