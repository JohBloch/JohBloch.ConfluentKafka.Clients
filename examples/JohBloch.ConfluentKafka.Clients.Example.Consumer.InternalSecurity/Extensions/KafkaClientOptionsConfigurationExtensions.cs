using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Models;
using Microsoft.Extensions.Configuration;

internal static class KafkaClientOptionsConfigurationExtensions
{
    public static KafkaClientOptions ApplySchemaRegistrySection(this KafkaClientOptions options, IConfiguration configuration)
    {
        IConfigurationSection schemaRegistry = configuration.GetSection("SchemaRegistry");

        options.SchemaRegistryUrl = schemaRegistry["Url"] ?? string.Empty;

        string? authority = GetNonEmpty(schemaRegistry, "Authority");
        string? tokenEndpointUrl = GetNonEmpty(schemaRegistry, "TokenEndpointUrl");
        if (tokenEndpointUrl == null && authority != null)
        {
            tokenEndpointUrl = authority.TrimEnd('/') + "/oauth2/v2.0/token";
        }

        options.OAuthTokenEndpoint = tokenEndpointUrl;
        options.OAuthClientId = GetNonEmpty(schemaRegistry, "ClientId");
        options.OAuthClientSecret = GetNonEmpty(schemaRegistry, "ClientSecret");
        options.OAuthScope = GetNonEmpty(schemaRegistry, "Scope");
        options.OAuthLogicalCluster = GetNonEmpty(schemaRegistry, "LogicalCluster");
        options.OAuthIdentityPoolId = GetNonEmpty(schemaRegistry, "IdentityPoolId");

        return options;
    }

    public static KafkaClientOptions ApplyKafkaSection(this KafkaClientOptions options, IConfiguration configuration)
    {
        IConfigurationSection kafka = configuration.GetSection("Kafka");
        options.BootstrapServers = kafka["BootstrapServers"] ?? string.Empty;
        return options;
    }

    public static KafkaClientOptions ApplyConsumerSection(this KafkaClientOptions options, IConfiguration configuration)
    {
        IConfigurationSection consumer = configuration.GetSection("Consumer");

        options.GroupId = consumer["GroupId"] ?? string.Empty;

        options.Consumer.GroupId = options.GroupId;

        // Topic subscription is handled by the example app (so it can subscribe to multiple topics).
        // We still set Topic when a single topic is provided for backwards compatibility.
        options.Consumer.Topic = consumer["Topic"] ?? string.Empty;
        options.Consumer.AutoOffsetReset = consumer["AutoOffsetReset"] ?? "earliest";
        options.Consumer.EnableAutoCommit = TryGetBool(consumer, "EnableAutoCommit", defaultValue: true);
        options.Consumer.DefaultSchemaType = Enum.TryParse(consumer["DefaultSchemaType"], ignoreCase: true, out SchemaType parsed)
            ? parsed
            : SchemaType.Json;
        options.Consumer.AutoDetectSchemaType = TryGetBool(consumer, "AutoDetectSchemaType", defaultValue: true);

        return options;
    }

    public static IReadOnlyList<string> GetConsumerTopics(IConfiguration configuration)
    {
        IConfigurationSection consumer = configuration.GetSection("Consumer");

        string? topicsRaw = consumer["Topics"];
        if (!string.IsNullOrWhiteSpace(topicsRaw))
        {
            string[] split = topicsRaw
                .Split([',', ';'], StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            return split.Length == 0 ? [] : split;
        }

        string? single = consumer["Topic"];
        return string.IsNullOrWhiteSpace(single) ? [] : [single.Trim()];
    }

    private static bool TryGetBool(IConfigurationSection section, string key, bool defaultValue)
    {
        string? raw = section[key];
        return bool.TryParse(raw, out bool parsed) ? parsed : defaultValue;
    }

    private static string? GetNonEmpty(IConfigurationSection section, string key)
    {
        string? value = section[key];
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
