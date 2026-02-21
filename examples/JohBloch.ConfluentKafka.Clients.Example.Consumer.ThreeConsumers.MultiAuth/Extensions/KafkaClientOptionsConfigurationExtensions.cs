using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Models;
using Microsoft.Extensions.Configuration;

namespace JohBloch.ConfluentKafka.Clients.Example.Consumer.ThreeConsumers.MultiAuth.Extensions;

internal static class KafkaClientOptionsConfigurationExtensions
{
    public static KafkaClientOptions ApplySchemaRegistrySection(this KafkaClientOptions options, IConfiguration configuration)
    {
        IConfigurationSection schemaRegistry = configuration.GetSection("SchemaRegistry");

        options.SchemaRegistryUrl = schemaRegistry["Url"] ?? string.Empty;

        string? authority = GetNonEmpty(schemaRegistry, "Authority");
        string? tokenEndpointUrl = GetNonEmpty(schemaRegistry, "TokenEndpointUrl");
        if (tokenEndpointUrl is null && authority is not null)
        {
            tokenEndpointUrl = authority.TrimEnd('/') + "/oauth2/v2.0/token";
        }

        options.SchemaRegistryOauthTokenEndpoint = tokenEndpointUrl;
        options.SchemaRegistryOauthClientId = GetNonEmpty(schemaRegistry, "ClientId");
        options.SchemaRegistryOauthClientSecret = GetNonEmpty(schemaRegistry, "ClientSecret");
        options.SchemaRegistryOauthScope = GetNonEmpty(schemaRegistry, "Scope");
        options.SchemaRegistryOauthLogicalCluster = GetNonEmpty(schemaRegistry, "LogicalCluster");
        options.SchemaRegistryOauthIdentityPoolId = GetNonEmpty(schemaRegistry, "IdentityPoolId");

        return options;
    }

    public static KafkaClientOptions ApplyKafkaSection(this KafkaClientOptions options, IConfiguration configuration)
    {
        IConfigurationSection kafka = configuration.GetSection("Kafka");

        options.BootstrapServers = kafka["BootstrapServers"] ?? string.Empty;

        IConfigurationSection kafkaOAuth = kafka.GetSection("OAuth");

        string? tokenEndpointUrl =
            GetNonEmpty(kafkaOAuth, "TokenEndpointUrl")
            ?? GetNonEmpty(kafkaOAuth, "TokenEndpoint")
            ?? GetNonEmpty(kafkaOAuth, "TokenEndpointUri");

        string? authority = GetNonEmpty(kafkaOAuth, "Authority");
        if (tokenEndpointUrl is null && authority is not null)
        {
            tokenEndpointUrl = authority.TrimEnd('/') + "/oauth2/v2.0/token";
        }

        string? clientId = GetNonEmpty(kafkaOAuth, "ClientId");
        string? clientSecret = GetNonEmpty(kafkaOAuth, "ClientSecret");
        string? scope = GetNonEmpty(kafkaOAuth, "Scope");
        string? logicalCluster = GetNonEmpty(kafkaOAuth, "LogicalCluster");
        string? identityPoolId = GetNonEmpty(kafkaOAuth, "IdentityPoolId");

        options.OAuth.TokenEndpointUrl = tokenEndpointUrl;
        options.OAuth.ClientId = clientId;
        options.OAuth.ClientSecret = clientSecret;
        options.OAuth.Scope = scope;
        options.OAuth.LogicalCluster = logicalCluster;
        options.OAuth.IdentityPoolId = identityPoolId;

        return options;
    }

    public static KafkaClientOptions ApplyConsumerSection(this KafkaClientOptions options, IConfiguration configuration)
    {
        IConfigurationSection consumer = configuration.GetSection("Consumer");

        options.GroupId = consumer["GroupId"] ?? string.Empty;
        options.Consumer.GroupId = options.GroupId;

        if (Enum.TryParse(consumer["SecurityMode"], ignoreCase: true, out KafkaConsumerSecurityMode mode))
        {
            options.Consumer.SecurityMode = mode;
        }

        options.Consumer.ApiKey = consumer["ApiKey"];
        options.Consumer.ApiSecret = consumer["ApiSecret"];

        // Topic subscription is handled by the example app (so it can subscribe to multiple topics).
        // We still set Topic when a single topic is provided for backwards compatibility.
        options.Consumer.Topic = consumer["Topic"] ?? string.Empty;
        options.Consumer.Topics = GetConsumerTopics(configuration).ToList();

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

    private static bool TryGetBool(IConfigurationSection section, string configKey, bool defaultValue)
    {
        string? raw = section[configKey];
        return bool.TryParse(raw, out bool parsed) ? parsed : defaultValue;
    }

    private static string? GetNonEmpty(IConfigurationSection section, string configKey)
    {
        string? value = section[configKey];
        return string.IsNullOrWhiteSpace(value) ? null : value;
    }
}
