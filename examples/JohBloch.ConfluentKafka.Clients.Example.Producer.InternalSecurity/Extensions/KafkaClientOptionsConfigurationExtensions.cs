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

        // Read nested Kafka:OAuth:* (matches env vars Kafka__OAuth__*)
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

        // Populate the new nested options (library supports this)
        options.OAuth.TokenEndpointUrl = tokenEndpointUrl;
        options.OAuth.ClientId = clientId;
        options.OAuth.ClientSecret = clientSecret;
        options.OAuth.Scope = scope;
        options.OAuth.LogicalCluster = logicalCluster;
        options.OAuth.IdentityPoolId = identityPoolId;

        return options;
    }

    public static KafkaClientOptions ApplyProducerSection(this KafkaClientOptions options, IConfiguration configuration)
    {
        IConfigurationSection producer = configuration.GetSection("Producer");

        // Producer definitions: Producer:Producers:<key>:{Topic,AutoDlqOnDeliveryFailure,...}
        IConfigurationSection producersSection = producer.GetSection("Producers");
        foreach (IConfigurationSection producerItem in producersSection.GetChildren())
        {
            string key = producerItem.Key;
            if (string.IsNullOrWhiteSpace(key))
            {
                continue;
            }

            string topic = producerItem["Topic"] ?? string.Empty;
            options.Producers[key] = new KafkaProducerOptions
            {
                Topic = topic,
                AutoDlqOnDeliveryFailure = TryGetBool(producerItem, "AutoDlqOnDeliveryFailure", defaultValue: false),
                IncludeStackTraceInDlq = TryGetBool(producerItem, "IncludeStackTraceInDlq", defaultValue: false),
                DeadLetterQueueTopicPattern = producerItem["DeadLetterQueueTopicPattern"] ?? "dlq-{topic}"
            };
        }

        // Backwards compatible single-producer keys (Producer:Key + Producer:Topic)
        string? singleKey = producer["Key"];
        string? singleTopic = producer["Topic"];
        if (!string.IsNullOrWhiteSpace(singleKey) && !string.IsNullOrWhiteSpace(singleTopic))
        {
            options.Producers[singleKey] = new KafkaProducerOptions
            {
                Topic = singleTopic,
                AutoDlqOnDeliveryFailure = TryGetBool(producer, "AutoDlqOnDeliveryFailure", defaultValue: false),
                IncludeStackTraceInDlq = TryGetBool(producer, "IncludeStackTraceInDlq", defaultValue: false),
                DeadLetterQueueTopicPattern = producer["DeadLetterQueueTopicPattern"] ?? "dlq-{topic}"
            };
        }

        options.GlobalProducerConfig = producer.GetSection("Config")
            .GetChildren()
            .Where(c => c.Value != null)
            .ToDictionary(c => c.Key, c => c.Value!, StringComparer.OrdinalIgnoreCase);

        return options;
    }

    public static IReadOnlyList<string> GetProducerKeys(IConfiguration configuration)
    {
        IConfigurationSection producers = configuration.GetSection("Producer").GetSection("Producers");
        List<string> keys = producers.GetChildren()
            .Select(c => c.Key)
            .Where(k => !string.IsNullOrWhiteSpace(k))
            .ToList();

        if (keys.Count > 0)
        {
            return keys;
        }

        string key = configuration.GetSection("Producer")["Key"] ?? "default";
        return [key];
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
