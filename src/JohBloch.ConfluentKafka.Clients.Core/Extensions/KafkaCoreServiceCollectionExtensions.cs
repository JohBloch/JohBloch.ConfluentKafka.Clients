using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Security;
using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces;
using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace JohBloch.ConfluentKafka.Clients.Core;

/// <summary>
/// Extension methods for setting up the shared Kafka infrastructure.
/// </summary>
public static class KafkaCoreServiceCollectionExtensions
{
    /// <summary>
    /// Adds shared Kafka infrastructure services to the service collection.
    /// This includes options mapping, OAuth token provider, HTTP client, and schema registry client.
    /// </summary>
    /// <param name="services">The IServiceCollection to add services to.</param>
    /// <param name="configureOptions">An action to configure the KafkaClientOptions.</param>
    /// <returns>The IServiceCollection so that additional calls can be chained.</returns>
    public static IServiceCollection AddKafkaCore(
        this IServiceCollection services,
        Action<KafkaClientOptions> configureOptions)
    {
        services.Configure(configureOptions);

        services.AddOptions<KafkaConsumerOptions>().Configure<IOptions<KafkaClientOptions>>((consumerOpts, clientOpts) =>
        {
            KafkaConsumerOptions source = clientOpts.Value.Consumer;
            KafkaClientOptions common = clientOpts.Value;

            consumerOpts.BootstrapServers = string.IsNullOrEmpty(source.BootstrapServers)
                ? common.BootstrapServers
                : source.BootstrapServers;

            consumerOpts.GroupId = string.IsNullOrEmpty(source.GroupId)
                ? common.GroupId
                : source.GroupId;

            consumerOpts.Topic = source.Topic;
            consumerOpts.EnableAutoCommit = source.EnableAutoCommit;
            consumerOpts.AutoOffsetReset = source.AutoOffsetReset;
            consumerOpts.SessionTimeoutMs = source.SessionTimeoutMs;
            consumerOpts.HeartbeatIntervalMs = source.HeartbeatIntervalMs;
            consumerOpts.DefaultSchemaType = source.DefaultSchemaType;
            consumerOpts.AutoDetectSchemaType = source.AutoDetectSchemaType;
            consumerOpts.TopicSchemaTypes = source.TopicSchemaTypes;
        });

        services.AddOptions<SchemaRegistryOptions>().Configure<IOptions<KafkaClientOptions>>((srOpts, clientOpts) =>
        {
            if (string.IsNullOrWhiteSpace(srOpts.Url))
            {
                srOpts.Url = clientOpts.Value.SchemaRegistryUrl;
            }

            if (string.IsNullOrWhiteSpace(srOpts.TokenEndpointUrl))
            {
                srOpts.TokenEndpointUrl = clientOpts.Value.OAuthTokenEndpoint ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(srOpts.ClientId))
            {
                srOpts.ClientId = clientOpts.Value.OAuthClientId ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(srOpts.ClientSecret))
            {
                srOpts.ClientSecret = clientOpts.Value.OAuthClientSecret ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(srOpts.Scope))
            {
                srOpts.Scope = clientOpts.Value.OAuthScope ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(srOpts.LogicalCluster))
            {
                srOpts.LogicalCluster = clientOpts.Value.OAuthLogicalCluster ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(srOpts.IdentityPoolId))
            {
                srOpts.IdentityPoolId = clientOpts.Value.OAuthIdentityPoolId ?? string.Empty;
            }
        });

        services.AddHttpClient("KafkaOAuth");
        services.TryAddSingleton<ISecurityTokenProvider, OAuthSecurityTokenProvider>();

        // Default schema cache (in-memory). Consumers can override by registering their own ISchemaCache
        // (e.g., Redis) before calling AddKafkaCore/AddKafkaClients.
        services.TryAddSingleton<ISchemaCache>(_ => new InMemorySchemaCache());

        services.TryAddSingleton<ISchemaRegistryExtClient>(sp =>
        {
            SchemaRegistryOptions srOpts = sp.GetRequiredService<IOptions<SchemaRegistryOptions>>().Value;
            KafkaClientOptions kafkaOpts = sp.GetRequiredService<IOptions<KafkaClientOptions>>().Value;
            ISecurityTokenProvider? security = sp.GetService<ISecurityTokenProvider>();
            ISchemaCache cache = sp.GetRequiredService<ISchemaCache>();

            SchemaRegistryConfig config = new SchemaRegistryConfig
            {
                Url = string.IsNullOrWhiteSpace(srOpts.Url) ? kafkaOpts.SchemaRegistryUrl : srOpts.Url
            };

            Func<Task<(string token, DateTime expiresAt)>>? tokenRefreshFunc = null;
            if (!string.IsNullOrWhiteSpace(srOpts.TokenEndpointUrl) && security != null)
            {
                tokenRefreshFunc = async () =>
                {
                    AccessToken token = await security.GetAccessTokenAsync().ConfigureAwait(false);
                    return (token.AccessTokenValue, token.ExpiresOn.UtcDateTime);
                };
            }

            SchemaClientOptions options = new SchemaClientOptions
            {
                LogicalCluster = srOpts.LogicalCluster,
                IdentityPoolId = srOpts.IdentityPoolId
            };

            if (tokenRefreshFunc == null)
            {
                return new JohBloch.ConfluentKafka.SchemaRegistryExtClient.Services.SchemaRegistryExtClient(
                    config,
                    tokenManager: null,
                    cache: cache,
                    options: options);
            }

            return new JohBloch.ConfluentKafka.SchemaRegistryExtClient.Services.SchemaRegistryExtClient(
                config,
                tokenRefreshFunc,
                cache: cache,
                options: options);
        });

        return services;
    }
}
