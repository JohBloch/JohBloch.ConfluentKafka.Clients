using Confluent.SchemaRegistry;
using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Security;
using JohBloch.ConfluentKafka.Clients.Services;
using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces;
using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Models;
using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace JohBloch.ConfluentKafka.Clients.Consumer;

/// <summary>
/// Extension methods for setting up Kafka consumer client services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Kafka consumer client to the service collection.
    /// </summary>
    /// <param name="services">The IServiceCollection to add services to.</param>
    /// <param name="configureOptions">An action to configure the KafkaClientOptions.</param>
    /// <returns>The IServiceCollection so that additional calls can be chained.</returns>
    public static IServiceCollection AddKafkaConsumerClient(
        this IServiceCollection services,
        Action<KafkaClientOptions> configureOptions)
    {
        AddKafkaCoreServices(services, configureOptions);

        services.TryAddSingleton<IKafkaConsumerClient>(sp =>
        {
            var consumerOpts = sp.GetRequiredService<IOptions<KafkaConsumerOptions>>();
            var srOpts = sp.GetRequiredService<IOptions<SchemaRegistryOptions>>();
            var security = sp.GetRequiredService<ISecurityTokenProvider>();
            var schemaRegistry = sp.GetRequiredService<ISchemaRegistryExtClient>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var logger = sp.GetRequiredService<ILogger<KafkaConsumerClient>>();
            var clientOptions = sp.GetRequiredService<IOptions<KafkaClientOptions>>().Value;

            return new KafkaConsumerClient(
                consumerOpts,
                srOpts,
                security,
                schemaRegistry,
                loggerFactory,
                logger,
                globalConfig: clientOptions.ConsumerConfig,
                consumerOverrides: null,
                consumerOverride: null);
        });

        return services;
    }

    private static IServiceCollection AddKafkaCoreServices(
        IServiceCollection services,
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
            bool hasCustomSecurityProvider = security is not null
                                            && security is not OAuthSecurityTokenProvider;

            if (security != null && (hasCustomSecurityProvider || !string.IsNullOrWhiteSpace(srOpts.TokenEndpointUrl)))
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

            if (tokenRefreshFunc is null)
            {
                return new global::JohBloch.ConfluentKafka.SchemaRegistryExtClient.Services.SchemaRegistryExtClient(
                    config,
                    tokenManager: null,
                    cache: cache,
                    options: options);
            }

            Func<Task<(string token, DateTime expiresAt)>> nonNullTokenRefreshFunc = tokenRefreshFunc
                ?? throw new InvalidOperationException("Token refresh function was expected to be non-null.");

            return new global::JohBloch.ConfluentKafka.SchemaRegistryExtClient.Services.SchemaRegistryExtClient(
                config,
                nonNullTokenRefreshFunc,
                cache: cache,
                options: options);
        });

        return services;
    }
}
