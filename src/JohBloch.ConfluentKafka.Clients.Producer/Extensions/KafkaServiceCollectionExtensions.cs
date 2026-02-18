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

namespace JohBloch.ConfluentKafka.Clients.Producer;

/// <summary>
/// Extension methods for setting up Kafka producer client services.
/// </summary>
public static class KafkaServiceCollectionExtensions
{
    /// <summary>
    /// Adds the Kafka producer client to the service collection.
    /// </summary>
    /// <param name="services">The IServiceCollection to add services to.</param>
    /// <param name="configureOptions">An action to configure the KafkaClientOptions.</param>
    /// <returns>The IServiceCollection so that additional calls can be chained.</returns>
    public static IServiceCollection AddKafkaProducerClient(
        this IServiceCollection services,
        Action<KafkaClientOptions> configureOptions)
    {
        AddKafkaCoreServices(services, configureOptions);

        services.TryAddSingleton<IKafkaProducerClient>(sp =>
        {
            KafkaClientOptions options = sp.GetRequiredService<IOptions<KafkaClientOptions>>().Value;
            ISecurityTokenProvider security = sp.GetRequiredService<ISecurityTokenProvider>();
            ISchemaRegistryExtClient schemaRegistry = sp.GetRequiredService<ISchemaRegistryExtClient>();
            ILoggerFactory loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            ILogger<KafkaProducerClient> logger = sp.GetRequiredService<ILogger<KafkaProducerClient>>();

            foreach (KeyValuePair<string, KafkaProducerOptions> kvp in options.Producers)
            {
                if (string.IsNullOrEmpty(kvp.Value.BootstrapServers))
                {
                    kvp.Value.BootstrapServers = options.BootstrapServers;
                }
            }

            return new KafkaProducerClient(
                options.Producers,
                security,
                schemaRegistry,
                loggerFactory,
                logger,
                options.GlobalProducerConfig,
                options.PerProducerConfigs.ToDictionary(k => k.Key, v => (IDictionary<string, string>)v.Value));
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
            KafkaClientOptions cfg = clientOpts.Value;

            if (string.IsNullOrWhiteSpace(srOpts.Url))
            {
                srOpts.Url = cfg.SchemaRegistryUrl;
            }

            if (string.IsNullOrWhiteSpace(srOpts.TokenEndpointUrl))
            {
                srOpts.TokenEndpointUrl = cfg.SchemaRegistryOauthTokenEndpoint
                                         ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(srOpts.ClientId))
            {
                srOpts.ClientId = cfg.SchemaRegistryOauthClientId
                               ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(srOpts.ClientSecret))
            {
                srOpts.ClientSecret = cfg.SchemaRegistryOauthClientSecret
                                   ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(srOpts.Scope))
            {
                srOpts.Scope = cfg.SchemaRegistryOauthScope
                            ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(srOpts.LogicalCluster))
            {
                srOpts.LogicalCluster = cfg.SchemaRegistryOauthLogicalCluster
                                     ?? string.Empty;
            }

            if (string.IsNullOrWhiteSpace(srOpts.IdentityPoolId))
            {
                srOpts.IdentityPoolId = cfg.SchemaRegistryOauthIdentityPoolId
                                      ?? string.Empty;
            }
        });

        services.AddHttpClient("KafkaOAuth");
        services.AddHttpClient("SchemaRegistryOAuth");
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

            // Schema Registry token flow:
            // - Prefer a custom security provider if registered (e.g. MSAL).
            // - Otherwise, if Schema Registry OAuth is configured, request token using SchemaRegistryOptions.
            if (security != null && hasCustomSecurityProvider)
            {
                tokenRefreshFunc = async () =>
                {
                    AccessToken token = await security.GetAccessTokenAsync().ConfigureAwait(false);
                    return (token.AccessTokenValue, token.ExpiresOn.UtcDateTime);
                };
            }
            else if (!string.IsNullOrWhiteSpace(srOpts.TokenEndpointUrl))
            {
                IHttpClientFactory httpClientFactory = sp.GetRequiredService<IHttpClientFactory>();
                ILogger logger = sp.GetRequiredService<ILoggerFactory>().CreateLogger("SchemaRegistryOAuth");
                HttpClient httpClient = httpClientFactory.CreateClient("SchemaRegistryOAuth");

                tokenRefreshFunc = async () =>
                {
                    AccessToken token = await OAuthClientCredentialsTokenClient.RequestTokenAsync(
                            httpClient,
                            srOpts.TokenEndpointUrl,
                            srOpts.ClientId,
                            srOpts.ClientSecret,
                            srOpts.Scope,
                            logger,
                            CancellationToken.None)
                        .ConfigureAwait(false);

                    return (token.AccessTokenValue, token.ExpiresOn.UtcDateTime);
                };
            }

            SchemaClientOptions options = new SchemaClientOptions();
            if (!string.IsNullOrWhiteSpace(srOpts.LogicalCluster))
            {
                options.LogicalCluster = srOpts.LogicalCluster;
            }

            if (!string.IsNullOrWhiteSpace(srOpts.IdentityPoolId))
            {
                options.IdentityPoolId = srOpts.IdentityPoolId;
            }

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
