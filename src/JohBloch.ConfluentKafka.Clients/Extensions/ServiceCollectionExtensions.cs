using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Security;
using JohBloch.ConfluentKafka.Clients.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;

namespace JohBloch.ConfluentKafka.Clients;

/// <summary>
/// Extension methods for setting up Kafka clients in an IServiceCollection.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds Kafka Producer and Consumer clients to the service collection.
    /// </summary>
    /// <param name="services">The IServiceCollection to add services to.</param>
    /// <param name="configureOptions">An action to configure the KafkaClientOptions.</param>
    /// <returns>The IServiceCollection so that additional calls can be chained.</returns>
    public static IServiceCollection AddKafkaClients(
        this IServiceCollection services, 
        Action<KafkaClientOptions> configureOptions)
    {
        // 1. Configure the simplified client options
        services.Configure(configureOptions);

        // 2. Map simplified options to internal KafkaConsumerOptions
        services.AddOptions<KafkaConsumerOptions>().Configure<IOptions<KafkaClientOptions>>((consumerOpts, clientOpts) => 
        {
            var source = clientOpts.Value.Consumer;
            var common = clientOpts.Value;
            
            // Fallback to common config if not specified in consumer config
            consumerOpts.BootstrapServers = string.IsNullOrEmpty(source.BootstrapServers) 
                ? common.BootstrapServers 
                : source.BootstrapServers;
                
            consumerOpts.GroupId = string.IsNullOrEmpty(source.GroupId) 
                ? common.GroupId 
                : source.GroupId;
            
            // Copy remaining properties
            consumerOpts.Topic = source.Topic;
            consumerOpts.EnableAutoCommit = source.EnableAutoCommit;
            consumerOpts.AutoOffsetReset = source.AutoOffsetReset;
            consumerOpts.SessionTimeoutMs = source.SessionTimeoutMs;
            consumerOpts.HeartbeatIntervalMs = source.HeartbeatIntervalMs;
            consumerOpts.DefaultSchemaType = source.DefaultSchemaType;
            consumerOpts.AutoDetectSchemaType = source.AutoDetectSchemaType;
            consumerOpts.TopicSchemaTypes = source.TopicSchemaTypes;
        });

        // 3. Map simplified options to SchemaRegistryOptions
        services.AddOptions<SchemaRegistryOptions>().Configure<IOptions<KafkaClientOptions>>((srOpts, clientOpts) =>
        {
            srOpts.Url = clientOpts.Value.SchemaRegistryUrl;
        });

        // 4. Register Infrastructure Services
        services.AddHttpClient("KafkaOAuth");
        services.TryAddSingleton<ISecurityTokenProvider, OAuthSecurityTokenProvider>();
        services.TryAddSingleton<ISchemaRegistryFactory, SchemaRegistryFactory>();

        // 5. Register Producer Client (Factory Pattern)
        services.TryAddSingleton<IKafkaProducerClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaClientOptions>>().Value;
            var security = sp.GetRequiredService<ISecurityTokenProvider>();
            var schemaRegistry = sp.GetRequiredService<ISchemaRegistryFactory>();
            var logger = sp.GetRequiredService<ILogger<KafkaProducerClient>>();

            // Ensure BootstrapServers is inherited if missing
            foreach (var kvp in options.Producers)
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
                logger,
                options.GlobalProducerConfig,
                options.PerProducerConfigs.ToDictionary(k => k.Key, v => (IDictionary<string, string>)v.Value)
            );
        });

        // 6. Register Consumer Client
        services.TryAddSingleton<IKafkaConsumerClient>(sp =>
        {
            var consumerOpts = sp.GetRequiredService<IOptions<KafkaConsumerOptions>>();
            var srOpts = sp.GetRequiredService<IOptions<SchemaRegistryOptions>>();
            var security = sp.GetRequiredService<ISecurityTokenProvider>();
            var schemaRegistry = sp.GetRequiredService<ISchemaRegistryFactory>();
            var logger = sp.GetRequiredService<ILogger<KafkaConsumerClient>>();
            var clientOptions = sp.GetRequiredService<IOptions<KafkaClientOptions>>().Value;

            return new KafkaConsumerClient(
                consumerOpts,
                srOpts,
                security,
                schemaRegistry,
                logger,
                globalConfig: clientOptions.ConsumerConfig,
                consumerOverrides: null);
        });

        return services;
    }
}
