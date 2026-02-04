using JohBloch.ConfluentKafka.Clients.Core;
using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Services;
using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace JohBloch.ConfluentKafka.Clients.Consumer;

/// <summary>
/// Extension methods for setting up Kafka consumer client services.
/// </summary>
public static class KafkaConsumerServiceCollectionExtensions
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
        services.AddKafkaCore(configureOptions);

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
}
