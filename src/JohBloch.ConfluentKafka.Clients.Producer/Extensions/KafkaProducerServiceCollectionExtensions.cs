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

namespace JohBloch.ConfluentKafka.Clients.Producer;

/// <summary>
/// Extension methods for setting up Kafka producer client services.
/// </summary>
public static class KafkaProducerServiceCollectionExtensions
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
        services.AddKafkaCore(configureOptions);

        services.TryAddSingleton<IKafkaProducerClient>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<KafkaClientOptions>>().Value;
            var security = sp.GetRequiredService<ISecurityTokenProvider>();
            var schemaRegistry = sp.GetRequiredService<ISchemaRegistryExtClient>();
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var logger = sp.GetRequiredService<ILogger<KafkaProducerClient>>();

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
                loggerFactory,
                logger,
                options.GlobalProducerConfig,
                options.PerProducerConfigs.ToDictionary(k => k.Key, v => (IDictionary<string, string>)v.Value));
        });

        return services;
    }
}
