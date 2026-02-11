using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces;
using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Models;
using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Services;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace JohBloch.ConfluentKafka.Clients;

public static class SchemaRegistryCacheServiceCollectionExtensions
{
    public static IServiceCollection AddSchemaRegistryCache(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        string? provider = configuration["SchemaRegistry:Cache:Provider"];

        if (string.Equals(provider, "InMemory", StringComparison.OrdinalIgnoreCase))
        {
            services.AddSingleton<ISchemaCache>(sp =>
            {
                SchemaCacheOptions options = new SchemaCacheOptions();
                return new InMemorySchemaCache(options: options);
            });

            return services;
        }

        if (!string.Equals(provider, "Redis", StringComparison.OrdinalIgnoreCase))
        {
            return services;
        }

        string? connectionString = configuration["SchemaRegistry:Cache:Redis:ConnectionString"];
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new InvalidOperationException(
                "SchemaRegistry cache provider is set to 'Redis' but 'SchemaRegistry:Cache:Redis:ConnectionString' is missing.");
        }

        string keyPrefix = configuration["SchemaRegistry:Cache:Redis:KeyPrefix"] ?? "schema-registry-cache:";

        string? ttlSecondsRaw = configuration["SchemaRegistry:Cache:Redis:DefaultTtlSeconds"];
        TimeSpan? defaultTtl = null;
        if (!string.IsNullOrWhiteSpace(ttlSecondsRaw) && int.TryParse(ttlSecondsRaw, out int ttlSeconds) && ttlSeconds > 0)
        {
            defaultTtl = TimeSpan.FromSeconds(ttlSeconds);
        }

        services.AddSingleton<IConnectionMultiplexer>(_ =>
        {
            ConfigurationOptions options = ConfigurationOptions.Parse(connectionString);
            options.AbortOnConnectFail = false;
            return ConnectionMultiplexer.Connect(options);
        });

        services.AddSingleton<ISchemaCache>(sp =>
        {
            IConnectionMultiplexer multiplexer = sp.GetRequiredService<IConnectionMultiplexer>();
            return new RedisSchemaCache(multiplexer, keyPrefix, defaultTtl);
        });

        return services;
    }
}
