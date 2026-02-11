# OAuth Authentication Example

This guide demonstrates how to configure OAuth/OIDC authentication for Kafka clients using the library.

## Prerequisites

- Kafka cluster with OAuth/SASL_OAUTHBEARER authentication enabled
- OAuth identity provider (Azure AD, Okta, Auth0, Keycloak, etc.)
- Client credentials (client ID and client secret)

Note: when `OAuthTokenEndpoint`/`OAuthClientId`/`OAuthClientSecret` are configured, the library will automatically add the needed SASL/OAUTHBEARER settings (including `security.protocol`, `sasl.mechanism`, `sasl.oauthbearer.method=OIDC`, `sasl.oauthbearer.token.endpoint.url`, and `sasl.oauthbearer.client.id`) to both producer and consumer configs.

## Basic OAuth Configuration

```csharp
using Microsoft.Extensions.DependencyInjection;
using JohBloch.ConfluentKafka.Clients.Configuration;

var services = new ServiceCollection();

services.AddKafkaClients(options =>
{
    options.BootstrapServers = "your-kafka-broker:9092";
    options.SchemaRegistryUrl = "https://your-schema-registry:8081";
    options.GroupId = "oauth-consumer-group";
    
    // OAuth configuration
    options.OAuthTokenEndpoint = "https://your-oauth-provider/oauth/token";
    options.OAuthClientId = "your-client-id";
    options.OAuthClientSecret = "your-client-secret";
    options.OAuthScope = "kafka"; // Optional, depends on your OAuth provider
});

var serviceProvider = services.BuildServiceProvider();
```

## Azure AD (Microsoft Entra ID) Configuration

```csharp
services.AddKafkaClients(options =>
{
    options.BootstrapServers = "your-kafka.servicebus.windows.net:9093";
    options.SchemaRegistryUrl = "https://your-schema-registry:8081";
    options.GroupId = "azure-ad-consumer-group";
    
    // Azure AD OAuth configuration
    options.OAuthTokenEndpoint = "https://login.microsoftonline.com/{tenant-id}/oauth2/v2.0/token";
    options.OAuthClientId = "your-app-client-id";
    options.OAuthClientSecret = "your-app-client-secret";
    options.OAuthScope = "https://your-kafka-instance/.default";
});
```

## Confluent Cloud Configuration

```csharp
services.AddKafkaClients(options =>
{
    options.BootstrapServers = "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092";
    options.SchemaRegistryUrl = "https://psrc-xxxxx.us-east-1.aws.confluent.cloud";
    options.GroupId = "confluent-cloud-group";
    
    // Confluent Cloud uses API Keys, not OAuth
    // See ApiKeyExample.md for Confluent Cloud configuration
});
```

## Okta Configuration

```csharp
services.AddKafkaClients(options =>
{
    options.BootstrapServers = "your-kafka-broker:9092";
    options.SchemaRegistryUrl = "https://your-schema-registry:8081";
    options.GroupId = "okta-consumer-group";
    
    // Okta OAuth configuration
    options.OAuthTokenEndpoint = "https://your-domain.okta.com/oauth2/default/v1/token";
    options.OAuthClientId = "your-okta-client-id";
    options.OAuthClientSecret = "your-okta-client-secret";
    options.OAuthScope = "kafka-access";
});
```

## Keycloak Configuration

```csharp
services.AddKafkaClients(options =>
{
    options.BootstrapServers = "your-kafka-broker:9092";
    options.SchemaRegistryUrl = "https://your-schema-registry:8081";
    options.GroupId = "keycloak-consumer-group";
    
    // Keycloak OAuth configuration
    options.OAuthTokenEndpoint = "https://your-keycloak-server/auth/realms/your-realm/protocol/openid-connect/token";
    options.OAuthClientId = "kafka-client";
    options.OAuthClientSecret = "your-keycloak-client-secret";
    options.OAuthScope = "openid";
});
```

## SSL/TLS Configuration

When using OAuth with SSL, configure certificate paths:

```csharp
services.AddKafkaClients(options =>
{
    options.BootstrapServers = "your-kafka-broker:9093";
    options.OAuthTokenEndpoint = "https://your-oauth-provider/oauth/token";
    options.OAuthClientId = "your-client-id";
    options.OAuthClientSecret = "your-client-secret";
    
    // Optional: add extra SSL settings (in addition to the auto-added SASL/OAUTHBEARER keys)
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        // SSL certificate configuration
        { "ssl.ca.location", "/path/to/ca-cert.pem" },
        { "ssl.certificate.location", "/path/to/client-cert.pem" },
        { "ssl.key.location", "/path/to/client-key.pem" },
        { "ssl.key.password", "cert-password" } // If key is encrypted
    };
});
```

## Environment-Based Configuration

```csharp
using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
    .AddJsonFile("appsettings.json")
    .AddEnvironmentVariables()
    .Build();

services.AddKafkaClients(options =>
{
    options.BootstrapServers = configuration["Kafka:BootstrapServers"]!;
    options.SchemaRegistryUrl = configuration["Kafka:SchemaRegistryUrl"]!;
    options.GroupId = configuration["Kafka:GroupId"]!;
    
    // OAuth from environment/config
    options.OAuthTokenEndpoint = configuration["Kafka:OAuth:TokenEndpoint"]!;
    options.OAuthClientId = configuration["Kafka:OAuth:ClientId"]!;
    options.OAuthClientSecret = configuration["Kafka:OAuth:ClientSecret"]!;
    options.OAuthScope = configuration["Kafka:OAuth:Scope"];

    // Optional token extensions used by some brokers (e.g. Confluent Cloud)
    options.OAuthLogicalCluster = configuration["Kafka:OAuth:LogicalCluster"];
    options.OAuthIdentityPoolId = configuration["Kafka:OAuth:IdentityPoolId"];
    
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", configuration["Kafka:SecurityProtocol"]! },
        { "sasl.mechanism", configuration["Kafka:SaslMechanism"]! }
    };
});
```

**appsettings.json**:
```json
{
  "Kafka": {
    "BootstrapServers": "your-kafka-broker:9092",
    "SchemaRegistryUrl": "https://your-schema-registry:8081",
    "GroupId": "my-consumer-group",
    "SecurityProtocol": "SASL_SSL",
    "SaslMechanism": "OAUTHBEARER",
    "OAuth": {
      "TokenEndpoint": "https://your-oauth-provider/oauth/token",
      "ClientId": "your-client-id",
      "ClientSecret": "your-client-secret",
            "Scope": "kafka",

            // Optional: Confluent Cloud style OAUTHBEARER extensions
            "LogicalCluster": "lkc-...",
            "IdentityPoolId": "pool-..."
    }
  }
}
```

## Token Refresh

The library uses Confluent.Kafka's OAuth bearer token refresh callbacks for both producers and consumers. Internally it calls `ISecurityTokenProvider.GetAccessTokenAsync()`.

- Tokens are cached in-memory and refreshed before expiration.
- If OAuth is partially configured (some OAuth fields are present) but required settings are missing, the default provider throws an `InvalidOperationException` to fail fast.
- You can replace `ISecurityTokenProvider` with your own implementation if you need non-standard flows, additional custom extensions beyond `logicalCluster`/`identityPoolId`, or different caching.

### Custom token provider (e.g. MSAL)

If you want to acquire tokens via MSAL (or any other mechanism), you can implement `ISecurityTokenProvider` in your application and register it in DI. The library registers its default provider using `TryAddSingleton`, so your custom provider will automatically win.

Important: register your custom provider **before** calling `AddKafkaClients` / `AddKafkaProducerClient` / `AddKafkaConsumerClient`.

```csharp
using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Security;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Your custom provider (MSAL, managed identity, etc.)
services.AddSingleton<ISecurityTokenProvider, MyMsalSecurityTokenProvider>();

services.AddKafkaClients(options =>
{
    options.BootstrapServers = "your-kafka-broker:9093";
    options.SchemaRegistryUrl = "https://your-schema-registry:8081";
    options.GroupId = "my-group";

    // If you're not using the built-in OAuthSecurityTokenProvider, you do not need to
    // configure OAuthTokenEndpoint/OAuthClientId/OAuthClientSecret for token acquisition.
    //
    // However, Kafka still needs SASL/OAUTHBEARER configuration. Provide it via:
    // - options.ConsumerConfig / options.GlobalProducerConfig, OR
    // - ISecurityTokenProvider.GetKafkaSaslConfig() in your implementation.
});
```

Schema Registry token refresh is set up when a security provider is available. With a custom `ISecurityTokenProvider`, Schema Registry refresh does not require `SchemaRegistryOptions.TokenEndpointUrl` to be set.

## Testing OAuth Configuration

```csharp
using JohBloch.ConfluentKafka.Clients.Services;

var producerClient = serviceProvider.GetRequiredService<IKafkaProducerClient>();

try
{
    // Attempt to produce a test message
    var result = await producerClient.ProduceAsync(
        topic: "test-topic",
        key: "test-key",
        value: new { message = "OAuth test" },
        serializationType: SerializationType.Json
    );
    
    Console.WriteLine("✅ OAuth authentication successful!");
    Console.WriteLine($"Message delivered to {result.TopicPartitionOffset}");
}
catch (Exception ex)
{
    Console.WriteLine("❌ OAuth authentication failed:");
    Console.WriteLine(ex.Message);
}
```

## Best Practices

1. **Secrets Management**: Never commit OAuth credentials to source control
   - Use environment variables or secret management services (Azure Key Vault, AWS Secrets Manager)
   
2. **Token Scope**: Request only the minimum required OAuth scopes
   
3. **Network Security**: Always use SSL/TLS (`SASL_SSL`) in production
   
4. **Certificate Validation**: Verify SSL certificates in production environments
   
5. **Error Handling**: Implement proper error handling for authentication failures
   
6. **Logging**: Enable debug logging to troubleshoot OAuth issues:
   ```csharp
   options.GlobalProducerConfig = new Dictionary<string, string>
   {
       { "debug", "security,broker,protocol" }
   };
   ```

7. **Token Expiration**: The library handles token refresh automatically, but monitor logs for refresh failures

## Troubleshooting

### Authentication Failed Error
```
Error: Authentication failed: Invalid client credentials
```
**Solution**: Verify client ID and client secret are correct

### Token Endpoint Unreachable
```
Error: Failed to connect to OAuth token endpoint
```
**Solution**: Check network connectivity and token endpoint URL

### Invalid Scope Error
```
Error: Requested scope is invalid
```
**Solution**: Verify the OAuth scope matches your provider's configuration

### SSL Certificate Error
```
Error: SSL certificate verification failed
```
**Solution**: Ensure CA certificate path is correct and certificate is valid

### Token Refresh Failed
```
Warning: OAuth token refresh failed, will retry
```
**Solution**: Check OAuth provider availability and token expiration settings

## Debug Logging

Enable detailed OAuth logging:

```csharp
options.GlobalProducerConfig = new Dictionary<string, string>
{
    { "debug", "security,broker,protocol" },
    { "log_level", "7" } // 0 = Emergency, 7 = Debug
};
```

## See Also

- [API Key Authentication](ApiKeyExample.md)
- [Avro Example](AvroExample.md)
- [JSON Example](JsonExample.md)
- [Security Best Practices](../SECURITY.md)
