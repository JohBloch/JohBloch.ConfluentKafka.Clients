# OAuth Authentication Example

This guide demonstrates how to configure OAuth/OIDC authentication for Kafka clients using the library.

## Prerequisites

- Kafka cluster with OAuth/SASL_OASLBEARER authentication enabled
- OAuth identity provider (Azure AD, Okta, Auth0, Keycloak, etc.)
- Client credentials (client ID and client secret)

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
    
    // Additional Kafka security settings
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "OAUTHBEARER" },
        { "ssl.ca.location", "/path/to/ca-cert.pem" } // Optional: if using custom CA
    };
    
    options.ConsumerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "OAUTHBEARER" }
    };
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
    
    // Azure Event Hubs for Kafka
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "OAUTHBEARER" }
    };
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
    
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "OAUTHBEARER" }
    };
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
    
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "OAUTHBEARER" }
    };
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
    
    options.GlobalProducerConfig = new Dictionary<string, string>
    {
        { "security.protocol", "SASL_SSL" },
        { "sasl.mechanism", "OAUTHBEARER" },
        
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
      "Scope": "kafka"
    }
  }
}
```

## Token Refresh

The library automatically handles OAuth token refresh:

```csharp
// Token refresh is handled automatically by the OAuthAuthenticationHandler
// Tokens are refreshed before expiration based on the token's expires_in value
// No manual refresh logic required in your application code

// The handler:
// 1. Obtains initial token on first connection
// 2. Monitors token expiration
// 3. Refreshes token before expiration
// 4. Handles refresh failures with exponential backoff
```

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
