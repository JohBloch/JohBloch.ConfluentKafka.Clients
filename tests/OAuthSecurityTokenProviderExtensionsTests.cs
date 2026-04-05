using System;
using System.Net.Http;
using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Security;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Xunit;

namespace JohBloch.ConfluentKafka.Clients.Tests;

public class OAuthSecurityTokenProviderExtensionsTests
{
    private static string CreateTestSecret() => Guid.NewGuid().ToString("N");

    [Fact]
    public void GetKafkaSaslConfig_WhenConfiguredViaNestedOAuthSection_ReturnsSaslSettings()
    {
        var clientSecret = CreateTestSecret();
        var options = new KafkaClientOptions
        {
            OAuth = new KafkaOAuthOptions
            {
                TokenEndpointUrl = "https://example.com/oauth/token",
                ClientId = "client-id",
                ClientSecret = clientSecret,
                Scope = "scope-a scope-b",
                LogicalCluster = "lkc-123",
                IdentityPoolId = "pool-456"
            }
        };

        var provider = new OAuthSecurityTokenProvider(
            Options.Create(options),
            NullLogger<OAuthSecurityTokenProvider>.Instance,
            new StubHttpClientFactory());

        var sasl = provider.GetKafkaSaslConfig();

        Assert.NotNull(sasl);
        Assert.Equal("OAUTHBEARER", sasl!["sasl.mechanism"], StringComparer.OrdinalIgnoreCase);
        // Tokens are injected via the refresh callback; do not enable librdkafka's built-in OIDC fetcher by default.
        Assert.False(sasl.ContainsKey("sasl.oauthbearer.method"));
        Assert.False(sasl.ContainsKey("sasl.oauthbearer.token.endpoint.url"));
        Assert.False(sasl.ContainsKey("sasl.oauthbearer.client.id"));
        Assert.False(sasl.ContainsKey("sasl.oauthbearer.client.secret"));
        Assert.Equal("scope-a scope-b", sasl["sasl.oauthbearer.scope"]);
    }

    [Fact]
    public void GetExtensions_WhenConfigured_ReturnsLogicalClusterAndIdentityPoolId()
    {
        var clientSecret = CreateTestSecret();
        var options = new KafkaClientOptions
        {
            OAuth = new KafkaOAuthOptions
            {
                TokenEndpointUrl = "https://example.com/oauth/token",
                ClientId = "client-id",
                ClientSecret = clientSecret,
                LogicalCluster = "lkc-123",
                IdentityPoolId = "pool-456"
            }
        };

        var provider = new OAuthSecurityTokenProvider(
            Options.Create(options),
            NullLogger<OAuthSecurityTokenProvider>.Instance,
            new StubHttpClientFactory());

        System.Collections.Generic.Dictionary<string, string>? extensions = provider.GetExtensions();

        Assert.NotNull(extensions);
        Assert.Equal("lkc-123", extensions!["logicalCluster"]);
        Assert.Equal("pool-456", extensions["identityPoolId"]);
    }

    [Fact]
    public void GetExtensions_WhenNotConfigured_ReturnsNull()
    {
        var clientSecret = CreateTestSecret();
        var options = new KafkaClientOptions
        {
            OAuth = new KafkaOAuthOptions
            {
                TokenEndpointUrl = "https://example.com/oauth/token",
                ClientId = "client-id",
                ClientSecret = clientSecret
            }
        };

        var provider = new OAuthSecurityTokenProvider(
            Options.Create(options),
            NullLogger<OAuthSecurityTokenProvider>.Instance,
            new StubHttpClientFactory());

        Assert.Null(provider.GetExtensions());
    }

    private sealed class StubHttpClientFactory : IHttpClientFactory
    {
        public HttpClient CreateClient(string name)
        {
            return new HttpClient(new HttpClientHandler());
        }
    }
}
