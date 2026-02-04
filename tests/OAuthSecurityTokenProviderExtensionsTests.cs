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
    public void GetExtensions_WhenConfigured_ReturnsLogicalClusterAndIdentityPoolId()
    {
        var clientSecret = CreateTestSecret();
        var options = new KafkaClientOptions
        {
            OAuthTokenEndpoint = "https://example.com/oauth/token",
            OAuthClientId = "client-id",
            OAuthClientSecret = clientSecret,
            OAuthLogicalCluster = "lkc-123",
            OAuthIdentityPoolId = "pool-456"
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
            OAuthTokenEndpoint = "https://example.com/oauth/token",
            OAuthClientId = "client-id",
            OAuthClientSecret = clientSecret
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
