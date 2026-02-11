using JohBloch.ConfluentKafka.Clients.Configuration;
using JohBloch.ConfluentKafka.Clients.Security;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Identity.Client;

namespace JohBloch.ConfluentKafka.Clients.Example.Consumer.Msal.ExternalRefresh.RedisCache;

public sealed class MsalDeviceCodeSecurityTokenProvider(
    IOptions<KafkaClientOptions> kafkaClientOptions,
    IOptions<MsalTokenProviderOptions> msalOptions,
    ILogger<MsalDeviceCodeSecurityTokenProvider> logger) : ISecurityTokenProvider
{
    private readonly KafkaClientOptions _kafkaClientOptions = kafkaClientOptions.Value;
    private readonly MsalTokenProviderOptions _msalOptions = msalOptions.Value;
    private TokenStatus? _lastTokenStatus;

    private readonly Lazy<IPublicClientApplication> _app = new(() =>
    {
        // NOTE: This field initializer runs before instance fields are assigned,
        // so we must use the primary-constructor parameter here.
        var options = msalOptions.Value;
        var authority = GetAuthority(options);

        var builder = PublicClientApplicationBuilder
            .Create(options.ClientId)
            .WithAuthority(authority);

        var app = builder.Build();

        var cacheFilePath = GetCacheFilePath(options);
        if (!string.IsNullOrWhiteSpace(cacheFilePath))
        {
            SimpleFileTokenCache.Bind(app.UserTokenCache, cacheFilePath);
        }

        return app;
    });

    public async Task<AccessToken> GetAccessTokenAsync(CancellationToken cancellationToken = default)
    {
        var scopes = ParseScopes(_msalOptions.Scopes);

        if (scopes.Length == 0)
        {
            throw new InvalidOperationException("MSAL scopes are missing. Set 'Msal:Scopes' in local settings.");
        }

        var app = _app.Value;
        var accounts = await app.GetAccountsAsync().ConfigureAwait(false);
        var account = accounts.FirstOrDefault();

        try
        {
            var result = await app
                .AcquireTokenSilent(scopes, account)
                .ExecuteAsync(cancellationToken)
                .ConfigureAwait(false);

            _lastTokenStatus = new TokenStatus(result.ExpiresOn.UtcDateTime);
            return new AccessToken(result.AccessToken, result.ExpiresOn);
        }
        catch (MsalUiRequiredException)
        {
            logger.LogWarning("No cached token available. Starting device-code flow.");

            var result = await app
                .AcquireTokenWithDeviceCode(scopes, callback =>
                {
                    logger.LogWarning("{DeviceCodeMessage}", callback.Message);
                    return Task.CompletedTask;
                })
                .ExecuteAsync(cancellationToken)
                .ConfigureAwait(false);

            _lastTokenStatus = new TokenStatus(result.ExpiresOn.UtcDateTime);
            return new AccessToken(result.AccessToken, result.ExpiresOn);
        }
    }

    public TokenStatus GetTokenStatus()
    {
        return _lastTokenStatus ?? new TokenStatus(DateTime.MinValue);
    }

    public Dictionary<string, string>? GetExtensions()
    {
        var extensions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (!string.IsNullOrWhiteSpace(_kafkaClientOptions.OAuthLogicalCluster))
        {
            extensions["logicalCluster"] = _kafkaClientOptions.OAuthLogicalCluster;
        }

        if (!string.IsNullOrWhiteSpace(_kafkaClientOptions.OAuthIdentityPoolId))
        {
            extensions["identityPoolId"] = _kafkaClientOptions.OAuthIdentityPoolId;
        }

        return extensions.Count == 0 ? null : extensions;
    }

    public Dictionary<string, string>? GetKafkaSaslConfig()
    {
        // IMPORTANT:
        // - This is intentionally "external refresh login": MSAL manages refresh tokens and renews access tokens.
        // - librdkafka still wants OIDC metadata (token endpoint) when using 'sasl.oauthbearer.method=oidc'.
        var tokenEndpointUrl = GetTokenEndpointUrl(_msalOptions);

        var config = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["sasl.mechanism"] = "OAUTHBEARER",
            ["sasl.oauthbearer.method"] = "oidc",
            ["sasl.oauthbearer.token.endpoint.url"] = tokenEndpointUrl,

            // Optional: used by some environments for validation; safe defaults.
            ["sasl.oauthbearer.extensions"] = BuildOauthExtensionsString(GetExtensions()),
        };

        return config;
    }

    private static string[] ParseScopes(string scopes)
    {
        return (scopes ?? string.Empty)
            .Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    private static string? GetCacheFilePath(MsalTokenProviderOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.CacheFilePath))
        {
            return options.CacheFilePath;
        }

        // Default under repo folder, next to the project.
        return Path.Combine(AppContext.BaseDirectory, "msal-cache.bin");
    }

    private static string GetAuthority(MsalTokenProviderOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.Authority))
        {
            return options.Authority;
        }

        if (string.IsNullOrWhiteSpace(options.TenantId))
        {
            throw new InvalidOperationException("MSAL authority is missing. Set 'Msal:TenantId' or 'Msal:Authority'.");
        }

        return $"https://login.microsoftonline.com/{options.TenantId}";
    }

    private static string GetTokenEndpointUrl(MsalTokenProviderOptions options)
    {
        if (!string.IsNullOrWhiteSpace(options.TokenEndpointUrl))
        {
            return options.TokenEndpointUrl;
        }

        var authority = GetAuthority(options).TrimEnd('/');
        return $"{authority}/oauth2/v2.0/token";
    }

    private static string BuildOauthExtensionsString(Dictionary<string, string>? extensions)
    {
        if (extensions is null || extensions.Count == 0)
        {
            return string.Empty;
        }

        return string.Join(",", extensions.Select(kvp => $"{kvp.Key}={kvp.Value}"));
    }
}
