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
        MsalTokenProviderOptions options = msalOptions.Value;
        string authority = GetAuthority(options);

        PublicClientApplicationBuilder builder = PublicClientApplicationBuilder
            .Create(options.ClientId)
            .WithAuthority(authority);

        IPublicClientApplication app = builder.Build();

        string? cacheFilePath = GetCacheFilePath(options);
        if (!string.IsNullOrWhiteSpace(cacheFilePath))
        {
            SimpleFileTokenCache.Bind(app.UserTokenCache, cacheFilePath);
        }

        return app;
    });

    public async Task<AccessToken> GetAccessTokenAsync(CancellationToken cancellationToken = default)
    {
        string[] scopes = ParseScopes(_msalOptions.Scopes);

        if (scopes.Length == 0)
        {
            throw new InvalidOperationException("MSAL scopes are missing. Set 'Msal:Scopes' in local settings.");
        }

        IPublicClientApplication app = _app.Value;
        System.Collections.Generic.IEnumerable<IAccount> accounts = await app.GetAccountsAsync().ConfigureAwait(false);
        IAccount? account = accounts.FirstOrDefault();

        try
        {
            AuthenticationResult result = await app
                .AcquireTokenSilent(scopes, account)
                .ExecuteAsync(cancellationToken)
                .ConfigureAwait(false);

            _lastTokenStatus = new TokenStatus(result.ExpiresOn.UtcDateTime);
            return new AccessToken(result.AccessToken, result.ExpiresOn);
        }
        catch (MsalUiRequiredException)
        {
            logger.LogWarning("No cached token available. Starting device-code flow.");

            AuthenticationResult result = await app
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
        return GetExtensions(
            _kafkaClientOptions.OAuth.LogicalCluster,
            _kafkaClientOptions.OAuth.IdentityPoolId);
    }

    internal static Dictionary<string, string>? GetExtensions(string? logicalCluster, string? identityPoolId)
    {
        Dictionary<string, string>? extensions = null;

        if (!string.IsNullOrWhiteSpace(logicalCluster))
        {
            extensions ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            extensions["logicalCluster"] = logicalCluster.Trim();
        }

        if (!string.IsNullOrWhiteSpace(identityPoolId))
        {
            extensions ??= new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            extensions["identityPoolId"] = identityPoolId.Trim();
        }

        return extensions;
    }

    public Dictionary<string, string>? GetKafkaSaslConfig()
    {
        // IMPORTANT:
        // - This is intentionally "external refresh login": MSAL manages refresh tokens and renews access tokens.
        // - librdkafka still wants OIDC metadata (token endpoint) when using 'sasl.oauthbearer.method=oidc'.
        string tokenEndpointUrl = GetTokenEndpointUrl(_msalOptions);

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

        string authority = GetAuthority(options).TrimEnd('/');
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
