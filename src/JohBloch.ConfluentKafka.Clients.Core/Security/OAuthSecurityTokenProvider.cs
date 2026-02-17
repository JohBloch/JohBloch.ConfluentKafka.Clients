using System.Net.Http.Json;
using System.ComponentModel;
using System.Text.Json.Serialization;
using JohBloch.ConfluentKafka.Clients.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace JohBloch.ConfluentKafka.Clients.Security;

/// <summary>
/// Reference implementation of ISecurityTokenProvider using standard OAuth Client Credentials flow.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public class OAuthSecurityTokenProvider : ISecurityTokenProvider
{
    private readonly KafkaClientOptions _options;
    private readonly OAuthConfig _kafkaOAuth;
    private readonly ILogger<OAuthSecurityTokenProvider> _logger;
    private readonly HttpClient _httpClient;
    private AccessToken? _cachedToken;

    /// <summary>
    /// Initializes a new instance of the <see cref="OAuthSecurityTokenProvider"/> class.
    /// </summary>
    /// <param name="options">The Kafka client options containing OAuth credentials and endpoint.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="httpClientFactory">The factory to create HttpClient instances.</param>
    public OAuthSecurityTokenProvider(
        IOptions<KafkaClientOptions> options,
        ILogger<OAuthSecurityTokenProvider> logger,
        IHttpClientFactory httpClientFactory)
    {
        _options = options.Value;
        _kafkaOAuth = OAuthConfig.FromKafkaOptions(_options);
        _logger = logger;
        _httpClient = httpClientFactory.CreateClient("KafkaOAuth");

        // Fail fast if OAuth is intended but required settings are missing.
        ValidateOAuthOptionsIfEnabled();
    }

    /// <inheritdoc />
    public async Task<AccessToken> GetAccessTokenAsync(CancellationToken cancellationToken = default)
    {
        // If OAuth is not configured (e.g. using API Keys), return empty.
        // If OAuth is intended, validation is performed and missing settings will throw.
        if (!IsOAuthEnabled())
        {
            _logger.LogWarning("OAuth is not configured, but GetAccessTokenAsync was called.");
            return new AccessToken(string.Empty, DateTimeOffset.UtcNow);
        }

        ValidateOAuthOptionsIfEnabled();

        // Return cached token if valid (simple in-memory cache)
        if (_cachedToken != null && _cachedToken.ExpiresOn > DateTimeOffset.UtcNow.AddMinutes(5))
        {
            return _cachedToken;
        }

        try
        {
            _logger.LogInformation("Fetching new OAuth token from {Endpoint}", _kafkaOAuth.TokenEndpoint);
            OAuthResponse response = await FetchTokenInternalAsync(cancellationToken);
            
            // Default to 1 hour if not provided
            int expirySeconds = response.ExpiresIn > 0 ? response.ExpiresIn : 3600;
            
            _cachedToken = new AccessToken(
                response.AccessToken, 
                DateTimeOffset.UtcNow.AddSeconds(expirySeconds));
                
            return _cachedToken;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to acquire OAuth token");
            throw;
        }
    }

    /// <inheritdoc />
    public TokenStatus GetTokenStatus()
    {
        if (_cachedToken == null)
        {
            return new TokenStatus(DateTimeOffset.MinValue.UtcDateTime);
        }
        return new TokenStatus(_cachedToken.ExpiresOn.UtcDateTime);
    }
    
    /// <inheritdoc />
    public Dictionary<string, string>? GetExtensions()
    {
        // These extensions are used by some brokers (e.g. Confluent Cloud) for OAUTHBEARER routing.
        // Keys are case-sensitive and must match the broker expectations.
        if (!IsOAuthEnabled())
        {
            return null;
        }

        return GetExtensions(_kafkaOAuth.LogicalCluster, _kafkaOAuth.IdentityPoolId);
    }

    internal static Dictionary<string, string>? GetExtensions(string? logicalCluster, string? identityPoolId)
    {
        Dictionary<string, string>? extensions = null;

        if (!string.IsNullOrWhiteSpace(logicalCluster))
        {
            extensions ??= new Dictionary<string, string>(StringComparer.Ordinal);
            extensions["logicalCluster"] = logicalCluster.Trim();
        }

        if (!string.IsNullOrWhiteSpace(identityPoolId))
        {
            extensions ??= new Dictionary<string, string>(StringComparer.Ordinal);
            extensions["identityPoolId"] = identityPoolId.Trim();
        }

        return extensions;
    }

    /// <inheritdoc />
    public Dictionary<string, string>? GetKafkaSaslConfig()
    {
        // Return SASL settings when OAuth is configured so both consumer and producer
        // can consistently enable OAUTHBEARER based on the same options.
        if (!IsOAuthEnabled())
        {
            return null;
        }

        ValidateOAuthOptionsIfEnabled();

        // Use standard librdkafka keys.
        // Note: some clusters validate OIDC config and require sasl.oauthbearer.client.id
        // when sasl.oauthbearer.method=oidc, even if tokens are provided via the refresh handler.
        var cfg = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["security.protocol"] = "SASL_SSL",
            ["sasl.mechanism"] = "OAUTHBEARER",

            // Many brokers use the OIDC method; endpoint url also helps future-proofing.
            ["sasl.oauthbearer.method"] = "OIDC",
            ["sasl.oauthbearer.token.endpoint.url"] = _kafkaOAuth.TokenEndpoint!
        };

        // Add OIDC-required client id (and commonly required secret) for brokers/librdkafka validation.
        // These are still useful even when tokens are set via OAuthBearerSetToken refresh callbacks.
        cfg["sasl.oauthbearer.client.id"] = _kafkaOAuth.ClientId!;
        cfg["sasl.oauthbearer.client.secret"] = _kafkaOAuth.ClientSecret!;

        if (!string.IsNullOrWhiteSpace(_kafkaOAuth.Scope))
        {
            // librdkafka expects a space-delimited scope string.
            cfg["sasl.oauthbearer.scope"] = _kafkaOAuth.Scope!;
        }

        return cfg;
    }

    private bool IsOAuthEnabled()
    {
        return _kafkaOAuth.IsConfigured;
    }

    private void ValidateOAuthOptionsIfEnabled()
    {
        if (!IsOAuthEnabled())
        {
            return;
        }

        var missing = new List<string>(capacity: 3);
        if (string.IsNullOrWhiteSpace(_kafkaOAuth.TokenEndpoint))
        {
            missing.Add(nameof(KafkaClientOptions.KafkaOauthTokenEndpoint));
        }
        if (string.IsNullOrWhiteSpace(_kafkaOAuth.ClientId))
        {
            missing.Add(nameof(KafkaClientOptions.KafkaOauthClientId));
        }
        if (string.IsNullOrWhiteSpace(_kafkaOAuth.ClientSecret))
        {
            missing.Add(nameof(KafkaClientOptions.KafkaOauthClientSecret));
        }

        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                $"OAuth2 is enabled (some OAuth settings are present), but the following required setting(s) are missing: {string.Join(", ", missing)}");
        }

        if (!Uri.TryCreate(_kafkaOAuth.TokenEndpoint, UriKind.Absolute, out _))
        {
            throw new InvalidOperationException(
            $"Invalid KafkaOauthTokenEndpoint '{_kafkaOAuth.TokenEndpoint}'. Expected an absolute URL.");
        }
    }

    private async Task<OAuthResponse> FetchTokenInternalAsync(CancellationToken cancellationToken)
    {
        var formData = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = _kafkaOAuth.ClientId ?? string.Empty,
            ["client_secret"] = _kafkaOAuth.ClientSecret ?? string.Empty
        };

        if (!string.IsNullOrEmpty(_kafkaOAuth.Scope))
        {
            formData["scope"] = _kafkaOAuth.Scope;
        }

        using var content = new FormUrlEncodedContent(formData);
        using HttpResponseMessage response = await _httpClient.PostAsync(_kafkaOAuth.TokenEndpoint, content, cancellationToken);
        
        if (!response.IsSuccessStatusCode)
        {
            string errorContent = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException($"OAuth token request failed: {response.StatusCode}. Details: {errorContent}");
        }

        OAuthResponse? result = await response.Content.ReadFromJsonAsync<OAuthResponse>(cancellationToken: cancellationToken);
        return result ?? throw new InvalidOperationException("OAuth response was empty or invalid JSON");
    }

    private class OAuthResponse
    {
        [JsonPropertyName("access_token")]
        public string AccessToken { get; set; } = string.Empty;

        [JsonPropertyName("expires_in")]
        public int ExpiresIn { get; set; }

        [JsonPropertyName("token_type")]
        public string TokenType { get; set; } = string.Empty;
    }

    private sealed record OAuthConfig(
        string? TokenEndpoint,
        string? ClientId,
        string? ClientSecret,
        string? Scope,
        string? LogicalCluster,
        string? IdentityPoolId)
    {
        public bool IsConfigured =>
            !string.IsNullOrWhiteSpace(TokenEndpoint)
            || !string.IsNullOrWhiteSpace(ClientId)
            || !string.IsNullOrWhiteSpace(ClientSecret)
            || !string.IsNullOrWhiteSpace(Scope);

        public static OAuthConfig FromKafkaOptions(KafkaClientOptions options)
        {
            return new OAuthConfig(
                TokenEndpoint: options.KafkaOauthTokenEndpoint,
                ClientId: options.KafkaOauthClientId,
                ClientSecret: options.KafkaOauthClientSecret,
                Scope: options.KafkaOauthScope,
                LogicalCluster: options.KafkaOauthLogicalCluster,
                IdentityPoolId: options.KafkaOauthIdentityPoolId);
        }
    }
}
