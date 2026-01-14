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
            _logger.LogInformation("Fetching new OAuth token from {Endpoint}", _options.OAuthTokenEndpoint);
            var response = await FetchTokenInternalAsync(cancellationToken);
            
            // Default to 1 hour if not provided
            var expirySeconds = response.ExpiresIn > 0 ? response.ExpiresIn : 3600;
            
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
        // Can be extended to support 'logicalCluster' or other OAuth extensions
        return null;
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
        // We still use the .NET refresh handler to fetch and set tokens.
        return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["security.protocol"] = "SASL_SSL",
            ["sasl.mechanism"] = "OAUTHBEARER",

            // Many brokers use the OIDC method; endpoint url also helps future-proofing.
            ["sasl.oauthbearer.method"] = "OIDC",
            ["sasl.oauthbearer.token.endpoint.url"] = _options.OAuthTokenEndpoint!
        };
    }

    private bool IsOAuthEnabled()
    {
        return !string.IsNullOrWhiteSpace(_options.OAuthTokenEndpoint)
               || !string.IsNullOrWhiteSpace(_options.OAuthClientId)
               || !string.IsNullOrWhiteSpace(_options.OAuthClientSecret)
               || !string.IsNullOrWhiteSpace(_options.OAuthScope);
    }

    private void ValidateOAuthOptionsIfEnabled()
    {
        if (!IsOAuthEnabled())
        {
            return;
        }

        var missing = new List<string>(capacity: 3);
        if (string.IsNullOrWhiteSpace(_options.OAuthTokenEndpoint)) missing.Add(nameof(KafkaClientOptions.OAuthTokenEndpoint));
        if (string.IsNullOrWhiteSpace(_options.OAuthClientId)) missing.Add(nameof(KafkaClientOptions.OAuthClientId));
        if (string.IsNullOrWhiteSpace(_options.OAuthClientSecret)) missing.Add(nameof(KafkaClientOptions.OAuthClientSecret));

        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                $"OAuth2 is enabled (some OAuth settings are present), but the following required setting(s) are missing: {string.Join(", ", missing)}");
        }

        if (!Uri.TryCreate(_options.OAuthTokenEndpoint, UriKind.Absolute, out _))
        {
            throw new InvalidOperationException(
                $"Invalid OAuthTokenEndpoint '{_options.OAuthTokenEndpoint}'. Expected an absolute URL.");
        }
    }

    private async Task<OAuthResponse> FetchTokenInternalAsync(CancellationToken cancellationToken)
    {
        var formData = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = _options.OAuthClientId ?? string.Empty,
            ["client_secret"] = _options.OAuthClientSecret ?? string.Empty
        };

        if (!string.IsNullOrEmpty(_options.OAuthScope))
        {
            formData["scope"] = _options.OAuthScope;
        }

        using var content = new FormUrlEncodedContent(formData);
        using var response = await _httpClient.PostAsync(_options.OAuthTokenEndpoint, content, cancellationToken);
        
        if (!response.IsSuccessStatusCode)
        {
            var errorContent = await response.Content.ReadAsStringAsync(cancellationToken);
            throw new HttpRequestException($"OAuth token request failed: {response.StatusCode}. Details: {errorContent}");
        }

        var result = await response.Content.ReadFromJsonAsync<OAuthResponse>(cancellationToken: cancellationToken);
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
}
