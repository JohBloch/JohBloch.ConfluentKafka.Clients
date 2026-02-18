using System.Net.Http.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;

namespace JohBloch.ConfluentKafka.Clients.Security;

internal static class OAuthClientCredentialsTokenClient
{
    public static async Task<AccessToken> RequestTokenAsync(
        HttpClient httpClient,
        string tokenEndpointUrl,
        string clientId,
        string clientSecret,
        string? scope,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(tokenEndpointUrl))
        {
            throw new InvalidOperationException("OAuth token endpoint URL is required.");
        }

        if (!Uri.TryCreate(tokenEndpointUrl, UriKind.Absolute, out var endpoint))
        {
            throw new InvalidOperationException($"Invalid OAuth token endpoint URL '{tokenEndpointUrl}'. Expected an absolute URL.");
        }

        if (string.IsNullOrWhiteSpace(clientId))
        {
            throw new InvalidOperationException("OAuth client id is required.");
        }

        if (string.IsNullOrWhiteSpace(clientSecret))
        {
            throw new InvalidOperationException("OAuth client secret is required.");
        }

        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug(
                "OAuth token request (client_credentials): endpoint={Endpoint}, clientId={ClientId}, scope={Scope}",
                endpoint,
                Mask(clientId),
                string.IsNullOrWhiteSpace(scope) ? "<empty>" : scope);
        }

        var formData = new Dictionary<string, string>
        {
            ["grant_type"] = "client_credentials",
            ["client_id"] = clientId,
            ["client_secret"] = clientSecret
        };

        if (!string.IsNullOrWhiteSpace(scope))
        {
            formData["scope"] = scope;
        }

        using var content = new FormUrlEncodedContent(formData);
        using HttpResponseMessage response = await httpClient.PostAsync(endpoint, content, cancellationToken).ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            string errorContent = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            logger.LogWarning(
                "OAuth token request failed: status={StatusCode}, endpoint={Endpoint}, body={Body}",
                (int)response.StatusCode,
                endpoint,
                Truncate(errorContent, 2048));

            throw new HttpRequestException($"OAuth token request failed: {response.StatusCode}. Details: {errorContent}");
        }

        OAuthResponse? result = await response.Content
            .ReadFromJsonAsync<OAuthResponse>(cancellationToken: cancellationToken)
            .ConfigureAwait(false);

        if (result is null || string.IsNullOrWhiteSpace(result.AccessToken))
        {
            throw new InvalidOperationException("OAuth response was empty or invalid JSON (missing access_token).");
        }

        // Default to 1 hour if not provided
        int expirySeconds = result.ExpiresIn > 0 ? result.ExpiresIn : 3600;
        if (logger.IsEnabled(LogLevel.Debug))
        {
            logger.LogDebug("OAuth token acquired: tokenType={TokenType}, expiresIn={ExpiresInSeconds}s", result.TokenType, expirySeconds);
        }

        return new AccessToken(result.AccessToken, DateTimeOffset.UtcNow.AddSeconds(expirySeconds));
    }

    private static string Mask(string value)
    {
        value = value.Trim();
        if (value.Length <= 8) return "***";
        return value[..3] + "***" + value[^3..];
    }

    private static string Truncate(string? value, int maxLen)
    {
        if (string.IsNullOrEmpty(value)) return string.Empty;
        return value.Length <= maxLen ? value : value[..maxLen] + "...";
    }

    private sealed class OAuthResponse
    {
        [JsonPropertyName("access_token")]
        public string AccessToken { get; set; } = string.Empty;

        [JsonPropertyName("expires_in")]
        public int ExpiresIn { get; set; }

        [JsonPropertyName("token_type")]
        public string TokenType { get; set; } = string.Empty;
    }
}
