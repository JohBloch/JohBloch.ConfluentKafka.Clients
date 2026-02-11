namespace JohBloch.ConfluentKafka.Clients.Security;

/// <summary>
/// Abstraction for acquiring and inspecting access tokens (compatible with MSAL-based implementations).
/// </summary>
public interface ISecurityTokenProvider
{
    /// <summary>
    /// Acquire an access token for Kafka (OAuth Bearer). Implementations may use MSAL.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Access token and expiry.</returns>
    Task<AccessToken> GetAccessTokenAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Return current token status (expiry, etc.).
    /// </summary>
    /// <returns>Status information for the current token.</returns>
    TokenStatus GetTokenStatus();

    /// <summary>
    /// Optional extensions to include with OAuth bearer token (e.g., logicalCluster).
    /// </summary>
    /// <returns>Dictionary of extension key/values or null.</returns>
    Dictionary<string, string>? GetExtensions();

    /// <summary>
    /// Optional extra SASL configuration key/values for Confluent client.
    /// </summary>
    /// <returns>Dictionary of SASL settings or null when using dynamic token refresh only.</returns>
    Dictionary<string, string>? GetKafkaSaslConfig();
}

/// <summary>
/// Returned access token value.
/// </summary>
public sealed class AccessToken
{
    /// <summary>
    /// Access token string value.
    /// </summary>
    public string AccessTokenValue { get; }

    /// <summary>
    /// Time when the token expires.
    /// </summary>
    public DateTimeOffset ExpiresOn { get; }

    /// <summary>
    /// Creates a new <see cref="AccessToken"/>.
    /// </summary>
    /// <param name="accessTokenValue">Token string.</param>
    /// <param name="expiresOn">Expiry time.</param>
    public AccessToken(string accessTokenValue, DateTimeOffset expiresOn)
    {
        AccessTokenValue = accessTokenValue;
        ExpiresOn = expiresOn;
    }
}

/// <summary>
/// Token status info used by OAuth refresh handler.
/// </summary>
public sealed class TokenStatus
{
    /// <summary>
    /// UTC time when the token expires.
    /// </summary>
    public DateTime ExpiresAt { get; }

    /// <summary>
    /// Creates a new <see cref="TokenStatus"/>.
    /// </summary>
    /// <param name="expiresAt">Expiry time in UTC.</param>
    public TokenStatus(DateTime expiresAt)
    {
        ExpiresAt = expiresAt;
    }
}
