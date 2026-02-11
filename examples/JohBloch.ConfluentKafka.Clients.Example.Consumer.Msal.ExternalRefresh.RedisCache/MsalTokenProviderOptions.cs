using System.ComponentModel;

namespace JohBloch.ConfluentKafka.Clients.Example.Consumer.Msal.ExternalRefresh.RedisCache;

[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class MsalTokenProviderOptions
{
    public string? TenantId { get; set; }

    public string ClientId { get; set; } = string.Empty;

    /// <summary>
    /// Space-delimited scopes (MSAL expects a string[]). Example: "api://xxx/.default".
    /// </summary>
    public string Scopes { get; set; } = string.Empty;

    /// <summary>
    /// Optional authority base URL. If omitted, defaults to https://login.microsoftonline.com/{TenantId}.
    /// </summary>
    public string? Authority { get; set; }

    /// <summary>
    /// Optional token endpoint URL used for librdkafka validation when sasl.oauthbearer.method=oidc.
    /// If omitted, it is derived from Authority.
    /// </summary>
    public string? TokenEndpointUrl { get; set; }

    /// <summary>
    /// File path used to persist the MSAL token cache (device code login becomes a one-time operation).
    /// </summary>
    public string? CacheFilePath { get; set; }
}
