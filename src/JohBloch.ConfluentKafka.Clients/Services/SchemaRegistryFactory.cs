using System.ComponentModel;

namespace JohBloch.ConfluentKafka.Clients.Services
{
    /// <summary>
    /// Factory for creating configured instances of <see cref="ISchemaRegistryClient"/>.
    /// Uses <see cref="SchemaRegistryOptions"/> to set OAuth and connectivity.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class SchemaRegistryFactory : ISchemaRegistryFactory
    {
        /// <summary>
        /// Schema Registry options used to configure the client.
        /// </summary>
        private readonly SchemaRegistryOptions _srOptions;

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaRegistryFactory"/> class.
        /// </summary>
        /// <param name="schemaRegistryOptions">Strongly-typed options for Schema Registry configuration.</param>
        public SchemaRegistryFactory(IOptions<SchemaRegistryOptions> schemaRegistryOptions)
        {
            if (schemaRegistryOptions is null)
                throw new ArgumentNullException(nameof(schemaRegistryOptions));
            if (schemaRegistryOptions.Value is null)
                throw new ArgumentNullException(nameof(schemaRegistryOptions));

            _srOptions = schemaRegistryOptions.Value;
        }

        /// <summary>
        /// Creates a new cached Schema Registry client configured for OAuth bearer authentication.
        /// </summary>
        /// <returns>An <see cref="ISchemaRegistryFactory"/> instance.</returns>
        public ISchemaRegistryClient CreateClient()
        {
            if (_srOptions is null)
                throw new InvalidOperationException("Schema registry options not initialized.");
            if (string.IsNullOrWhiteSpace(_srOptions.Url))
                throw new ArgumentException("SchemaRegistryOptions.Url must be provided", nameof(_srOptions.Url));

            var config = new SchemaRegistryConfig
            {
                Url = _srOptions.Url
            };

            // Only enable OAuth bearer auth when it's explicitly configured.
            if (!string.IsNullOrWhiteSpace(_srOptions.TokenEndpointUrl) &&
                !string.IsNullOrWhiteSpace(_srOptions.ClientId) &&
                !string.IsNullOrWhiteSpace(_srOptions.ClientSecret))
            {
                config.BearerAuthCredentialsSource = BearerAuthCredentialsSource.OAuthBearer;
                config.BearerAuthClientId = _srOptions.ClientId;
                config.BearerAuthClientSecret = _srOptions.ClientSecret;
                config.BearerAuthScope = _srOptions.Scope;
                config.BearerAuthLogicalCluster = _srOptions.LogicalCluster;
                config.BearerAuthTokenEndpointUrl = _srOptions.TokenEndpointUrl;
                config.BearerAuthIdentityPoolId = _srOptions.IdentityPoolId;
            }

            return new CachedSchemaRegistryClient(config);
        }
    }
}
