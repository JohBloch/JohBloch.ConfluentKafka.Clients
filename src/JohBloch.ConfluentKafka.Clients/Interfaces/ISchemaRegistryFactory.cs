namespace JohBloch.ConfluentKafka.Clients.Interfaces
{
    /// <summary>
    /// Factory API to create configured instances of Schema Registry clients.
    /// </summary>
    public interface ISchemaRegistryFactory
    {
        /// <summary>
        /// Create a new <see cref="ISchemaRegistryFactory"/> configured per application settings.
        /// </summary>
        /// <returns>An initialized Schema Registry client.</returns>
        ISchemaRegistryClient CreateClient();
    }
}
