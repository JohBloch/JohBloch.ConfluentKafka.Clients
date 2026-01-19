namespace JohBloch.ConfluentKafka.Clients.Services.Serialization.Avro
{
    /// <summary>
    /// Avro deserializer using Chr.Avro with Schema Registry support.
    /// </summary>
    /// <typeparam name="T">The target POCO type.</typeparam>
    public class AvroDeserializer<T> : IMessageDeserializer<T>
    {
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly ILogger<AvroDeserializer<T>> _logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="AvroDeserializer{T}"/> class.
        /// </summary>
        /// <param name="schemaRegistry">Schema Registry client.</param>
        /// <param name="logger">Logger instance.</param>
        public AvroDeserializer(ISchemaRegistryClient schemaRegistry, ILogger<AvroDeserializer<T>> logger)
        {
            _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <inheritdoc />
        public async Task<T> DeserializeAsync(byte[] data, SerializationContext context)
        {
            try
            {
                var deserializer = new AsyncSchemaRegistryDeserializer<T>(_schemaRegistry);
                return await deserializer.DeserializeAsync(data, false, context);
            }
            catch (SchemaRegistryException ex)
            {
                _logger.LogError(
                    ex,
                    "Failed to deserialize Avro message from topic {Topic}. Schema Registry request failed (error code: {ErrorCode}). " +
                    "If this is 401, verify Schema Registry auth settings (Kafka__SchemaRegistry__TokenEndpointUrl/ClientId/ClientSecret/Scope and, when applicable, LogicalCluster/IdentityPoolId).",
                    context.Topic,
                    ex.ErrorCode);
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to deserialize Avro message from topic {Topic}", context.Topic);
                throw;
            }
        }
    }
}
