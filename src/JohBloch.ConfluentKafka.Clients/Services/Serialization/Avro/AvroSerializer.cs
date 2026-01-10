namespace JohBloch.ConfluentKafka.Clients.Services.Serialization.Avro
{
    /// <summary>
    /// Avro serializer using Chr.Avro with Schema Registry support.
    /// </summary>
    public class AvroSerializer<T> : IMessageSerializer<T>
    {
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly ILogger<AvroSerializer<T>> _logger;

        /// <summary>
        /// Initializes a new instance of the AvroSerializer class.
        /// </summary>
        public AvroSerializer(ISchemaRegistryClient schemaRegistry, ILogger<AvroSerializer<T>> logger)
        {
            _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Serializes a value to a byte array using Avro format.
        /// </summary>
        public async Task<byte[]> SerializeAsync(T value, SerializationContext context)
        {
            try
            {
                var serializer = new AsyncSchemaRegistrySerializer<T>(_schemaRegistry);
                return await serializer.SerializeAsync(value, context);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to serialize Avro message for topic {Topic}", context.Topic);
                throw;
            }
        }
    }
}
