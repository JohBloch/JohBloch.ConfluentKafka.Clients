namespace JohBloch.ConfluentKafka.Clients.Services.Serialization.Json
{
    /// <summary>
    /// JSON deserializer with Schema Registry support.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    public class JsonDeserializer<T> : IMessageDeserializer<T>
    {
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly ILogger<JsonDeserializer<T>> _logger;
        private readonly System.Text.Json.JsonSerializerOptions _jsonOptions;

        /// <summary>
        /// Initializes a new instance of the JsonDeserializer class.
        /// </summary>
        /// <param name="schemaRegistry">The schema registry client.</param>
        /// <param name="logger">The logger instance.</param>
        public JsonDeserializer(ISchemaRegistryClient schemaRegistry, ILogger<JsonDeserializer<T>> logger)
        {
            _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            
            _jsonOptions = new System.Text.Json.JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase
            };
        }

        /// <inheritdoc />
        public Task<T> DeserializeAsync(byte[] data, SerializationContext context)
        {
            try
            {
                // Extract JSON payload, handling Schema Registry wire format if present
                var jsonData = SchemaRegistryWireFormat.ExtractPayload(data);

                var json = System.Text.Encoding.UTF8.GetString(jsonData);
                var result = System.Text.Json.JsonSerializer.Deserialize<T>(json, _jsonOptions);
                
                if (result == null)
                {
                    throw new InvalidDataException("Deserialized JSON result is null");
                }
                
                return Task.FromResult(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to deserialize JSON message from topic {Topic}", context.Topic);
                throw;
            }
        }
    }
}
