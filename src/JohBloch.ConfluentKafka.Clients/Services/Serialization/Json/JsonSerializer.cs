namespace JohBloch.ConfluentKafka.Clients.Services.Serialization.Json
{
    /// <summary>
    /// JSON serializer with Schema Registry support.
    /// </summary>
    public class JsonSerializer<T> : IMessageSerializer<T>
    {
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly ILogger<JsonSerializer<T>> _logger;
        private readonly JsonSerializerOptions _jsonOptions;

        /// <summary>
        /// Initializes a new instance of the JsonSerializer class.
        /// </summary>
        public JsonSerializer(ISchemaRegistryClient schemaRegistry, ILogger<JsonSerializer<T>> logger)
        {
            _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _jsonOptions = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = false
            };
        }

        /// <summary>
        /// Serializes a value to a byte array using JSON format.
        /// </summary>
        public async Task<byte[]> SerializeAsync(T value, SerializationContext context)
        {
            try
            {
                var jsonString = System.Text.Json.JsonSerializer.Serialize(value, _jsonOptions);
                return await Task.FromResult(Encoding.UTF8.GetBytes(jsonString));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to serialize JSON message for topic {Topic}", context.Topic);
                throw;
            }
        }
    }
}
