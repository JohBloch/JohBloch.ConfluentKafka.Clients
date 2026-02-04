namespace JohBloch.ConfluentKafka.Clients.Services.Serialization.Protobuf
{
    /// <summary>
    /// Protobuf deserializer supporting POCOs with Schema Registry.
    /// Allows deserialization to plain C# classes with [ProtoContract] and [ProtoMember] attributes.
    /// </summary>
    public class ProtobufDeserializer<T> : IMessageDeserializer<T>
    {
        private readonly JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient _schemaRegistry;
        private readonly ILogger _logger;

        /// <summary>
        /// Initializes a new instance of the ProtobufDeserializer class.
        /// </summary>
        public ProtobufDeserializer(JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient schemaRegistry, ILogger logger)
        {
            _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Deserializes a byte array to a POCO using protobuf-net.
        /// </summary>
        public async Task<T> DeserializeAsync(byte[] data, SerializationContext context)
        {
            try
            {
                // Extract Protobuf payload, handling Schema Registry wire format if present
                var protoData = SchemaRegistryWireFormat.ExtractPayload(data);

                // Deserialize using protobuf-net
                using var stream = new MemoryStream(protoData);
                var result = ProtoBuf.Serializer.Deserialize<T>(stream);
                
                return await Task.FromResult(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to deserialize Protobuf-net message from topic {Topic}", context.Topic);
                throw;
            }
        }
    }
}
