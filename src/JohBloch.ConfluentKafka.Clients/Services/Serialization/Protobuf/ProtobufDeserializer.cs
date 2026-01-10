namespace JohBloch.ConfluentKafka.Clients.Services.Serialization.Protobuf
{
    /// <summary>
    /// Protobuf deserializer supporting POCOs with Schema Registry.
    /// Allows deserialization to plain C# classes with [ProtoContract] and [ProtoMember] attributes.
    /// </summary>
    public class ProtobufDeserializer<T> : IMessageDeserializer<T>
    {
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly ILogger _logger;

        /// <summary>
        /// Initializes a new instance of the ProtobufDeserializer class.
        /// </summary>
        public ProtobufDeserializer(ISchemaRegistryClient schemaRegistry, ILogger logger)
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
                // Protocol Buffers wire format with Schema Registry:
                // Byte 0: Magic byte (0x00)
                // Bytes 1-4: Schema ID (big-endian)
                // Bytes 5+: Protobuf message
                
                byte[] protoData;
                
                if (data.Length >= 5 && data[0] == 0x00)
                {
                    // Has Schema Registry wire format - skip magic byte and schema ID
                    protoData = data[5..];
                }
                else
                {
                    // Plain Protobuf data without Schema Registry wrapper
                    protoData = data;
                }

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
