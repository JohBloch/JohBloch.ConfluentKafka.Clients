namespace JohBloch.ConfluentKafka.Clients.Services.Serialization.Protobuf
{
    /// <summary>
    /// Protobuf serializer supporting POCOs with Schema Registry.
    /// Allows serialization of plain C# classes with [ProtoContract] and [ProtoMember] attributes.
    /// </summary>
    public class ProtobufSerializer<T> : IMessageSerializer<T>
    {
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly ILogger _logger;

        /// <summary>
        /// Initializes a new instance of the ProtobufSerializer class.
        /// </summary>
        public ProtobufSerializer(ISchemaRegistryClient schemaRegistry, ILogger logger)
        {
            _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        /// <summary>
        /// Serializes a POCO to a byte array using protobuf-net.
        /// </summary>
        public async Task<byte[]> SerializeAsync(T value, SerializationContext context)
        {
            try
            {
                using var stream = new MemoryStream();
                ProtoBuf.Serializer.Serialize(stream, value);
                return await Task.FromResult(stream.ToArray());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to serialize Protobuf-net message for topic {Topic}", context.Topic);
                throw;
            }
        }
    }
}
