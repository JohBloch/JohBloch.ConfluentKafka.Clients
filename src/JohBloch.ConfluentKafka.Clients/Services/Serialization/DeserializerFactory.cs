using JohBloch.ConfluentKafka.Clients.Services.Serialization.Avro;
using JohBloch.ConfluentKafka.Clients.Services.Serialization.Json;
using JohBloch.ConfluentKafka.Clients.Services.Serialization.Protobuf;

namespace JohBloch.ConfluentKafka.Clients.Services.Serialization
{
    /// <summary>
    /// Factory for creating message deserializers based on schema type.
    /// </summary>
    public class DeserializerFactory
    {
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly ILoggerFactory _loggerFactory;

        /// <summary>
        /// Initializes a new instance of the DeserializerFactory class.
        /// </summary>
        /// <param name="schemaRegistry">The schema registry client.</param>
        /// <param name="loggerFactory">The logger factory.</param>
        public DeserializerFactory(ISchemaRegistryClient schemaRegistry, ILoggerFactory loggerFactory)
        {
            _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        }

        /// <summary>
        /// Creates a deserializer for the specified schema type and target type.
        /// </summary>
        /// <typeparam name="T">The target type to deserialize into.</typeparam>
        /// <param name="schemaType">The schema type to use for deserialization.</param>
        /// <returns>An appropriate deserializer instance.</returns>
        public IMessageDeserializer<T> Create<T>(Models.SchemaType schemaType)
        {
            return schemaType switch
            {
                Models.SchemaType.Avro => new AvroDeserializer<T>(_schemaRegistry, _loggerFactory.CreateLogger<AvroDeserializer<T>>()),
                Models.SchemaType.Protobuf => CreateProtobufDeserializer<T>(),
                Models.SchemaType.Json => new Json.JsonDeserializer<T>(_schemaRegistry, _loggerFactory.CreateLogger<Json.JsonDeserializer<T>>()),
                _ => throw new NotSupportedException($"Schema type {schemaType} is not supported")
            };
        }

        private IMessageDeserializer<T> CreateProtobufDeserializer<T>()
        {
            var targetType = typeof(T);
            var protobufDeserializerType = typeof(ProtobufDeserializer<>).MakeGenericType(targetType);
            var protobufLogger = _loggerFactory.CreateLogger(protobufDeserializerType.FullName ?? protobufDeserializerType.Name);
            return (IMessageDeserializer<T>)Activator.CreateInstance(protobufDeserializerType, _schemaRegistry, protobufLogger)!;
        }

        /// <summary>
        /// Detects the schema type from the raw message bytes and Schema Registry metadata.
        /// </summary>
        /// <param name="data">The raw message bytes.</param>
        /// <param name="topic">The topic name.</param>
        /// <returns>The detected schema type.</returns>
        public async Task<Models.SchemaType> DetectSchemaTypeAsync(byte[] data, string topic)
        {
            try
            {
                // Check if data has Schema Registry wire format (magic byte + schema ID)
                if (data.Length < 5 || data[0] != 0x00)
                {
                    // No magic byte - assume plain JSON
                    return Models.SchemaType.Json;
                }

                // Extract schema ID (big-endian)
                var schemaId = (data[1] << 24) | (data[2] << 16) | (data[3] << 8) | data[4];

                // Fetch schema from registry
                var schema = await _schemaRegistry.GetSchemaAsync(schemaId, "serialized");

                // Determine type from schema metadata (SchemaType is an enum from Confluent)
                return schema.SchemaType switch
                {
                    Confluent.SchemaRegistry.SchemaType.Avro => Models.SchemaType.Avro,
                    Confluent.SchemaRegistry.SchemaType.Protobuf => Models.SchemaType.Protobuf,
                    Confluent.SchemaRegistry.SchemaType.Json => Models.SchemaType.Json,
                    _ => Models.SchemaType.Avro // Default to Avro for backward compatibility
                };
            }
            catch (Exception)
            {
                // If schema detection fails, default to Avro
                return Models.SchemaType.Avro;
            }
        }
    }
}
