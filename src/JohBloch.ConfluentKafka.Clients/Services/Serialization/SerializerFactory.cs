using JohBloch.ConfluentKafka.Clients.Services.Serialization.Avro;
using JohBloch.ConfluentKafka.Clients.Services.Serialization.Json;
using JohBloch.ConfluentKafka.Clients.Services.Serialization.Protobuf;

namespace JohBloch.ConfluentKafka.Clients.Services.Serialization
{
    /// <summary>
    /// Factory for creating message serializers based on schema type.
    /// </summary>
    public class SerializerFactory
    {
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly ILoggerFactory _loggerFactory;

        /// <summary>
        /// Initializes a new instance of the SerializerFactory class.
        /// </summary>
        /// <param name="schemaRegistry">The schema registry client.</param>
        /// <param name="loggerFactory">The logger factory.</param>
        public SerializerFactory(ISchemaRegistryClient schemaRegistry, ILoggerFactory loggerFactory)
        {
            _schemaRegistry = schemaRegistry ?? throw new ArgumentNullException(nameof(schemaRegistry));
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        }

        /// <summary>
        /// Creates a serializer for the specified schema type and source type.
        /// </summary>
        /// <typeparam name="T">The source type to serialize from.</typeparam>
        /// <param name="schemaType">The schema type to use for serialization.</param>
        /// <returns>An appropriate serializer instance.</returns>
        public IMessageSerializer<T> Create<T>(Models.SchemaType schemaType)
        {
            return schemaType switch
            {
                Models.SchemaType.Avro => new AvroSerializer<T>(_schemaRegistry, _loggerFactory.CreateLogger<AvroSerializer<T>>()),
                Models.SchemaType.Protobuf => CreateProtobufSerializer<T>(),
                Models.SchemaType.Json => new Json.JsonSerializer<T>(_schemaRegistry, _loggerFactory.CreateLogger<Json.JsonSerializer<T>>()),
                _ => throw new NotSupportedException($"Schema type {schemaType} is not supported")
            };
        }

        private IMessageSerializer<T> CreateProtobufSerializer<T>()
        {
            var targetType = typeof(T);
            var protobufSerializerType = typeof(ProtobufSerializer<>).MakeGenericType(targetType);
            var protobufLogger = _loggerFactory.CreateLogger(protobufSerializerType.FullName ?? protobufSerializerType.Name);
            return (IMessageSerializer<T>)Activator.CreateInstance(protobufSerializerType, _schemaRegistry, protobufLogger)!;
        }

        /// <summary>
        /// Gets the schema type for a given topic from the Schema Registry.
        /// </summary>
        /// <param name="topic">The topic name.</param>
        /// <param name="isKey">Whether this is for a key (true) or value (false).</param>
        /// <returns>The detected schema type.</returns>
        public async Task<Models.SchemaType> GetSchemaTypeForTopicAsync(string topic, bool isKey = false)
        {
            try
            {
                var subject = isKey ? $"{topic}-key" : $"{topic}-value";
                var latestSchema = await _schemaRegistry.GetLatestSchemaAsync(subject);

                return latestSchema.SchemaType switch
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
