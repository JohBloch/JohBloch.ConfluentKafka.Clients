namespace JohBloch.ConfluentKafka.Clients.Models;

/// <summary>
/// Supported schema types for message serialization/deserialization.
/// </summary>
public enum SchemaType
{
    /// <summary>Apache Avro schema format.</summary>
    Avro,

    /// <summary>Protocol Buffers schema format.</summary>
    Protobuf,

    /// <summary>JSON Schema format.</summary>
    Json
}
