namespace JohBloch.ConfluentKafka.Clients.Interfaces;

/// <summary>
/// Generic interface for serializing messages to different schema formats.
/// </summary>
public interface IMessageSerializer<T>
{
    /// <summary>
    /// Serializes a value to a byte array.
    /// </summary>
    /// <param name="value">The value to serialize.</param>
    /// <param name="context">The serialization context.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the serialized byte array.</returns>
    Task<byte[]> SerializeAsync(T value, SerializationContext context);
}
