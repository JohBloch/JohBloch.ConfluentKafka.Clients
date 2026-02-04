namespace JohBloch.ConfluentKafka.Clients.Interfaces
{
    /// <summary>
    /// Generic interface for deserializing messages from different schema formats.
    /// </summary>
    /// <typeparam name="T">The target type to deserialize into.</typeparam>
    public interface IMessageDeserializer<T>
    {
        /// <summary>
        /// Deserializes a byte array into the specified type using the appropriate schema format.
        /// </summary>
        /// <param name="data">The raw message bytes to deserialize.</param>
        /// <param name="context">Serialization context containing topic and message component information.</param>
        /// <returns>The deserialized message of type T.</returns>
        Task<T> DeserializeAsync(byte[] data, SerializationContext context);
    }
}
