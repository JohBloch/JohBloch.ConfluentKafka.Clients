namespace JohBloch.ConfluentKafka.Clients.Services.Serialization;

/// <summary>
/// Constants and utilities for Confluent Schema Registry wire format.
/// </summary>
public static class SchemaRegistryWireFormat
{
    /// <summary>
    /// Magic byte that indicates Schema Registry wire format (0x00).
    /// </summary>
    public const byte MagicByte = 0x00;

    /// <summary>
    /// Minimum length of Schema Registry wire format message (magic byte + 4-byte schema ID).
    /// </summary>
    public const int MinimumMessageLength = 5;

    /// <summary>
    /// Offset where payload data begins after Schema Registry header (after magic byte + schema ID).
    /// </summary>
    public const int PayloadOffset = 5;

    /// <summary>
    /// Checks if data has Schema Registry wire format.
    /// </summary>
    /// <param name="data">The message data to check.</param>
    /// <returns>True if data has Schema Registry wire format, false otherwise.</returns>
    public static bool HasWireFormat(byte[] data)
    {
        return data.Length >= MinimumMessageLength && data[0] == MagicByte;
    }

    /// <summary>
    /// Extracts the payload from Schema Registry wire format data.
    /// If data doesn't have wire format, returns the original data.
    /// </summary>
    /// <param name="data">The message data.</param>
    /// <returns>The payload without Schema Registry header.</returns>
    public static byte[] ExtractPayload(byte[] data)
    {
        return HasWireFormat(data) ? data[PayloadOffset..] : data;
    }

    /// <summary>
    /// Extracts the schema ID from Schema Registry wire format data.
    /// </summary>
    /// <param name="data">The message data with Schema Registry wire format.</param>
    /// <returns>The schema ID in big-endian format.</returns>
    /// <exception cref="InvalidDataException">If data doesn't have Schema Registry wire format.</exception>
    public static int ExtractSchemaId(byte[] data)
    {
        if (!HasWireFormat(data))
        {
            throw new InvalidDataException("Data does not have Schema Registry wire format");
        }

        // Schema ID is bytes 1-4 in big-endian format
        if (BitConverter.IsLittleEndian)
        {
            return (data[1] << 24) | (data[2] << 16) | (data[3] << 8) | data[4];
        }
        return BitConverter.ToInt32(data, 1);
    }
}
