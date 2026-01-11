namespace JohBloch.ConfluentKafka.Clients.Models;

/// <summary>
/// Per-message delivery outcome for Kafka sends.
/// </summary>
public sealed class KafkaResult
{
    /// <summary>Whether the send operation succeeded.</summary>
    public bool Success { get; set; }

    /// <summary>Topic name of the delivered message.</summary>
    public string? Topic { get; set; }

    /// <summary>Partition number of the delivered message.</summary>
    public int Partition { get; set; }

    /// <summary>Offset of the delivered message within the partition.</summary>
    public long Offset { get; set; }

    /// <summary>Message key used for partitioning.</summary>
    public string? Key { get; set; }

    /// <summary>Error message when send fails.</summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// Initializes an empty result.
    /// </summary>
    public KafkaResult() { }

    /// <summary>
    /// Initializes a new result.
    /// </summary>
    /// <param name="success">True when delivered.</param>
    /// <param name="topic">Topic name.</param>
    /// <param name="partition">Partition number.</param>
    /// <param name="offset">Delivery offset.</param>
    /// <param name="key">Message key.</param>
    /// <param name="errorMessage">Error message on failure.</param>
    public KafkaResult(bool success, string topic = "", int partition = 0, long offset = 0, string? key = null, string? errorMessage = null)
    {
        Success = success;
        Topic = topic;
        Partition = partition;
        Offset = offset;
        Key = key;
        ErrorMessage = errorMessage;
    }
}
