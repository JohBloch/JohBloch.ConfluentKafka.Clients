namespace JohBloch.ConfluentKafka.Clients.Models;

/// <summary>
/// Result of a batch operation to Kafka
/// </summary>
public class BatchResult
{
    private readonly List<KafkaResult> _results = new();

    /// <summary>Total messages attempted.</summary>
    public int TotalMessages { get; }

    /// <summary>Total successful deliveries.</summary>
    public int SuccessCount { get; private set; }

    /// <summary>Total delivery failures.</summary>
    public int FailureCount { get; private set; }

    /// <summary>Optional error message for batch-level failure.</summary>
    public string? ErrorMessage { get; set; }

    /// <summary>List of individual results.</summary>
    public IReadOnlyList<KafkaResult> Results => _results;

    /// <summary>
    /// Creates a new batch result.
    /// </summary>
    /// <param name="total">Total messages in the batch.</param>
    public BatchResult(int total)
    {
        TotalMessages = total;
    }

    /// <summary>Add a successful delivery result.</summary>
    public void AddSuccess(string topic, int partition, long offset, string key)
    {
        SuccessCount++;
        _results.Add(new KafkaResult(true, topic, partition, offset, key));
    }

    /// <summary>Add a failure result.</summary>
    public void AddFailure(string error)
    {
        FailureCount++;
        _results.Add(new KafkaResult(false, errorMessage: error));
    }

    /// <summary>Return a success result for empty batches.</summary>
    public BatchResult SucceedEmpty()
    {
        SuccessCount = TotalMessages;
        return this;
    }
}
