namespace JohBloch.ConfluentKafka.Clients.Models;

/// <summary>
/// Represents a dead letter queue message containing information about a failed message.
/// </summary>
public class DeadLetterMessage
{
    /// <summary>
    /// The original topic where the message failed.
    /// </summary>
    public string OriginalTopic { get; set; } = string.Empty;

    /// <summary>
    /// The partition of the original message.
    /// </summary>
    public int Partition { get; set; }

    /// <summary>
    /// The offset of the original message.
    /// </summary>
    public long Offset { get; set; }

    /// <summary>
    /// Timestamp when the message failed processing.
    /// </summary>
    public DateTime FailedAt { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// The error message that caused the failure.
    /// </summary>
    public string ErrorMessage { get; set; } = string.Empty;

    /// <summary>
    /// The type or category of the error (e.g., "DeserializationError", "ValidationError").
    /// </summary>
    public string ErrorType { get; set; } = string.Empty;

    /// <summary>
    /// Stack trace of the exception (optional, can be large).
    /// </summary>
    public string? StackTrace { get; set; }

    /// <summary>
    /// Number of times this message has been retried.
    /// </summary>
    public int RetryCount { get; set; }

    /// <summary>
    /// Severity level for monitoring/alerting (e.g., "error", "warning", "critical").
    /// </summary>
    public string Severity { get; set; } = "error";

    /// <summary>
    /// The original message key (if available).
    /// </summary>
    public string? OriginalKey { get; set; }

    /// <summary>
    /// The original message value as base64 encoded string (for JSON compatibility).
    /// </summary>
    public string? OriginalValueBase64 { get; set; }

    /// <summary>
    /// Headers from the original message.
    /// </summary>
    public Dictionary<string, string> Headers { get; set; } = new();

    /// <summary>
    /// Additional metadata for debugging and tracing.
    /// </summary>
    public Dictionary<string, string> Metadata { get; set; } = new();

    /// <summary>
    /// Application or service name that encountered the error.
    /// </summary>
    public string? ApplicationName { get; set; }

    /// <summary>
    /// Hostname or pod name where the error occurred.
    /// </summary>
    public string? Hostname { get; set; }
}
