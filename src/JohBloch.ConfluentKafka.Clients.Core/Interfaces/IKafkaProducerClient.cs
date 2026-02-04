namespace JohBloch.ConfluentKafka.Clients.Interfaces
{
    /// <summary>
    /// Produces messages to Kafka topics with support for single and batch sends.
    /// </summary>
    public interface IKafkaProducerClient : IDisposable
    {
        /// <summary>
        /// Produces a single message to the configured topic for the specified producer key.
        /// Convenience alias for <see cref="SendMessageAsync{T}(T,string,string,Confluent.Kafka.Headers?,Confluent.Kafka.ISerializer{T}?,System.Threading.CancellationToken)"/>.
        /// </summary>
        /// <typeparam name="T">Type of the message value.</typeparam>
        /// <param name="message">Message payload to send.</param>
        /// <param name="key">Partitioning key for the message.</param>
        /// <param name="producerKey">Logical producer configuration key.</param>
        /// <param name="headers">Optional Kafka headers.</param>
        /// <param name="serializer">Optional serializer for the value type.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Result with delivery metadata and success flag.</returns>
        Task<KafkaResult> ProduceAsync<T>(T message, string key, string producerKey, Headers? headers, ISerializer<T>? serializer, CancellationToken ct);

        /// <summary>
        /// Produces a batch of messages to Kafka using batch-optimized producer settings.
        /// Convenience alias for <see cref="SendBatchAsync{T}(System.Collections.Generic.IEnumerable{T},System.Func{T,string},string,Confluent.Kafka.Headers?,Confluent.Kafka.ISerializer{T}?,System.Threading.CancellationToken)"/>.
        /// Accepts arrays and lists via <see cref="IEnumerable{T}"/>.
        /// </summary>
        /// <typeparam name="T">Type of the message value.</typeparam>
        /// <param name="messages">Collection of messages to send.</param>
        /// <param name="keySelector">Function selecting the key for each message.</param>
        /// <param name="producerKey">Logical producer configuration key.</param>
        /// <param name="headers">Optional Kafka headers applied to all messages.</param>
        /// <param name="serializer">Optional serializer for the value type.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Batch result with per-message outcomes.</returns>
        Task<BatchResult> ProduceAsync<T>(IEnumerable<T> messages, Func<T, string> keySelector, string producerKey, Headers? headers, ISerializer<T>? serializer, CancellationToken ct);

        /// <summary>
        /// Sends a single message to the configured topic for the specified producer key.
        /// </summary>
        /// <typeparam name="T">Type of the message value.</typeparam>
        /// <param name="message">Message payload to send.</param>
        /// <param name="key">Partitioning key for the message.</param>
        /// <param name="producerKey">Logical producer configuration key.</param>
        /// <param name="headers">Optional Kafka headers.</param>
        /// <param name="serializer">Optional serializer for the value type.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Result with delivery metadata and success flag.</returns>
        Task<KafkaResult> SendMessageAsync<T>(T message, string key, string producerKey, Headers? headers = null, ISerializer<T>? serializer = null, CancellationToken ct = default);

        /// <summary>
        /// Sends a single message to Kafka using a specific schema type.
        /// </summary>
        /// <typeparam name="T">Type of the message value.</typeparam>
        /// <param name="message">Message payload to send.</param>
        /// <param name="key">Partitioning key for the message.</param>
        /// <param name="producerKey">Logical producer configuration key.</param>
        /// <param name="schemaType">The schema type to use for serialization.</param>
        /// <param name="headers">Optional Kafka headers.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Result with delivery metadata and success flag.</returns>
        Task<KafkaResult> SendMessageWithSchemaAsync<T>(T message, string key, string producerKey, Models.SchemaType schemaType, Headers? headers = null, CancellationToken ct = default);

        /// <summary>
        /// Sends a batch of messages to Kafka using batch-optimized producer settings.
        /// </summary>
        /// <typeparam name="T">Type of the message value.</typeparam>
        /// <param name="messages">Collection of messages to send.</param>
        /// <param name="keySelector">Function selecting the key for each message.</param>
        /// <param name="producerKey">Logical producer configuration key.</param>
        /// <param name="headers">Optional Kafka headers applied to all messages.</param>
        /// <param name="serializer">Optional serializer for the value type.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Batch result with per-message outcomes.</returns>
        Task<BatchResult> SendBatchAsync<T>(IEnumerable<T> messages, Func<T, string> keySelector, string producerKey, Headers? headers = null, ISerializer<T>? serializer = null, CancellationToken ct = default);

        /// <summary>
        /// Sends a dead letter message to the dead letter queue using the default producer configuration.
        /// Uses the configured DLQ topic pattern (default: "dlq-{topic}").
        /// </summary>
        /// <param name="dlqMessage">Dead letter queue message with error details.</param>
        /// <returns>Result with delivery metadata and success flag.</returns>
        Task<KafkaResult> SendToDeadLetterQueueAsync(Models.DeadLetterMessage dlqMessage);

        /// <inheritdoc cref="SendToDeadLetterQueueAsync(JohBloch.ConfluentKafka.Clients.Models.DeadLetterMessage)" />
        Task<KafkaResult> SendToDeadLetterQueueAsync(Models.DeadLetterMessage dlqMessage, CancellationToken ct);

        /// <inheritdoc cref="SendToDeadLetterQueueAsync(JohBloch.ConfluentKafka.Clients.Models.DeadLetterMessage)" />
        Task<KafkaResult> SendToDeadLetterQueueAsync(Models.DeadLetterMessage dlqMessage, string producerKey);

        /// <inheritdoc cref="SendToDeadLetterQueueAsync(JohBloch.ConfluentKafka.Clients.Models.DeadLetterMessage)" />
        Task<KafkaResult> SendToDeadLetterQueueAsync(Models.DeadLetterMessage dlqMessage, string producerKey, CancellationToken ct);

        /// <inheritdoc cref="SendToDeadLetterQueueAsync(JohBloch.ConfluentKafka.Clients.Models.DeadLetterMessage)" />
        Task<KafkaResult> SendToDeadLetterQueueAsync(Models.DeadLetterMessage dlqMessage, string? key, string producerKey, CancellationToken ct);

        /// <summary>
        /// Sends a failed message to the dead letter queue, automatically creating the DLQ message from a consume result and exception.
        /// Uses the configured DLQ topic pattern (default: "dlq-{topic}").
        /// </summary>
        /// <typeparam name="TKey">Type of the message key.</typeparam>
        /// <typeparam name="TValue">Type of the message value.</typeparam>
        /// <param name="originalMessage">The original consumed message that failed.</param>
        /// <param name="exception">The exception that caused the failure.</param>
        /// <returns>Result with delivery metadata and success flag.</returns>
        Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(ConsumeResult<TKey, TValue> originalMessage, Exception exception);

        /// <inheritdoc cref="SendToDeadLetterQueueAsync{TKey,TValue}(Confluent.Kafka.ConsumeResult{TKey,TValue},System.Exception)" />
        Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(ConsumeResult<TKey, TValue> originalMessage, Exception exception, CancellationToken ct);

        /// <inheritdoc cref="SendToDeadLetterQueueAsync{TKey,TValue}(Confluent.Kafka.ConsumeResult{TKey,TValue},System.Exception)" />
        Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(ConsumeResult<TKey, TValue> originalMessage, Exception exception, int retryCount);

        /// <inheritdoc cref="SendToDeadLetterQueueAsync{TKey,TValue}(Confluent.Kafka.ConsumeResult{TKey,TValue},System.Exception)" />
        Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(ConsumeResult<TKey, TValue> originalMessage, Exception exception, int retryCount, CancellationToken ct);

        /// <summary>
        /// Sends a failed message to the dead letter queue, allowing full control over retry metadata and target producer configuration.
        /// </summary>
        /// <typeparam name="TKey">Type of the message key.</typeparam>
        /// <typeparam name="TValue">Type of the message value.</typeparam>
        /// <param name="originalMessage">The original consumed message that failed.</param>
        /// <param name="exception">The exception that caused the failure.</param>
        /// <param name="retryCount">Number of times this message has been retried.</param>
        /// <param name="producerKey">Logical producer configuration key.</param>
        /// <param name="additionalMetadata">Optional additional metadata to include in the DLQ message.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>Result with delivery metadata and success flag.</returns>
        Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(
            ConsumeResult<TKey, TValue> originalMessage,
            Exception exception,
            int retryCount,
            string producerKey,
            Dictionary<string, string>? additionalMetadata,
            CancellationToken ct);
    }
}
