namespace JohBloch.ConfluentKafka.Clients.Interfaces
{
    /// <summary>
    /// Generic Kafka consumer client interface supporting schema-aware and schema-less consumption.
    /// </summary>
    public interface IKafkaConsumerClient : IDisposable
    {
        /// <summary>
        /// Subscribes to the specified topics.
        /// </summary>
        /// <param name="topics">The topics to subscribe to.</param>
        void Subscribe(IEnumerable<string> topics);

        /// <summary>
        /// Consumes a single message from Kafka with generic type deserialization.
        /// </summary>
        /// <typeparam name="T">The type of the message value.</typeparam>
        /// <param name="ct">A cancellation token.</param>
        /// <returns>A <see cref="ConsumeResult{TKey, TValue}"/> containing the consumed message, or null if no message was consumed.</returns>
        Task<ConsumeResult<string, T>?> ConsumeAsync<T>(CancellationToken ct = default);

        /// <summary>
        /// Consumes a batch of messages from Kafka with generic type deserialization.
        /// </summary>
        /// <typeparam name="T">The type of the message value.</typeparam>
        /// <param name="maxMessages">Maximum number of messages to consume.</param>
        /// <param name="timeoutMs">Timeout in milliseconds for the batch operation.</param>
        /// <param name="ct">A cancellation token.</param>
        /// <returns>A list of consumed messages.</returns>
        Task<List<ConsumeResult<string, T>>> ConsumeBatchAsync<T>(int maxMessages, int timeoutMs = 5000, CancellationToken ct = default);

        /// <summary>
        /// Manually commits the current offset for all partitions.
        /// </summary>
        void Commit();

        /// <summary>
        /// Manually commits the offset for the specified message.
        /// </summary>
        /// <param name="result">The consume result to commit.</param>
        void Commit<T>(ConsumeResult<string, T> result);

        /// <summary>
        /// Unsubscribes from all currently subscribed topics.
        /// </summary>
        void Unsubscribe();

        /// <summary>
        /// Gets the consumer's current assignment (list of partitions assigned to this consumer).
        /// </summary>
        /// <returns>List of assigned topic partitions.</returns>
        List<TopicPartition> Assignment { get; }

        /// <summary>
        /// Gets the current consumer subscription (list of subscribed topics).
        /// </summary>
        /// <returns>List of subscribed topics.</returns>
        List<string> Subscription { get; }
    }
}
