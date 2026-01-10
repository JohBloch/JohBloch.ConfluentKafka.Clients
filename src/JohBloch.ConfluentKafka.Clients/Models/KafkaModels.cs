namespace JohBloch.ConfluentKafka.Clients.Models
{
    /// <summary>
    /// System health status information
    /// </summary>
    public class HealthStatus
    {
        /// <summary>
        /// Whether the system is healthy overall
        /// </summary>
        public bool IsHealthy { get; set; }

        /// <summary>
        /// Whether Kafka is connected and accessible
        /// </summary>
        public bool IsKafkaConnected { get; set; }

        /// <summary>
        /// Whether Schema Registry is connected and accessible
        /// </summary>
        public bool IsSchemaRegistryConnected { get; set; }

        /// <summary>
        /// Last error message encountered during health check
        /// </summary>
        public string? ErrorMessage { get; set; }

        /// <summary>
        /// Additional details about system health
        /// </summary>
        public Dictionary<string, string> Details { get; set; } = new();
    }

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

    // OPTIONS-CLASSES
    /// <summary>
    /// Root options for Kafka clients.
    /// </summary>
    public sealed class KafkaOptions
    {
        /// <summary>Producer options keyed by logical producer name.</summary>
        public Dictionary<string, KafkaProducerOptions> KafkaProducerOptions { get; set; } = new();
        /// <summary>Consumer options keyed by logical consumer name.</summary>
        public Dictionary<string, KafkaConsumerOptions> KafkaConsumerOptions { get; set; } = new();
        /// <summary>Schema Registry connectivity and auth options.</summary>
        public SchemaRegistryOptions SchemaRegistryOptions { get; set; } = new();
    }

    /// <summary>
    /// Configuration for Kafka producers.
    /// </summary>
    public sealed class KafkaProducerOptions
    {
        /// <summary>Bootstrap servers for Kafka cluster.</summary>
        public string BootstrapServers { get; set; } = string.Empty;
        /// <summary>Topic to produce to.</summary>
        public string Topic { get; set; } = string.Empty;
        /// <summary>Application/client identifier used by producers.</summary>
        public string ApplicationId { get; set; } = string.Empty;
        /// <summary>Dead letter queue topic naming pattern. Use {topic} placeholder for original topic name.</summary>
        public string DeadLetterQueueTopicPattern { get; set; } = "dlq-{topic}";
        /// <summary>Whether to include stack trace in DLQ messages (can be large).</summary>
        public bool IncludeStackTraceInDlq { get; set; } = false;
        /// <summary>Batch size in KB.</summary>
        public int BatchSizeKB { get; set; } = 32;
        /// <summary>Linger time in milliseconds.</summary>
        public int LingerMS { get; set; } = 100;
        /// <summary>Queue buffering max messages.</summary>
        public int QueueBufferMaxMessages { get; set; } = 50000;
        /// <summary>Compression type name (e.g., gzip, snappy).</summary>
        public string CompressionType { get; set; } = "none";
        /// <summary>Compression level if supported.</summary>
        public int CompressionLevel { get; set; } = 0;
    }

    /// <summary>
    /// Configuration for Kafka consumers.
    /// </summary>
    public sealed class KafkaConsumerOptions
    {
        /// <summary>Bootstrap servers for Kafka cluster.</summary>
        public string BootstrapServers { get; set; } = string.Empty;
        /// <summary>Consumer group id.</summary>
        public string GroupId { get; set; } = string.Empty;
        /// <summary>Topic to consume from.</summary>
        public string Topic { get; set; } = string.Empty;
        /// <summary>Enable auto commit of offsets.</summary>
        public bool EnableAutoCommit { get; set; } = true;
        /// <summary>Auto offset reset policy.</summary>
        public string AutoOffsetReset { get; set; } = "earliest";
        /// <summary>Session timeout in milliseconds.</summary>
        public int SessionTimeoutMs { get; set; } = 45000;
        /// <summary>Heartbeat interval in milliseconds.</summary>
        public int HeartbeatIntervalMs { get; set; } = 3000;
        /// <summary>Default schema type to use for deserialization when auto-detection is disabled.</summary>
        public SchemaType DefaultSchemaType { get; set; } = SchemaType.Avro;
        /// <summary>Whether to automatically detect schema type from Schema Registry.</summary>
        public bool AutoDetectSchemaType { get; set; } = true;
        /// <summary>Per-topic schema type overrides. Key is topic name, value is schema type.</summary>
        public Dictionary<string, SchemaType> TopicSchemaTypes { get; set; } = new();
    }

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

    /// <summary>
    /// Configuration for Confluent Schema Registry.
    /// </summary>
    public sealed class SchemaRegistryOptions
    {
        /// <summary>Schema Registry base URL.</summary>
        public string Url { get; set; } = string.Empty;
        /// <summary>OAuth client id.</summary>
        public string ClientId { get; set; } = string.Empty;
        /// <summary>OAuth client secret.</summary>
        public string ClientSecret { get; set; } = string.Empty;
        /// <summary>OAuth scope.</summary>
        public string Scope { get; set; } = string.Empty;
        /// <summary>Logical cluster id used as OAuth extension.</summary>
        public string LogicalCluster { get; set; } = string.Empty;
        /// <summary>OAuth token endpoint URL.</summary>
        public string TokenEndpointUrl { get; set; } = string.Empty;
        /// <summary>Identity pool id (optional).</summary>
        public string IdentityPoolId { get; set; } = string.Empty;
    }
}
