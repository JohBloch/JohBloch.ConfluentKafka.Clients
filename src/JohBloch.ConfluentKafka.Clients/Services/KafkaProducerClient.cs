using JohBloch.ConfluentKafka.Clients.Services.Serialization;

namespace JohBloch.ConfluentKafka.Clients.Services
{
    /// <summary>
    /// Kafka producer client supporting single and batch message production with OAuth bearer authentication.
    /// </summary>
    public class KafkaProducerClient : IKafkaProducerClient, IDisposable
    {
        private readonly ILogger<KafkaProducerClient> _logger;
        private readonly ISecurityTokenProvider _security;
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly SerializerFactory _serializerFactory;
        private readonly ConcurrentDictionary<(string ProducerKey, Type Type, bool Batch), object> _producers = new();
        private readonly Dictionary<string, KafkaProducerOptions> _producerOptions;
        // New: optional passthrough config dictionaries
        private readonly IDictionary<string, string>? _globalConfig;
        private readonly IDictionary<string, IDictionary<string, string>>? _perProducerConfigs;

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaProducerClient"/>.
        /// </summary>
        /// <param name="producerOptions">Producer options keyed by logical producer name.</param>
        /// <param name="securityTokenProvider">Provider for OAuth bearer tokens and SASL settings.</param>
        /// <param name="schemaRegistryFactory">Factory to create Schema Registry clients.</param>
        /// <param name="logger">Logger instance.</param>
        /// <param name="globalConfig">Optional global librdkafka key/values to apply when set.</param>
        /// <param name="perProducerConfigs">Optional per-producer librdkafka overrides (by producer key).</param>
        public KafkaProducerClient(
            IDictionary<string, KafkaProducerOptions> producerOptions,
            ISecurityTokenProvider securityTokenProvider,
            ISchemaRegistryFactory schemaRegistryFactory,
            ILogger<KafkaProducerClient> logger,
            IDictionary<string, string>? globalConfig = null,
            IDictionary<string, IDictionary<string, string>>? perProducerConfigs = null)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _producerOptions = new Dictionary<string, KafkaProducerOptions>(producerOptions);
            _security = securityTokenProvider ?? throw new ArgumentNullException(nameof(securityTokenProvider));
            _schemaRegistry = schemaRegistryFactory.CreateClient();
            _globalConfig = globalConfig;
            _perProducerConfigs = perProducerConfigs;
            
            // Initialize SerializerFactory
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            _serializerFactory = new SerializerFactory(_schemaRegistry, loggerFactory);
        }

        private IProducer<string, TValue> CreateProducer<TValue>(string producerKey, bool batchOptimized, ISerializer<TValue>? serializer = null)
        {
            var cfg = BuildConfig(producerKey, batchOptimized);
            var builder = new ProducerBuilder<string, TValue>(cfg)
                .SetOAuthBearerTokenRefreshHandler(async (client, _) =>
                {
                    try
                    {
                        var token = await _security.GetAccessTokenAsync(CancellationToken.None);
                        var extensions = _security.GetExtensions();
                        var lifetimeMs = (long)Math.Max(1, (token.ExpiresOn - DateTimeOffset.UtcNow).TotalMilliseconds);
                        client.OAuthBearerSetToken(token.AccessTokenValue, lifetimeMs, null, extensions);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "OAuth token refresh failed");
                        client.OAuthBearerSetTokenFailure(ex.Message);
                    }
                })
                .SetLogHandler((_, log) => _logger.LogInformation("Kafka: {msg}", log.Message))
                .SetErrorHandler((_, err) => _logger.LogError("Kafka error: {err}", err.Reason));

            // Use provided serializer if any, otherwise default to Chr.Avro Async schema serializer
            if (serializer is not null)
            {
                builder.SetValueSerializer(serializer);
            }
            else
            {
                builder.SetValueSerializer(new AsyncSchemaRegistrySerializer<TValue>(_schemaRegistry).AsSyncOverAsync());
            }

            return builder.Build();
        }

        private ProducerConfig BuildConfig(string producerKey, bool batchOptimized)
        {
            var producerOpts = _producerOptions[producerKey];
            var saslCfg = _security.GetKafkaSaslConfig();
            var config = KafkaConfigHelper.CreateBaseConfig(producerOpts, saslCfg);

            // Apply optional global configs (only if value is non-empty)
            if (_globalConfig is not null)
            {
                foreach (var kvp in _globalConfig)
                {
                    if (!string.IsNullOrWhiteSpace(kvp.Value))
                    {
                        config.Set(kvp.Key, kvp.Value);
                    }
                }
            }
            // Apply optional per-producer overrides
            if (_perProducerConfigs is not null && _perProducerConfigs.TryGetValue(producerKey, out var overrides) && overrides is not null)
            {
                foreach (var kvp in overrides)
                {
                    if (!string.IsNullOrWhiteSpace(kvp.Value))
                    {
                        config.Set(kvp.Key, kvp.Value);
                    }
                }
            }

            if (batchOptimized)
            {
                KafkaConfigHelper.ApplyBatchOptimizedSettings(config, producerOpts);
            }
            return config;
        }

        /// <summary>
        /// Helper methods to build producer configuration.
        /// </summary>
        public static class KafkaConfigHelper
        {
            /// <summary>
            /// Creates a base <see cref="ProducerConfig"/> using application options and optional SASL settings.
            /// </summary>
            /// <param name="producerOpts">Producer options.</param>
            /// <param name="saslCfg">Optional SASL configuration key/values.</param>
            /// <returns>Configured <see cref="ProducerConfig"/>.</returns>
            public static ProducerConfig CreateBaseConfig(KafkaProducerOptions producerOpts, IDictionary<string, string>? saslCfg)
            {
                var cfg = new ProducerConfig
                {
                    BootstrapServers = producerOpts.BootstrapServers,
                    SecurityProtocol = SecurityProtocol.SaslSsl,
                    SaslMechanism = SaslMechanism.OAuthBearer,
                    ClientId = producerOpts.ApplicationId
                };

                // Apply SASL settings provided by the security provider, if any
                if (saslCfg != null)
                {
                    foreach (var kvp in saslCfg)
                    {
                        cfg.Set(kvp.Key, kvp.Value);
                    }
                }

                return cfg;
            }

            /// <summary>
            /// Applies batch-optimized settings to the producer configuration.
            /// </summary>
            /// <param name="config">Producer configuration.</param>
            /// <param name="producerOpts">Producer options.</param>
            public static void ApplyBatchOptimizedSettings(ProducerConfig config, KafkaProducerOptions producerOpts)
            {
                config.BatchSize = producerOpts.BatchSizeKB * 1024;
                config.LingerMs = 100;
                config.QueueBufferingMaxMessages = producerOpts.QueueBufferMaxMessages;
                config.CompressionType = ParseCompressionType(producerOpts.CompressionType);
                config.EnableIdempotence = true;
                config.Acks = Acks.All;
                config.MessageSendMaxRetries = 3;
                config.RequestTimeoutMs = 10000;
                config.MessageTimeoutMs = 30000;
            }
        }

        private static CompressionType ParseCompressionType(string compressionType)
        {
            return compressionType?.ToLowerInvariant() switch
            {
                "none" => CompressionType.None,
                "gzip" => CompressionType.Gzip,
                "snappy" => CompressionType.Snappy,
                "lz4" => CompressionType.Lz4,
                "zstd" => CompressionType.Zstd,
                _ => CompressionType.None
            };
        }

        private int ParseInt(string value)
        {
            return int.TryParse(value, out var result) ? result : throw new InvalidOperationException($"Invalid integer value: {value}");
        }

        private TEnum ParseEnum<TEnum>(string value) where TEnum : struct
        {
            return Enum.TryParse(value, out TEnum result) ? result : throw new InvalidOperationException($"Invalid enum value: {value}");
        }

        private IProducer<string, T> GetProducer<T>(string producerKey, bool batchOptimized, ISerializer<T>? serializer = null)
        {
            // Include serializer type in cache key to avoid mixing different serializers for same T
            var serializerKey = serializer?.GetType();
            return (IProducer<string, T>)_producers.GetOrAdd((producerKey + (serializerKey?.FullName ?? ""), typeof(T), batchOptimized), _ =>
            {
                var producer = CreateProducer<T>(producerKey, batchOptimized, serializer);
                _logger.LogInformation("Created Kafka producer for {Type} (batch={Batch}, key={ProducerKey}, serializer={Serializer})", typeof(T).Name, batchOptimized, producerKey, serializerKey?.Name ?? "default");
                return producer;
            });
        }

        /// <summary>
        /// Sends a single message to Kafka.
        /// </summary>
        /// <typeparam name="T">The type of the message value.</typeparam>
        /// <param name="message">The message to send.</param>
        /// <param name="key">The key for the message.</param>
        /// <param name="producerKey">The producer key.</param>
        /// <param name="serializer">Optional serializer for the message value.</param>
        /// <param name="headers">Optional headers for the message.</param>
        /// <param name="ct">A cancellation token.</param>
        /// <returns>A <see cref="KafkaResult"/> indicating the result of the operation.</returns>
        public async Task<KafkaResult> SendMessageAsync<T>(
            T message,
            string key,
            string producerKey,
            Headers? headers = null,
            ISerializer<T>? serializer = null,
            CancellationToken ct = default)
        {
            var producer = GetProducer<T>(producerKey, batchOptimized: false, serializer: serializer);
            var topic = _producerOptions[producerKey].Topic;
            return await ProduceMessageAsync(producer, message, key, topic, headers, ct);
        }

        /// <summary>
        /// Sends a single message to Kafka using a specific schema type.
        /// </summary>
        /// <typeparam name="T">The type of the message value.</typeparam>
        /// <param name="message">The message to send.</param>
        /// <param name="key">The key for the message.</param>
        /// <param name="producerKey">The producer key.</param>
        /// <param name="schemaType">The schema type to use for serialization.</param>
        /// <param name="headers">Optional headers for the message.</param>
        /// <param name="ct">A cancellation token.</param>
        /// <returns>A <see cref="KafkaResult"/> indicating the result of the operation.</returns>
        public async Task<KafkaResult> SendMessageWithSchemaAsync<T>(
            T message,
            string key,
            string producerKey,
            Models.SchemaType schemaType,
            Headers? headers = null,
            CancellationToken ct = default)
        {
            var serializer = _serializerFactory.Create<T>(schemaType);
            var wrappedSerializer = new AsyncSerializerWrapper<T>(serializer);
            return await SendMessageAsync(message, key, producerKey, headers, wrappedSerializer, ct);
        }

        private async Task<KafkaResult> ProduceMessageAsync<T>(
            IProducer<string, T> producer,
            T message,
            string key,
            string topic,
            Headers? headers,
            CancellationToken ct)
        {
            try
            {
                var deliveryResult = await producer.ProduceAsync(
                    topic,
                    new Message<string, T>
                    {
                        Key = key,
                        Value = message,
                        Headers = headers
                    }, ct);

                return new KafkaResult(true, deliveryResult.Topic, deliveryResult.Partition.Value, deliveryResult.Offset.Value, key);
            }
            catch (ProduceException<string, T> ex)
            {
                _logger.LogError(ex, "Produce failed: {reason}", ex.Error.Reason);
                return new KafkaResult(false, errorMessage: ex.Error.Reason);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SendMessageAsync failed");
                return new KafkaResult(false, errorMessage: ex.Message);
            }
        }

        /// <summary>
        /// Sends a batch of messages to Kafka.
        /// </summary>
        /// <typeparam name="T">The type of the message.</typeparam>
        /// <param name="messages">The messages to send.</param>
        /// <param name="keySelector">A function to select the key for each message.</param>
        /// <param name="producerKey">The producer key.</param>
        /// <param name="serializer">Optional serializer for the message value.</param>
        /// <param name="headers">Optional headers for the messages.</param>
        /// <param name="ct">A cancellation token.</param>
        /// <returns>A <see cref="BatchResult"/> indicating the result of the operation.</returns>
        public async Task<BatchResult> SendBatchAsync<T>(
            IEnumerable<T> messages,
            Func<T, string> keySelector,
            string producerKey,
            Headers? headers = null,
            ISerializer<T>? serializer = null,
            CancellationToken ct = default)
        {
            var batchId = Guid.NewGuid().ToString();
            using var scope = _logger.BeginScope(new { BatchId = batchId });

            var result = new BatchResult(messages.Count());
            if (!messages.Any()) return result.SucceedEmpty();

            var producer = GetProducer<T>(producerKey, batchOptimized: true, serializer: serializer);
            var topic = _producerOptions[producerKey].Topic;

            using var kafkaCts = CancellationTokenSource.CreateLinkedTokenSource(CancellationToken.None);
            kafkaCts.CancelAfter(TimeSpan.FromSeconds(60));
            var produceToken = kafkaCts.Token;

            _logger.LogInformation("Starting batch {batchId} with {count} messages (timeout 60s)", batchId, result.TotalMessages);

            var tasks = PrepareBatchTasks(producer, messages, keySelector, headers, batchId, topic, produceToken);

            await ProcessBatchTasks(tasks, result, batchId, produceToken);

            try
            {
                producer.Flush(TimeSpan.FromSeconds(5));
            }
            catch (Exception flushEx)
            {
                _logger.LogWarning(flushEx, "Flush issue for batch {batchId}", batchId);
            }

            _logger.LogInformation("Batch {id} completed: {succ}/{total} (failures: {fail})", batchId, result.SuccessCount, result.TotalMessages, result.FailureCount);
            return result;
        }

        private IEnumerable<Task<DeliveryResult<string, T>>> PrepareBatchTasks<T>(
            IProducer<string, T> producer,
            IEnumerable<T> messages,
            Func<T, string> keySelector,
            Headers? headers,
            string batchId,
            string topic,
            CancellationToken ct)
        {
            var hdr = headers ?? new Headers();
            hdr.Add("batch-id", Encoding.UTF8.GetBytes(batchId));

            int index = 0;
            return messages.Select(m =>
            {
                var key = keySelector(m);
                _logger.LogDebug("Queue produce (batch {batchId}) msgIndex={index} key={key}", batchId, index++, key);
                return producer.ProduceAsync(topic,
                    new Message<string, T>
                    {
                        Key = key,
                        Value = m,
                        Headers = hdr
                    }, ct);
            });
        }

        private async Task ProcessBatchTasks<T>(
            IEnumerable<Task<DeliveryResult<string, T>>> tasks,
            BatchResult result,
            string batchId,
            CancellationToken ct)
        {
            int i = 0;
            foreach (var task in tasks)
            {
                try
                {
                    var deliveryResult = await task;
                    result.AddSuccess(deliveryResult.Topic, deliveryResult.Partition.Value, deliveryResult.Offset.Value, deliveryResult.Key);
                    _logger.LogDebug("Delivered (batch {batchId}) msgIndex={index} key={key} part={part} offset={offset}", batchId, i, deliveryResult.Key, deliveryResult.Partition.Value, deliveryResult.Offset.Value);
                }
                catch (TaskCanceledException tce)
                {
                    _logger.LogWarning(tce, "Produce canceled (batch {batchId}) msgIndex={index} tokenCanceled={tokenCanceled}", batchId, i, ct.IsCancellationRequested);
                    result.AddFailure("canceled");
                }
                catch (ProduceException<string, T> pex)
                {
                    _logger.LogError(pex, "Produce failed in batch {id}: {reason}", batchId, pex.Error.Reason);
                    result.AddFailure(pex.Error.Reason);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Produce failed in batch {id}: {reason}", batchId, ex.Message);
                    result.AddFailure(ex.Message);
                }
                i++;
            }
        }

        /// <summary>
        /// Creates or retrieves a DLQ producer for the specified producer key and DLQ topic.
        /// </summary>
        private string GetOrCreateDlqProducerKey(string producerKey, string dlqTopic)
        {
            var dlqProducerKey = $"_dlq_{producerKey}";
            if (!_producerOptions.ContainsKey(dlqProducerKey))
            {
                var options = _producerOptions[producerKey];
                var dlqOptions = new KafkaProducerOptions
                {
                    BootstrapServers = options.BootstrapServers,
                    Topic = dlqTopic,
                    ApplicationId = options.ApplicationId,
                    DeadLetterQueueTopicPattern = options.DeadLetterQueueTopicPattern,
                    IncludeStackTraceInDlq = options.IncludeStackTraceInDlq
                };
                _producerOptions[dlqProducerKey] = dlqOptions;
            }
            return dlqProducerKey;
        }

        /// <summary>
        /// Extracts headers from a Kafka message into a dictionary.
        /// </summary>
        private Dictionary<string, string> ExtractHeaders(Headers? headers)
        {
            var result = new Dictionary<string, string>();
            if (headers == null) return result;

            foreach (var header in headers)
            {
                try
                {
                    var headerValue = Encoding.UTF8.GetString(header.GetValueBytes());
                    result[header.Key] = headerValue;
                }
                catch
                {
                    result[header.Key] = Convert.ToBase64String(header.GetValueBytes());
                }
            }
            return result;
        }

        /// <summary>
        /// Serializes a message value to base64 for storage in DLQ.
        /// </summary>
        private string? SerializeValueToBase64<TValue>(TValue? value)
        {
            if (value == null) return null;

            try
            {
                var valueBytes = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(value);
                return Convert.ToBase64String(valueBytes);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to serialize original message value for DLQ");
                return Convert.ToBase64String(Encoding.UTF8.GetBytes(value.ToString() ?? ""));
            }
        }

        /// <summary>
        /// Disposes producers and the schema registry client, flushing outstanding messages.
        /// </summary>
        public void Dispose()
        {
            foreach (var p in _producers.Values)
            {
                try
                {
                    dynamic prod = p;
                    // Flush outstanding messages before disposing
                    prod.Flush(TimeSpan.FromSeconds(5));
                    ((IDisposable)prod).Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Error disposing Kafka producer instance");
                }
            }
            _schemaRegistry?.Dispose();
            _logger.LogInformation("KafkaProducerClient disposed");
        }

        /// <summary>
        /// Sends a failed message to the dead letter queue with JSON schema.
        /// Uses the configured DLQ topic pattern (default: "dlq-{topic}").
        /// </summary>
        public async Task<KafkaResult> SendToDeadLetterQueueAsync(
            Models.DeadLetterMessage dlqMessage,
            string? key = null,
            string producerKey = "default",
            CancellationToken ct = default)
        {
            if (dlqMessage == null) throw new ArgumentNullException(nameof(dlqMessage));
            if (!_producerOptions.TryGetValue(producerKey, out var options))
                throw new ArgumentException($"Producer key '{producerKey}' not found in configuration", nameof(producerKey));

            // Build DLQ topic name from pattern
            var dlqTopic = options.DeadLetterQueueTopicPattern.Replace("{topic}", dlqMessage.OriginalTopic);

            // Enrich DLQ message with hostname if not set
            if (string.IsNullOrEmpty(dlqMessage.Hostname))
            {
                dlqMessage.Hostname = Environment.MachineName;
            }

            // Use original key if not specified
            var messageKey = key ?? dlqMessage.OriginalKey ?? dlqMessage.OriginalTopic;

            _logger.LogWarning("Sending message to DLQ: Topic={DlqTopic}, OriginalTopic={OriginalTopic}, Error={ErrorType}: {ErrorMessage}",
                dlqTopic, dlqMessage.OriginalTopic, dlqMessage.ErrorType, dlqMessage.ErrorMessage);

            // Create or retrieve DLQ producer
            var dlqProducerKey = GetOrCreateDlqProducerKey(producerKey, dlqTopic);

            // Send using JSON schema
            return await SendMessageWithSchemaAsync(
                message: dlqMessage,
                key: messageKey,
                producerKey: dlqProducerKey,
                schemaType: Models.SchemaType.Json,
                headers: null,
                ct: ct);
        }

        /// <summary>
        /// Sends a failed message to the dead letter queue, automatically creating the DLQ message from a consume result and exception.
        /// Uses the configured DLQ topic pattern (default: "dlq-{topic}").
        /// </summary>
        public async Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(
            ConsumeResult<TKey, TValue> originalMessage,
            Exception exception,
            int retryCount = 0,
            string producerKey = "default",
            Dictionary<string, string>? additionalMetadata = null,
            CancellationToken ct = default)
        {
            if (originalMessage == null) throw new ArgumentNullException(nameof(originalMessage));
            if (exception == null) throw new ArgumentNullException(nameof(exception));
            if (!_producerOptions.TryGetValue(producerKey, out var options))
                throw new ArgumentException($"Producer key '{producerKey}' not found in configuration", nameof(producerKey));

            // Build DLQ message
            var dlqMessage = new Models.DeadLetterMessage
            {
                OriginalTopic = originalMessage.Topic,
                Partition = originalMessage.Partition.Value,
                Offset = originalMessage.Offset.Value,
                FailedAt = DateTime.UtcNow,
                ErrorMessage = exception.Message,
                ErrorType = exception.GetType().Name,
                StackTrace = options.IncludeStackTraceInDlq ? exception.StackTrace : null,
                RetryCount = retryCount,
                OriginalKey = originalMessage.Message.Key?.ToString(),
                ApplicationName = options.ApplicationId,
                Hostname = Environment.MachineName,
                OriginalValueBase64 = SerializeValueToBase64(originalMessage.Message.Value),
                Headers = ExtractHeaders(originalMessage.Message.Headers)
            };

            // Add additional metadata
            if (additionalMetadata != null)
            {
                foreach (var kvp in additionalMetadata)
                {
                    dlqMessage.Metadata[kvp.Key] = kvp.Value;
                }
            }

            return await SendToDeadLetterQueueAsync(dlqMessage, key: null, producerKey: producerKey, ct: ct);
        }
    }

    /// <summary>
    /// Wrapper to adapt IMessageSerializer to Confluent's ISerializer interface.
    /// </summary>
    internal class AsyncSerializerWrapper<T> : ISerializer<T>
    {
        private readonly IMessageSerializer<T> _serializer;

        public AsyncSerializerWrapper(IMessageSerializer<T> serializer)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        }

        public byte[] Serialize(T data, SerializationContext context)
        {
            // Synchronous wrapper - blocks on async call
            return _serializer.SerializeAsync(data, context).GetAwaiter().GetResult();
        }
    }
}