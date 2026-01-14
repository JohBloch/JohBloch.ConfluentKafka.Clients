using JohBloch.ConfluentKafka.Clients.Services.Serialization;
using System.ComponentModel;

namespace JohBloch.ConfluentKafka.Clients.Services
{
    /// <summary>
    /// Kafka producer client supporting single and batch message production with OAuth bearer authentication.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class KafkaProducerClient : IKafkaProducerClient, IDisposable
    {
        private readonly ILogger<KafkaProducerClient> _logger;
        private readonly ISecurityTokenProvider _security;
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly SerializerFactory _serializerFactory;
        private readonly ConcurrentDictionary<(string ProducerKey, Type Type, bool Batch), object> _producers = new();
        private readonly Dictionary<string, KafkaProducerOptions> _producerOptions;
        private int _disposed;
        // New: optional passthrough config dictionaries
        private readonly IDictionary<string, string>? _globalConfig;
        private readonly IDictionary<string, IDictionary<string, string>>? _perProducerConfigs;

        private void ThrowIfDisposed()
        {
            if (System.Threading.Volatile.Read(ref _disposed) != 0)
            {
                throw new ObjectDisposedException(nameof(KafkaProducerClient));
            }
        }

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
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(securityTokenProvider);
            ArgumentNullException.ThrowIfNull(schemaRegistryFactory);
            ArgumentNullException.ThrowIfNull(producerOptions);
            
            _logger = logger;
            _producerOptions = new Dictionary<string, KafkaProducerOptions>(producerOptions);
            _security = securityTokenProvider;
            _schemaRegistry = schemaRegistryFactory.CreateClient();
            _globalConfig = globalConfig;
            _perProducerConfigs = perProducerConfigs;
            
            // Initialize SerializerFactory
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            _serializerFactory = new SerializerFactory(_schemaRegistry, loggerFactory);

            ValidateAutoDlqConfiguration();
        }

        private void ValidateAutoDlqConfiguration()
        {
            foreach (var kvp in _producerOptions)
            {
                var producerKey = kvp.Key;
                var opts = kvp.Value;

                // Only validate user-defined producers; DLQ producers are derived.
                if (producerKey.StartsWith("_dlq_", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (!opts.AutoDlqOnDeliveryFailure)
                {
                    continue;
                }

                if (string.IsNullOrWhiteSpace(opts.Topic))
                {
                    throw new InvalidOperationException($"AutoDlqOnDeliveryFailure is enabled for producer '{producerKey}', but Topic is not configured.");
                }

                if (string.IsNullOrWhiteSpace(opts.DeadLetterQueueTopicPattern))
                {
                    throw new InvalidOperationException($"AutoDlqOnDeliveryFailure is enabled for producer '{producerKey}', but DeadLetterQueueTopicPattern is not configured.");
                }

                var dlqTopic = opts.DeadLetterQueueTopicPattern.Replace("{topic}", opts.Topic);
                if (string.IsNullOrWhiteSpace(dlqTopic))
                {
                    throw new InvalidOperationException($"AutoDlqOnDeliveryFailure is enabled for producer '{producerKey}', but DLQ topic resolved to an empty value.");
                }

                var dlqProducerKey = $"_dlq_{producerKey}";
                if (!_producerOptions.TryGetValue(dlqProducerKey, out var dlqOpts))
                {
                    throw new InvalidOperationException(
                        $"AutoDlqOnDeliveryFailure is enabled for producer '{producerKey}', but the DLQ producer '{dlqProducerKey}' is not configured in the producer options dictionary. " +
                        $"Add a producer entry with key '{dlqProducerKey}' and Topic='{dlqTopic}'.");
                }

                if (!string.Equals(dlqOpts.Topic, dlqTopic, StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"AutoDlqOnDeliveryFailure is enabled for producer '{producerKey}', but the configured DLQ producer '{dlqProducerKey}' has Topic='{dlqOpts.Topic}', expected '{dlqTopic}'.");
                }
            }
        }

        private IProducer<string, TValue> CreateProducer<TValue>(string producerKey, bool batchOptimized, ISerializer<TValue>? serializer = null)
        {
            var cfg = BuildConfig(producerKey, batchOptimized);
            var builder = new ProducerBuilder<string, TValue>(cfg);

            // Only attach OAuth refresh handler when OAuth is explicitly enabled.
            if (cfg.SaslMechanism == SaslMechanism.OAuthBearer)
            {
                builder.SetOAuthBearerTokenRefreshHandler(async (client, _) =>
                {
                    try
                    {
                        var token = await _security.GetAccessTokenAsync(CancellationToken.None);
                        var extensions = _security.GetExtensions();
                        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        var expMs = token.ExpiresOn.ToUnixTimeMilliseconds();
                        if (expMs <= nowMs)
                        {
                            throw new InvalidOperationException(
                                $"OAuth token is already expired (nowMs={nowMs}, expMs={expMs}).");
                        }

                        // librdkafka expects an absolute expiry timestamp (ms since epoch).
                        client.OAuthBearerSetToken(token.AccessTokenValue, expMs, null, extensions);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "OAuth token refresh failed");
                        client.OAuthBearerSetTokenFailure(ex.Message);
                    }
                });
            }

            builder
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

            // Apply optional global configs and per-producer overrides
            KafkaConfigHelper.ApplyConfigDictionary(config, _globalConfig);
            
            if (_perProducerConfigs?.TryGetValue(producerKey, out var overrides) == true)
            {
                KafkaConfigHelper.ApplyConfigDictionary(config, overrides);
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
                    // Default to plaintext to allow local testing without requiring SASL/OAuth.
                    SecurityProtocol = SecurityProtocol.Plaintext,
                    ClientId = producerOpts.ApplicationId
                };

                // If SASL is configured, switch protocol and set mechanism when applicable.
                if (saslCfg is not null && saslCfg.Count > 0)
                {
                    if (saslCfg.TryGetValue("sasl.mechanism", out var mech) &&
                        mech.Equals("oauthbearer", StringComparison.OrdinalIgnoreCase))
                    {
                        cfg.SecurityProtocol = SecurityProtocol.SaslSsl;
                        cfg.SaslMechanism = SaslMechanism.OAuthBearer;
                    }
                    else if (saslCfg.Keys.Any(k => k.StartsWith("sasl.", StringComparison.OrdinalIgnoreCase)))
                    {
                        // SASL is being used (mechanism might be set via config dictionary)
                        cfg.SecurityProtocol = SecurityProtocol.SaslSsl;
                    }
                }

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
            /// Applies configuration dictionary to a client config, skipping null or whitespace values.
            /// </summary>
            /// <param name="config">The client configuration to apply settings to.</param>
            /// <param name="configDictionary">Dictionary of librdkafka configuration key-value pairs.</param>
            public static void ApplyConfigDictionary(ClientConfig config, IDictionary<string, string>? configDictionary)
            {
                if (configDictionary is null) return;

                foreach (var kvp in configDictionary)
                {
                    if (!string.IsNullOrWhiteSpace(kvp.Value))
                    {
                        config.Set(kvp.Key, kvp.Value);
                    }
                }
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
            ThrowIfDisposed();
            var producer = GetProducer<T>(producerKey, batchOptimized: false, serializer: serializer);
            var topic = _producerOptions[producerKey].Topic;
            return await ProduceMessageAsync(producer, message, key, topic, headers, producerKey, ct);
        }

        Task<KafkaResult> IKafkaProducerClient.ProduceAsync<T>(
            T message,
            string key,
            string producerKey,
            Headers? headers,
            ISerializer<T>? serializer,
            CancellationToken ct)
            => SendMessageAsync(message, key, producerKey, headers, serializer, ct);

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
            ThrowIfDisposed();
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
            string producerKey,
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
                var result = new KafkaResult(false, topic: topic, key: key, errorMessage: ex.Error.Reason);
                await ApplyAutoDlqOnDeliveryFailureAsync(result, message, key, topic, headers, producerKey, ex, ct);
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "SendMessageAsync failed");
                var result = new KafkaResult(false, topic: topic, key: key, errorMessage: ex.Message);
                await ApplyAutoDlqOnDeliveryFailureAsync(result, message, key, topic, headers, producerKey, ex, ct);
                return result;
            }
        }

        private async Task ApplyAutoDlqOnDeliveryFailureAsync<T>(
            KafkaResult result,
            T originalMessage,
            string key,
            string originalTopic,
            Headers? headers,
            string producerKey,
            Exception exception,
            CancellationToken ct)
        {
            if (result is null)
            {
                throw new ArgumentNullException(nameof(result));
            }

            if (!_producerOptions.TryGetValue(producerKey, out var options))
            {
                return;
            }

            if (!options.AutoDlqOnDeliveryFailure)
            {
                return;
            }

            result.DlqAttempted = true;

            var errorMessage = exception is ProduceException<string, T> pex ? pex.Error.Reason : exception.Message;

            var dlqMessage = new Models.DeadLetterMessage
            {
                OriginalTopic = originalTopic,
                Partition = -1,
                Offset = -1,
                FailedAt = DateTime.UtcNow,
                ErrorMessage = errorMessage,
                ErrorType = exception.GetType().Name,
                StackTrace = options.IncludeStackTraceInDlq ? exception.StackTrace : null,
                RetryCount = 0,
                OriginalKey = key,
                ApplicationName = options.ApplicationId,
                Hostname = Environment.MachineName,
                OriginalValueBase64 = SerializeValueToBase64(originalMessage),
                Headers = ExtractHeaders(headers)
            };

            try
            {
                var dlqResult = await SendToConfiguredDeadLetterQueueAsync(dlqMessage, key: key, producerKey: producerKey, ct: ct);
                result.DlqSuccess = dlqResult.Success;
                result.DlqTopic = dlqResult.Topic;
                result.DlqPartition = dlqResult.Partition;
                result.DlqOffset = dlqResult.Offset;
                if (!dlqResult.Success)
                {
                    result.DlqErrorMessage = dlqResult.ErrorMessage;
                }
            }
            catch (Exception dlqEx)
            {
                _logger.LogError(dlqEx, "Auto-DLQ failed (producerKey={ProducerKey}, originalTopic={Topic})", producerKey, originalTopic);
                result.DlqSuccess = false;
                result.DlqErrorMessage = dlqEx.Message;
            }
        }

        private async Task<KafkaResult> SendToConfiguredDeadLetterQueueAsync(
            Models.DeadLetterMessage dlqMessage,
            string? key,
            string producerKey,
            CancellationToken ct)
        {
            if (dlqMessage == null) throw new ArgumentNullException(nameof(dlqMessage));
            if (!_producerOptions.TryGetValue(producerKey, out var options))
                throw new ArgumentException($"Producer key '{producerKey}' not found in configuration", nameof(producerKey));

            var dlqTopic = options.DeadLetterQueueTopicPattern.Replace("{topic}", dlqMessage.OriginalTopic);
            if (string.IsNullOrWhiteSpace(dlqTopic))
            {
                throw new InvalidOperationException($"DLQ topic resolved to empty for producer '{producerKey}'.");
            }

            if (string.IsNullOrEmpty(dlqMessage.Hostname))
            {
                dlqMessage.Hostname = Environment.MachineName;
            }

            var messageKey = key ?? dlqMessage.OriginalKey ?? dlqMessage.OriginalTopic;

            var dlqProducerKey = $"_dlq_{producerKey}";
            if (!_producerOptions.TryGetValue(dlqProducerKey, out var dlqOpts))
            {
                throw new InvalidOperationException(
                    $"AutoDlqOnDeliveryFailure requires DLQ producer '{dlqProducerKey}' to be configured (missing). Expected Topic='{dlqTopic}'.");
            }

            if (!string.Equals(dlqOpts.Topic, dlqTopic, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    $"AutoDlqOnDeliveryFailure requires DLQ producer '{dlqProducerKey}' to have Topic='{dlqTopic}', but found '{dlqOpts.Topic}'.");
            }

            return await SendMessageWithSchemaAsync(
                message: dlqMessage,
                key: messageKey,
                producerKey: dlqProducerKey,
                schemaType: Models.SchemaType.Json,
                headers: null,
                ct: ct);
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
            ThrowIfDisposed();
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

            var workItems = PrepareBatchTasks(producer, messages, keySelector, headers, batchId, topic, produceToken);

            await ProcessBatchTasks(workItems, result, batchId, topic, producerKey, produceToken);

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

        Task<BatchResult> IKafkaProducerClient.ProduceAsync<T>(
            IEnumerable<T> messages,
            Func<T, string> keySelector,
            string producerKey,
            Headers? headers,
            ISerializer<T>? serializer,
            CancellationToken ct)
            => SendBatchAsync(messages, keySelector, producerKey, headers, serializer, ct);

        private sealed class BatchProduceWorkItem<T>
        {
            public BatchProduceWorkItem(string key, T message, Headers headers, Task<DeliveryResult<string, T>> task)
            {
                Key = key;
                Message = message;
                Headers = headers;
                Task = task;
            }

            public string Key { get; }
            public T Message { get; }
            public Headers Headers { get; }
            public Task<DeliveryResult<string, T>> Task { get; }
        }

        private IEnumerable<BatchProduceWorkItem<T>> PrepareBatchTasks<T>(
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
                var task = producer.ProduceAsync(topic,
                    new Message<string, T>
                    {
                        Key = key,
                        Value = m,
                        Headers = hdr
                    }, ct);

                return new BatchProduceWorkItem<T>(key, m, hdr, task);
            });
        }

        private async Task ProcessBatchTasks<T>(
            IEnumerable<BatchProduceWorkItem<T>> workItems,
            BatchResult result,
            string batchId,
            string topic,
            string producerKey,
            CancellationToken ct)
        {
            int i = 0;
            foreach (var item in workItems)
            {
                try
                {
                    var deliveryResult = await item.Task;
                    result.AddSuccess(deliveryResult.Topic, deliveryResult.Partition.Value, deliveryResult.Offset.Value, deliveryResult.Key);
                    _logger.LogDebug("Delivered (batch {batchId}) msgIndex={index} key={key} part={part} offset={offset}", batchId, i, deliveryResult.Key, deliveryResult.Partition.Value, deliveryResult.Offset.Value);
                }
                catch (TaskCanceledException tce)
                {
                    _logger.LogWarning(tce, "Produce canceled (batch {batchId}) msgIndex={index} tokenCanceled={tokenCanceled}", batchId, i, ct.IsCancellationRequested);
                    var r = new KafkaResult(false, topic: topic, key: item.Key, errorMessage: "canceled");
                    await ApplyAutoDlqOnDeliveryFailureAsync(r, item.Message, item.Key, topic, item.Headers, producerKey, tce, ct);
                    result.AddResult(r);
                }
                catch (ProduceException<string, T> pex)
                {
                    _logger.LogError(pex, "Produce failed in batch {id}: {reason}", batchId, pex.Error.Reason);
                    var r = new KafkaResult(false, topic: topic, key: item.Key, errorMessage: pex.Error.Reason);
                    await ApplyAutoDlqOnDeliveryFailureAsync(r, item.Message, item.Key, topic, item.Headers, producerKey, pex, ct);
                    result.AddResult(r);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Produce failed in batch {id}: {reason}", batchId, ex.Message);
                    var r = new KafkaResult(false, topic: topic, key: item.Key, errorMessage: ex.Message);
                    await ApplyAutoDlqOnDeliveryFailureAsync(r, item.Message, item.Key, topic, item.Headers, producerKey, ex, ct);
                    result.AddResult(r);
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
            if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

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

            _producers.Clear();

            try
            {
                _schemaRegistry.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error during schema registry dispose");
            }

            _logger.LogInformation("KafkaProducerClient disposed");
        }

        /// <summary>
        /// Sends a failed message to the dead letter queue with JSON schema.
        /// Uses the configured DLQ topic pattern (default: "dlq-{topic}").
        /// </summary>
        public Task<KafkaResult> SendToDeadLetterQueueAsync(Models.DeadLetterMessage dlqMessage)
            => SendToDeadLetterQueueAsync(dlqMessage, producerKey: "default", ct: default);

        /// <inheritdoc cref="JohBloch.ConfluentKafka.Clients.Interfaces.IKafkaProducerClient.SendToDeadLetterQueueAsync(JohBloch.ConfluentKafka.Clients.Models.DeadLetterMessage,System.Threading.CancellationToken)" />
        public Task<KafkaResult> SendToDeadLetterQueueAsync(Models.DeadLetterMessage dlqMessage, CancellationToken ct)
            => SendToDeadLetterQueueAsync(dlqMessage, producerKey: "default", ct: ct);

        /// <inheritdoc cref="JohBloch.ConfluentKafka.Clients.Interfaces.IKafkaProducerClient.SendToDeadLetterQueueAsync(JohBloch.ConfluentKafka.Clients.Models.DeadLetterMessage,string)" />
        public Task<KafkaResult> SendToDeadLetterQueueAsync(Models.DeadLetterMessage dlqMessage, string producerKey)
            => SendToDeadLetterQueueAsync(dlqMessage, producerKey: producerKey, ct: default);

        /// <inheritdoc cref="JohBloch.ConfluentKafka.Clients.Interfaces.IKafkaProducerClient.SendToDeadLetterQueueAsync(JohBloch.ConfluentKafka.Clients.Models.DeadLetterMessage,string,System.Threading.CancellationToken)" />
        public Task<KafkaResult> SendToDeadLetterQueueAsync(Models.DeadLetterMessage dlqMessage, string producerKey, CancellationToken ct)
            => SendToDeadLetterQueueAsync(dlqMessage, key: null, producerKey: producerKey, ct: ct);

        /// <inheritdoc cref="JohBloch.ConfluentKafka.Clients.Interfaces.IKafkaProducerClient.SendToDeadLetterQueueAsync(JohBloch.ConfluentKafka.Clients.Models.DeadLetterMessage,string,string,System.Threading.CancellationToken)" />
        public async Task<KafkaResult> SendToDeadLetterQueueAsync(
            Models.DeadLetterMessage dlqMessage,
            string? key,
            string producerKey,
            CancellationToken ct)
        {
            ThrowIfDisposed();
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
        public Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(ConsumeResult<TKey, TValue> originalMessage, Exception exception)
            => SendToDeadLetterQueueAsync(originalMessage, exception, retryCount: 0, producerKey: "default", additionalMetadata: null, ct: default);

        /// <inheritdoc cref="JohBloch.ConfluentKafka.Clients.Interfaces.IKafkaProducerClient.SendToDeadLetterQueueAsync{TKey,TValue}(Confluent.Kafka.ConsumeResult{TKey,TValue},System.Exception,System.Threading.CancellationToken)" />
        public Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(ConsumeResult<TKey, TValue> originalMessage, Exception exception, CancellationToken ct)
            => SendToDeadLetterQueueAsync(originalMessage, exception, retryCount: 0, producerKey: "default", additionalMetadata: null, ct: ct);

        /// <inheritdoc cref="JohBloch.ConfluentKafka.Clients.Interfaces.IKafkaProducerClient.SendToDeadLetterQueueAsync{TKey,TValue}(Confluent.Kafka.ConsumeResult{TKey,TValue},System.Exception,int)" />
        public Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(ConsumeResult<TKey, TValue> originalMessage, Exception exception, int retryCount)
            => SendToDeadLetterQueueAsync(originalMessage, exception, retryCount: retryCount, producerKey: "default", additionalMetadata: null, ct: default);

        /// <inheritdoc cref="JohBloch.ConfluentKafka.Clients.Interfaces.IKafkaProducerClient.SendToDeadLetterQueueAsync{TKey,TValue}(Confluent.Kafka.ConsumeResult{TKey,TValue},System.Exception,int,System.Threading.CancellationToken)" />
        public Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(ConsumeResult<TKey, TValue> originalMessage, Exception exception, int retryCount, CancellationToken ct)
            => SendToDeadLetterQueueAsync(originalMessage, exception, retryCount: retryCount, producerKey: "default", additionalMetadata: null, ct: ct);

        /// <inheritdoc cref="JohBloch.ConfluentKafka.Clients.Interfaces.IKafkaProducerClient.SendToDeadLetterQueueAsync{TKey,TValue}(Confluent.Kafka.ConsumeResult{TKey,TValue},System.Exception,int,string,System.Collections.Generic.Dictionary{string,string},System.Threading.CancellationToken)" />
        public async Task<KafkaResult> SendToDeadLetterQueueAsync<TKey, TValue>(
            ConsumeResult<TKey, TValue> originalMessage,
            Exception exception,
            int retryCount,
            string producerKey,
            Dictionary<string, string>? additionalMetadata,
            CancellationToken ct)
        {
            ThrowIfDisposed();
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