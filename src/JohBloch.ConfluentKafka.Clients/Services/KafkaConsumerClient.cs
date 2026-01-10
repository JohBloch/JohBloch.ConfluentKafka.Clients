using JohBloch.ConfluentKafka.Clients.Services.Serialization;

namespace JohBloch.ConfluentKafka.Clients.Services
{
    /// <summary>
    /// Kafka consumer client for consuming messages from Kafka topics.
    /// Supports both GenericRecord and SpecificRecord handling.
    /// </summary>
    public class KafkaConsumerClient : IKafkaConsumerClient, IDisposable
    {
        private readonly ILogger<KafkaConsumerClient> _logger;
        private readonly ISchemaRegistryClient _schemaRegistry;
        private readonly KafkaConsumerOptions _kafkaConsumerOpts;
        private readonly SchemaRegistryOptions _srOpts;
        private readonly ISecurityTokenProvider _securityProvider;
        private readonly IConsumer<string, byte[]> _consumer;
        private readonly DeserializerFactory _deserializerFactory;
        private bool _disposed = false;
        private readonly IDictionary<string, string>? _globalConfig;
        private readonly IDictionary<string, string>? _consumerOverrides;

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaConsumerClient"/> class.
        /// </summary>
        public KafkaConsumerClient(
            IOptions<KafkaConsumerOptions> kafkaConsumerOptions,
            IOptions<SchemaRegistryOptions> schemaRegistryOptions,
            ISecurityTokenProvider securityProvider,
            ISchemaRegistryFactory schemaRegistryFactory,
            ILogger<KafkaConsumerClient> logger,
            IDictionary<string, string>? globalConfig = null,
            IDictionary<string, string>? consumerOverrides = null)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _kafkaConsumerOpts = kafkaConsumerOptions?.Value ?? throw new ArgumentNullException(nameof(kafkaConsumerOptions));
            _srOpts = schemaRegistryOptions?.Value ?? throw new ArgumentNullException(nameof(schemaRegistryOptions));
            _securityProvider = securityProvider ?? throw new ArgumentNullException(nameof(securityProvider));

            _schemaRegistry = schemaRegistryFactory?.CreateClient()
                ?? throw new ArgumentNullException(nameof(schemaRegistryFactory));

            // Initialize deserializer factory
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            _deserializerFactory = new DeserializerFactory(_schemaRegistry, loggerFactory);

            _globalConfig = globalConfig;
            _consumerOverrides = consumerOverrides;

            // Initialize consumer
            _consumer = InitializeConsumer();

            // Subscribe to topics
            if (!string.IsNullOrWhiteSpace(_kafkaConsumerOpts.Topic))
            {
                _consumer.Subscribe(_kafkaConsumerOpts.Topic);
                _logger.LogInformation("Subscribed to topic: {topic}", _kafkaConsumerOpts.Topic);
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaConsumerClient"/> class using a dictionary of consumer options keyed by logical consumer name.
        /// </summary>
        /// <param name="consumerOptions">Consumer options keyed by logical consumer name.</param>
        /// <param name="consumerKey">The logical consumer key to select options from the dictionary.</param>
        /// <param name="schemaRegistryOptions">Schema Registry options.</param>
        /// <param name="securityProvider">Security token provider.</param>
        /// <param name="schemaRegistryFactory">Schema Registry client factory.</param>
        /// <param name="logger">Logger instance.</param>
        /// <param name="globalConfig">Optional global configuration settings.</param>
        /// <param name="perConsumerConfigs">Optional per-consumer configuration overrides.</param>
        public KafkaConsumerClient(
            IDictionary<string, KafkaConsumerOptions> consumerOptions,
            string consumerKey,
            IOptions<SchemaRegistryOptions> schemaRegistryOptions,
            ISecurityTokenProvider securityProvider,
            ISchemaRegistryFactory schemaRegistryFactory,
            ILogger<KafkaConsumerClient> logger,
            IDictionary<string, string>? globalConfig = null,
            IDictionary<string, IDictionary<string, string>>? perConsumerConfigs = null)
        {
            if (consumerOptions == null) throw new ArgumentNullException(nameof(consumerOptions));
            if (string.IsNullOrWhiteSpace(consumerKey)) throw new ArgumentNullException(nameof(consumerKey));

            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _srOpts = schemaRegistryOptions?.Value ?? throw new ArgumentNullException(nameof(schemaRegistryOptions));
            _securityProvider = securityProvider ?? throw new ArgumentNullException(nameof(securityProvider));

            if (!consumerOptions.TryGetValue(consumerKey, out var selected))
                throw new KeyNotFoundException($"Consumer options not found for key '{consumerKey}'");
            _kafkaConsumerOpts = selected;

            _schemaRegistry = schemaRegistryFactory?.CreateClient()
                ?? throw new ArgumentNullException(nameof(schemaRegistryFactory));

            // Initialize deserializer factory
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            _deserializerFactory = new DeserializerFactory(_schemaRegistry, loggerFactory);

            _globalConfig = globalConfig;
            _consumerOverrides = perConsumerConfigs != null && perConsumerConfigs.TryGetValue(consumerKey, out var over) ? over : null;

            // Initialize consumer
            _consumer = InitializeConsumer();

            // Subscribe to topics if provided
            if (!string.IsNullOrWhiteSpace(_kafkaConsumerOpts.Topic))
            {
                _consumer.Subscribe(_kafkaConsumerOpts.Topic);
                _logger.LogInformation("[{key}] Subscribed to topic: {topic}", consumerKey, _kafkaConsumerOpts.Topic);
            }
        }

        /// <summary>
        /// Initializes the Kafka consumer with the configured options.
        /// </summary>
    private IConsumer<string, byte[]> InitializeConsumer()
        {
            // Initialization without verbose logging

            var config = BuildConsumerConfig();

            var consumerBuilder = new ConsumerBuilder<string, byte[]>(config)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetValueDeserializer(Deserializers.ByteArray)
                .SetErrorHandler((_, e) =>
                {
                    _logger.LogError("Kafka consumer error: {reason} - {error}", e.Reason, e.ToString());
                })
                .SetLogHandler((_, logMessage) =>
                {
                    var level = logMessage.Level switch
                    {
                        SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical or SyslogLevel.Error => LogLevel.Error,
                        SyslogLevel.Warning => LogLevel.Warning,
                        SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
                        SyslogLevel.Debug => LogLevel.Debug,
                        _ => LogLevel.Information
                    };
                    _logger.Log(level, "Kafka: {message}", logMessage.Message);
                })
                .SetStatisticsHandler((_, json) => { })
                .SetPartitionsAssignedHandler((_, partitions) => { })
                .SetPartitionsRevokedHandler((_, partitions) => { })
                .SetOAuthBearerTokenRefreshHandler(async (consumer, _) =>
                {
                    try
                    {
                        var token = await _securityProvider.GetAccessTokenAsync(CancellationToken.None);
                        var extensions = _securityProvider.GetExtensions() ?? new Dictionary<string, string>();
                        var lifetimeMs = (long)Math.Max(1, (token.ExpiresOn - DateTimeOffset.UtcNow).TotalMilliseconds);
                        consumer.OAuthBearerSetToken(token.AccessTokenValue, lifetimeMs, null, extensions);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to refresh OAuth token for Kafka consumer");
                        consumer.OAuthBearerSetTokenFailure($"Token refresh failed: {ex.Message}");
                    }
                });

            return consumerBuilder.Build();
        }

        /// <summary>
        /// Builds the Kafka consumer configuration.
        /// </summary>
        private ConsumerConfig BuildConsumerConfig()
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = _kafkaConsumerOpts.BootstrapServers,
                GroupId = _kafkaConsumerOpts.GroupId,
                AutoOffsetReset = Enum.TryParse<AutoOffsetReset>(_kafkaConsumerOpts.AutoOffsetReset, out var offsetReset)
                    ? offsetReset
                    : AutoOffsetReset.Earliest,
                EnableAutoCommit = _kafkaConsumerOpts.EnableAutoCommit,
                SessionTimeoutMs = _kafkaConsumerOpts.SessionTimeoutMs,
                // Default to Plaintext in absence of explicit SASL settings to allow local testing without broker security
                SecurityProtocol = SecurityProtocol.Plaintext,
                StatisticsIntervalMs = 5000,
                EnablePartitionEof = true,
                Debug = "cgrp,consumer,fetch,topic,protocol,broker,security"
            };

            // Apply optional global configs
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
            // Apply optional per-consumer overrides
            if (_consumerOverrides is not null)
            {
                foreach (var kvp in _consumerOverrides)
                {
                    if (!string.IsNullOrWhiteSpace(kvp.Value))
                    {
                        config.Set(kvp.Key, kvp.Value);
                    }
                }
            }

            // Optional: let SAL provide additional SASL configs if necessary
            var salConfig = _securityProvider.GetKafkaSaslConfig();
            if (salConfig is not null && salConfig.Count > 0)
            {
                // If SAL provides SASL settings, switch protocol to SaslSsl and apply values
                // Expectation: SAL includes keys like 'sasl.mechanism', 'sasl.oauthbearer.method', and token endpoint url when using OIDC
                config.SecurityProtocol = SecurityProtocol.Ssl; // or SaslSsl depending on cluster setup

                foreach (var kv in salConfig)
                {
                    config.Set(kv.Key, kv.Value);
                }

                // If SAL did not explicitly set mechanism/method, do not force OIDC
                // Only set OAuth/OIDC when required keys exist
                if (salConfig.TryGetValue("sasl.mechanism", out var mech) && mech.Equals("oauthbearer", StringComparison.OrdinalIgnoreCase))
                {
                    // Only set OIDC if token endpoint URL is supplied
                    if (salConfig.ContainsKey("sasl.oauthbearer.token.endpoint.url"))
                    {
                        config.SaslMechanism = SaslMechanism.OAuthBearer;
                        config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
                    }
                    else
                    {
                        // Leave method unset to avoid Kafka requiring missing endpoint
                        config.SaslMechanism = SaslMechanism.OAuthBearer;
                    }
                }
            }

            return config;
        }

        /// <summary>
        /// Subscribes to the specified topics.
        /// </summary>
        /// <param name="topics">The topics to subscribe to.</param>
        public void Subscribe(IEnumerable<string> topics)
        {
            if (topics == null)
            {
                throw new ArgumentNullException(nameof(topics));
            }

            var topicsList = topics.ToList();
            if (topicsList.Count == 0)
            {
                throw new ArgumentException("At least one topic must be specified", nameof(topics));
            }

            _consumer.Subscribe(topicsList);
            // Intentionally no verbose subscription logging
        }

        /// <summary>
        /// Generic consume method that handles deserialization for POCOs and raw types.
        /// </summary>
        public async Task<ConsumeResult<string, T>?> ConsumeAsync<T>(CancellationToken ct = default)
        {
            try
            {
                // bounded poll with cancellation
                var timeout = TimeSpan.FromSeconds(5);
                ConsumeResult<string, byte[]>? result = null;

                var source = CancellationTokenSource.CreateLinkedTokenSource(ct);
                source.CancelAfter(timeout);

                try
                {
                    result = _consumer.Consume(source.Token);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    return null; // timeout
                }

                if (result is null || result.IsPartitionEOF || result.Message is null)
                {
                    return null;
                }

                // raw value
                var bytes = result.Message.Value;
                if (bytes is null)
                {
                    return null; // tombstone
                }

                T value;
                if (typeof(T) == typeof(byte[]))
                {
                    value = (T)(object)bytes;
                }
                else if (typeof(T) == typeof(string))
                {
                    value = (T)(object)System.Text.Encoding.UTF8.GetString(bytes);
                }
                else
                {
                    // Determine schema type
                    var schemaType = await DetermineSchemaTypeAsync(bytes, result.Topic);
                    
                    // Use appropriate deserializer
                    var deserializer = _deserializerFactory.Create<T>(schemaType);
                    value = await deserializer.DeserializeAsync(
                        bytes,
                        new SerializationContext(MessageComponentType.Value, result.Topic));
                }

                return new ConsumeResult<string, T>
                {
                    Topic = result.Topic,
                    Partition = result.Partition,
                    Offset = result.Offset,
                    Message = new Message<string, T>
                    {
                        Key = result.Message.Key,
                        Value = value,
                        Headers = result.Message.Headers,
                        Timestamp = result.Message.Timestamp
                    }
                };
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_TimedOut)
            {
                return null;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                _logger.LogWarning("ConsumeAsync<{type}>: Operation cancelled", typeof(T).Name);
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ConsumeAsync<{type}>: Error consuming message", typeof(T).Name);
                throw;
            }
        }

        /// <summary>
        /// Consumes a message with a specific deserializer.
        /// </summary>
    private async Task<ConsumeResult<string, T>?> ConsumeWithDeserializer<T>(CancellationToken ct)
        {
            try
            {
                //

                // Set a timeout to avoid blocking indefinitely
                var timeout = TimeSpan.FromSeconds(5);
                ConsumeResult<string, byte[]>? result = null;

                // Try to consume with cancellation token support
                var source = CancellationTokenSource.CreateLinkedTokenSource(ct);
                source.CancelAfter(timeout);

                try
                {
                    // Consume raw message
                    result = _consumer.Consume(source.Token);
                }
                catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                {
                    // timeout
                    return null;
                }

                if (result == null)
                {
                    // no message
                    return null;
                }

                if (result.IsPartitionEOF)
                {
                    // reached EOF
                    return null;
                }

                if (result.Message is null)
                {
                    // non-message event
                    return null;
                }

                // message received

                // Get the raw bytes
                var bytes = result.Message.Value;
                if (bytes is null)
                {
                    // tombstone
                    return null;
                }

                // Determine schema type and create deserializer
                var schemaType = await DetermineSchemaTypeAsync(bytes, result.Topic);
                var deserializer = _deserializerFactory.Create<T>(schemaType);

                // Deserialize the bytes
                var value = await deserializer.DeserializeAsync(
                    bytes,
                    new SerializationContext(MessageComponentType.Value, result.Topic));

                return new ConsumeResult<string, T>
                {
                    Topic = result.Topic,
                    Partition = result.Partition,
                    Offset = result.Offset,
                    Message = new Message<string, T>
                    {
                        Key = result.Message.Key,
                        Value = value,
                        Headers = result.Message.Headers,
                        Timestamp = result.Message.Timestamp
                    }
                };
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.Local_TimedOut)
            {
                // timeout
                return null;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                _logger.LogWarning("ConsumeAsync<{type}>: Operation cancelled", typeof(T).Name);
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ConsumeAsync<{type}>: Error consuming message", typeof(T).Name);
                throw;
            }
        }

        /// <summary>
        /// Consumes a batch of messages.
        /// </summary>
        public async Task<List<ConsumeResult<string, T>>> ConsumeBatchAsync<T>(int maxMessages, int timeoutMs = 5000, CancellationToken ct = default)
        {
            // Simplified: only generic/POCO and raw types supported. Avro GenericRecord/SpecificRecord paths removed.
            return await ConsumeBatchWithDeserializer<T>(maxMessages, timeoutMs, ct);
        }

        /// <summary>
        /// Consumes a batch of messages with a specific deserializer.
        /// </summary>
        private async Task<List<ConsumeResult<string, T>>> ConsumeBatchWithDeserializer<T>(int maxMessages, int timeoutMs, CancellationToken ct)
        {
            var results = new List<ConsumeResult<string, T>>();
            var stopwatch = System.Diagnostics.Stopwatch.StartNew();

            try
            {
                //

            // Check if consumer is initialized
            if (_consumer == null)
            {
                _logger.LogError("Consumer is null - not initialized");
                return results;
            }

                //

                if (ct.IsCancellationRequested)
                {
                    //
                } else {
                    //
                }    


                    //
                while (results.Count < maxMessages &&
                       stopwatch.ElapsedMilliseconds < timeoutMs &&
                       !ct.IsCancellationRequested)
                {
                    //

                    // Calculate remaining timeout for the overall batch and enforce a sensible per-iteration floor to avoid near-immediate cancellation
                    var remainingTimeout = Math.Max(1, timeoutMs - (int)stopwatch.ElapsedMilliseconds);
                    var perIterationTimeoutMs = Math.Min(1000, Math.Max(200, remainingTimeout)); // 200ms-1s window
                    //
                    LogAssignmentAndLag(nameof(ConsumeBatchWithDeserializer));
                    // Try to consume a single message using bounded poll; null on timeout
                    ConsumeResult<string, byte[]>? result = null;
                    try
                    {
                        result = _consumer.Consume(TimeSpan.FromMilliseconds(perIterationTimeoutMs));
                        // no verbose logging here
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Unexpected exception during per-iteration consume");
                        continue;
                    }

                    if (result == null)
                    {
                    //
                        // No message, but we might still have time for another attempt
                        continue;
                    }
                    //
                    // New: handle EOF/non-message events early to avoid accessing Message
                    if (result.IsPartitionEOF)
                    {
                        // EOF
                        continue;
                    }
                    if (result.Message is null)
                    {
                        // non-message event
                        continue;
                    }
                    //
                    // Log structured message meta instead of object ToString
                    var keyStr = result.Message?.Key;
                    //
                    var headersStr = FormatHeaders(result.Message?.Headers);
                    //
                    var valLen = result.Message?.Value?.Length ?? 0;
                    //
                    var preview = result.Message?.Value is null ? "null" : PreviewBytes(result.Message.Value, 64);
                    //
                    // Message is guaranteed non-null here due to early guard above
                    var msg = result.Message;

                    //
                    // Get the raw bytes
                    var bytes = msg!.Value;
                    if (bytes is null)
                    {
                        // tombstone
                        continue;
                    }
                    
                    // Determine schema type and create deserializer
                    var schemaType = await DetermineSchemaTypeAsync(bytes, result.Topic);
                    var deserializer = _deserializerFactory.Create<T>(schemaType);
                    
                    //                    
                    // Deserialize the bytes
                    var value = await deserializer.DeserializeAsync(
                        bytes,
                        new SerializationContext(MessageComponentType.Value, result.Topic));
                    //
                    try
                    {
                        var formatted = FormatValue(value, 2000);
                        //
                    }
                    catch { }
                    var typedResult = new ConsumeResult<string, T>
                    {
                        Topic = result.Topic,
                        Partition = result.Partition,
                        Offset = result.Offset,
                        Message = new Message<string, T>
                        {
                            Key = msg.Key,
                            Value = value,
                            Headers = msg.Headers,
                            Timestamp = msg.Timestamp
                        }
                    };
                    //
                    results.Add(typedResult);
                }

                //

                return results;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                _logger.LogWarning("ConsumeBatchAsync<{type}>: Operation cancelled after consuming {count} messages",
                    typeof(T).Name, results.Count);
                return results; // Return whatever we've collected so far
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ConsumeBatchAsync<{type}>: Error consuming batch", typeof(T).Name);
                throw;
            }
        }

        /// <summary>
        /// Commits the offset for a specific consumed message.
        /// </summary>
    public void CommitAsync(ConsumeResult<string, byte[]> result)
        {
            if (result == null)
            {
                throw new ArgumentNullException(nameof(result));
            }
            if (!_kafkaConsumerOpts.EnableAutoCommit)
            {
                _consumer.Commit(new[] { result.TopicPartitionOffset });
                //
            }
            else
            {
                //
            }
        }

        /// <summary>
        /// Converts a ConsumeResult from one type to another.
        /// </summary>
        private ConsumeResult<string, TOut>? ConvertConsumeResult<TIn, TOut>(ConsumeResult<string, TIn>? result)
        {
            if (result is null || result.Message is null || result.Message.Value is null)
            {
                return null;
            }

            return new ConsumeResult<string, TOut>
            {
                Topic = result.Topic,
                Partition = result.Partition,
                Offset = result.Offset,
                Message = new Message<string, TOut>
                {
                    Key = result.Message.Key,
                    Value = (TOut)(object)result.Message.Value!,
                    Headers = result.Message.Headers,
                    Timestamp = result.Message.Timestamp
                }
            };
        }

        /// <summary>
        /// Manually commits the current offset for all partitions.
        /// </summary>
        public void Commit()
        {
            try
            {
                _consumer.Commit();
                //
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during manual commit: {error}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Manually commits the offset for the specified message.
        /// </summary>
        public void Commit<T>(ConsumeResult<string, T> result)
        {
            try
            {
                _consumer.Commit(new[] { result.TopicPartitionOffset });
                //
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during manual commit for offset {offset}: {error}", result.Offset, ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Unsubscribes from all currently subscribed topics.
        /// </summary>
        public void Unsubscribe()
        {
            try
            {
                _consumer.Unsubscribe();
                //
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during unsubscribe: {error}", ex.Message);
                throw;
            }
        }

        /// <summary>
        /// Gets the consumer's current assignment (list of partitions assigned to this consumer).
        /// </summary>
        public List<TopicPartition> Assignment => _consumer.Assignment;

        /// <summary>
        /// Gets the current consumer subscription (list of subscribed topics).
        /// </summary>
        public List<string> Subscription => _consumer.Subscription;

        /// <summary>
        /// Disposes the consumer resources.
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _consumer?.Close();
                _consumer?.Dispose();
                _schemaRegistry?.Dispose();
                _disposed = true;
                //
            }
        }

        /// <summary>
        /// Logs current assignment and per-partition lag using Position and watermark offsets.
        /// Helps diagnose why a bounded Consume may return null (no assignment or at high watermark).
        /// </summary>
        private void LogAssignmentAndLag(string phase)
        {
            try
            {
                var assignment = _consumer?.Assignment ?? new List<TopicPartition>();
                if (assignment.Count == 0)
                {
                    // No partitions assigned yet
                    return;
                }

                foreach (var tp in assignment)
                {
                    var pos = _consumer!.Position(tp);
                    WatermarkOffsets wm;
                    try
                    {
                        wm = _consumer.QueryWatermarkOffsets(tp, TimeSpan.FromMilliseconds(500));
                    }
                    catch (Exception)
                    {
                        // Ignore watermark query errors to avoid noisy logs
                        continue;
                    }

                    long high = wm.High.Value;
                    long current = pos.IsSpecial ? high : pos.Value;
                    long lag = Math.Max(0, high - current);
                    // Intentionally not logging each partition to keep noise low
                }
            }
            catch (Exception)
            {
                // Swallow diagnostics errors
            }
        }

        private static string FormatHeaders(Headers? headers)
        {
            if (headers == null || headers.Count == 0)
                return string.Empty;
            try
            {
                return string.Join(", ", headers.Select(h =>
                {
                    var pv = h.GetValueBytes();
                    var prev = pv is null ? "null" : PreviewBytes(pv, 16);
                    return $"{h.Key}={prev}";
                }));
            }
            catch { return "<headers-error>"; }
        }

        private static string PreviewBytes(byte[]? data, int max)
        {
            var take = Math.Min(max, data?.Length ?? 0);
            if (take == 0) return string.Empty;
            var sb = new System.Text.StringBuilder(take * 2);
            for (int i = 0; i < take; i++) sb.Append(data![i].ToString("X2"));
            if (take < (data?.Length ?? 0)) sb.Append("...");
            return sb.ToString();
        }

        private static string FormatValue(object? value, int maxChars)
        {
            if (value is null) return "null";
            try
            {
                string s;
                try { s = System.Text.Json.JsonSerializer.Serialize(value); }
                catch { s = value.ToString() ?? value.GetType().Name; }
                if (s.Length > maxChars) s = s.Substring(0, maxChars) + "...";
                return s;
            }
            catch { return "<format-error>"; }
        }

        /// <summary>
        /// Determines the schema type to use for deserialization based on configuration and auto-detection.
        /// </summary>
        private async Task<Models.SchemaType> DetermineSchemaTypeAsync(byte[] data, string topic)
        {
            // Check if there's a per-topic override
            if (_kafkaConsumerOpts.TopicSchemaTypes.TryGetValue(topic, out var overrideType))
            {
                return overrideType;
            }

            // Use auto-detection if enabled
            if (_kafkaConsumerOpts.AutoDetectSchemaType)
            {
                return await _deserializerFactory.DetectSchemaTypeAsync(data, topic);
            }

            // Fall back to default schema type
            return _kafkaConsumerOpts.DefaultSchemaType;
        }
    }
}