using JohBloch.ConfluentKafka.Clients.Services.Serialization;
using System.ComponentModel;

namespace JohBloch.ConfluentKafka.Clients.Services
{
    /// <summary>
    /// Kafka consumer client for consuming messages from Kafka topics.
    /// Supports both GenericRecord and SpecificRecord handling.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public class KafkaConsumerClient : IKafkaConsumerClient, IDisposable
    {
        private readonly ILogger<KafkaConsumerClient> _logger;
        private readonly JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient _schemaRegistry;
        private readonly bool _ownsSchemaRegistry;
        private readonly KafkaConsumerOptions _kafkaConsumerOpts;
        private readonly SchemaRegistryOptions _srOpts;
        private readonly ISecurityTokenProvider _securityProvider;
        private readonly IConsumer<string, byte[]> _consumer;
        private readonly DeserializerFactory _deserializerFactory;
        private int _disposed;
        private readonly IDictionary<string, string>? _globalConfig;
        private readonly IDictionary<string, string>? _consumerOverrides;
        private bool _oauthRefreshHandlerEnabled;

        private void ThrowIfDisposed()
        {
            if (System.Threading.Volatile.Read(ref _disposed) != 0)
            {
                throw new ObjectDisposedException(nameof(KafkaConsumerClient));
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaConsumerClient"/> class.
        /// Preferred overload: takes an <see cref="JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient"/> directly.
        /// </summary>
        public KafkaConsumerClient(
            IOptions<KafkaConsumerOptions> kafkaConsumerOptions,
            IOptions<SchemaRegistryOptions> schemaRegistryOptions,
            ISecurityTokenProvider securityProvider,
            JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient schemaRegistry,
            ILoggerFactory loggerFactory,
            ILogger<KafkaConsumerClient> logger)
            : this(
                kafkaConsumerOptions,
                schemaRegistryOptions,
                securityProvider,
                schemaRegistry,
                loggerFactory,
                logger,
                globalConfig: null,
                consumerOverrides: null,
                consumerOverride: null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaConsumerClient"/> class.
        /// Preferred overload: takes an <see cref="JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient"/> directly.
        /// </summary>
        /// <param name="kafkaConsumerOptions">Consumer options.</param>
        /// <param name="schemaRegistryOptions">Schema Registry options.</param>
        /// <param name="securityProvider">Security token provider.</param>
        /// <param name="schemaRegistry">Schema Registry extended client.</param>
        /// <param name="loggerFactory">Logger factory (used by serializers/deserializers).</param>
        /// <param name="logger">Logger instance.</param>
        /// <param name="globalConfig">Optional global configuration settings applied to the consumer.</param>
        /// <param name="consumerOverrides">Optional per-consumer configuration overrides.</param>
        public KafkaConsumerClient(
            IOptions<KafkaConsumerOptions> kafkaConsumerOptions,
            IOptions<SchemaRegistryOptions> schemaRegistryOptions,
            ISecurityTokenProvider securityProvider,
            JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient schemaRegistry,
            ILoggerFactory loggerFactory,
            ILogger<KafkaConsumerClient> logger,
            IDictionary<string, string>? globalConfig,
            IDictionary<string, string>? consumerOverrides)
            : this(
                kafkaConsumerOptions,
                schemaRegistryOptions,
                securityProvider,
                schemaRegistry,
                loggerFactory,
                logger,
                globalConfig,
                consumerOverrides,
                consumerOverride: null)
        {
        }

        internal KafkaConsumerClient(
            IOptions<KafkaConsumerOptions> kafkaConsumerOptions,
            IOptions<SchemaRegistryOptions> schemaRegistryOptions,
            ISecurityTokenProvider securityProvider,
            JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient schemaRegistry,
            ILoggerFactory loggerFactory,
            ILogger<KafkaConsumerClient> logger,
            IDictionary<string, string>? globalConfig,
            IDictionary<string, string>? consumerOverrides,
            IConsumer<string, byte[]>? consumerOverride)
        {
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(loggerFactory);
            ArgumentNullException.ThrowIfNull(kafkaConsumerOptions);
            ArgumentNullException.ThrowIfNull(schemaRegistryOptions);
            ArgumentNullException.ThrowIfNull(securityProvider);
            ArgumentNullException.ThrowIfNull(schemaRegistry);

            _logger = logger;
            _kafkaConsumerOpts = kafkaConsumerOptions.Value;
            _srOpts = schemaRegistryOptions.Value;
            _securityProvider = securityProvider;
            _schemaRegistry = schemaRegistry;
            _ownsSchemaRegistry = false;

            _deserializerFactory = new DeserializerFactory(_schemaRegistry, loggerFactory);

            _globalConfig = globalConfig;
            _consumerOverrides = consumerOverrides;

            _consumer = consumerOverride ?? InitializeConsumer();

            SubscribeFromOptions(prefix: null);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaConsumerClient"/> class using a dictionary of consumer options keyed by logical consumer name.
        /// Preferred overload: takes an <see cref="JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient"/> directly.
        /// </summary>
        /// <param name="consumerOptions">Consumer options keyed by logical consumer name.</param>
        /// <param name="consumerKey">The logical consumer key to select options from the dictionary.</param>
        /// <param name="schemaRegistryOptions">Schema Registry options.</param>
        /// <param name="securityProvider">Security token provider.</param>
        /// <param name="schemaRegistry">Schema Registry extended client.</param>
        /// <param name="loggerFactory">Logger factory (used by serializers/deserializers).</param>
        /// <param name="logger">Logger instance.</param>
        public KafkaConsumerClient(
            IDictionary<string, KafkaConsumerOptions> consumerOptions,
            string consumerKey,
            IOptions<SchemaRegistryOptions> schemaRegistryOptions,
            ISecurityTokenProvider securityProvider,
            JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient schemaRegistry,
            ILoggerFactory loggerFactory,
            ILogger<KafkaConsumerClient> logger)
            : this(
                consumerOptions,
                consumerKey,
                schemaRegistryOptions,
                securityProvider,
                schemaRegistry,
                loggerFactory,
                logger,
                globalConfig: null,
                perConsumerConfigs: null,
                consumerOverride: null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="KafkaConsumerClient"/> class using a dictionary of consumer options keyed by logical consumer name.
        /// Preferred overload: takes an <see cref="JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient"/> directly.
        /// </summary>
        /// <param name="consumerOptions">Consumer options keyed by logical consumer name.</param>
        /// <param name="consumerKey">The logical consumer key to select options from the dictionary.</param>
        /// <param name="schemaRegistryOptions">Schema Registry options.</param>
        /// <param name="securityProvider">Security token provider.</param>
        /// <param name="schemaRegistry">Schema Registry extended client.</param>
        /// <param name="loggerFactory">Logger factory (used by serializers/deserializers).</param>
        /// <param name="logger">Logger instance.</param>
        /// <param name="globalConfig">Optional global configuration settings applied to the consumer.</param>
        /// <param name="perConsumerConfigs">Optional per-consumer configuration overrides keyed by consumer key.</param>
        public KafkaConsumerClient(
            IDictionary<string, KafkaConsumerOptions> consumerOptions,
            string consumerKey,
            IOptions<SchemaRegistryOptions> schemaRegistryOptions,
            ISecurityTokenProvider securityProvider,
            JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient schemaRegistry,
            ILoggerFactory loggerFactory,
            ILogger<KafkaConsumerClient> logger,
            IDictionary<string, string>? globalConfig,
            IDictionary<string, IDictionary<string, string>>? perConsumerConfigs)
            : this(
                consumerOptions,
                consumerKey,
                schemaRegistryOptions,
                securityProvider,
                schemaRegistry,
                loggerFactory,
                logger,
                globalConfig,
                perConsumerConfigs,
                consumerOverride: null)
        {
        }

        internal KafkaConsumerClient(
            IDictionary<string, KafkaConsumerOptions> consumerOptions,
            string consumerKey,
            IOptions<SchemaRegistryOptions> schemaRegistryOptions,
            ISecurityTokenProvider securityProvider,
            JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient schemaRegistry,
            ILoggerFactory loggerFactory,
            ILogger<KafkaConsumerClient> logger,
            IDictionary<string, string>? globalConfig,
            IDictionary<string, IDictionary<string, string>>? perConsumerConfigs,
            IConsumer<string, byte[]>? consumerOverride)
        {
            if (consumerOptions == null)
            {
                throw new ArgumentNullException(nameof(consumerOptions));
            }
            if (string.IsNullOrWhiteSpace(consumerKey))
            {
                throw new ArgumentNullException(nameof(consumerKey));
            }

            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(loggerFactory);
            ArgumentNullException.ThrowIfNull(schemaRegistryOptions);
            ArgumentNullException.ThrowIfNull(securityProvider);
            ArgumentNullException.ThrowIfNull(schemaRegistry);

            _logger = logger;
            _srOpts = schemaRegistryOptions.Value;
            _securityProvider = securityProvider;

            if (!consumerOptions.TryGetValue(consumerKey, out KafkaConsumerOptions? selected))
            {
                throw new KeyNotFoundException($"Consumer options not found for key '{consumerKey}'");
            }
            _kafkaConsumerOpts = selected;

            _schemaRegistry = schemaRegistry;
            _ownsSchemaRegistry = false;

            _deserializerFactory = new DeserializerFactory(_schemaRegistry, loggerFactory);

            _globalConfig = globalConfig;
            IDictionary<string, string>? over = null;
            bool foundOverrides = perConsumerConfigs is not null
                && perConsumerConfigs.TryGetValue(consumerKey, out over);
            _consumerOverrides = foundOverrides ? over : null;

            _consumer = consumerOverride ?? InitializeConsumer();

            SubscribeFromOptions(prefix: consumerKey);
        }

        private void SubscribeFromOptions(string? prefix)
        {
            IReadOnlyList<string> topics = _kafkaConsumerOpts.GetTopics();
            if (topics.Count == 0)
            {
                return;
            }

            _consumer.Subscribe(topics);

            string joined = string.Join(",", topics);
            if (topics.Count == 1)
            {
                if (string.IsNullOrWhiteSpace(prefix))
                {
                    _logger.LogInformation("Subscribed to topic: {topic}", joined);
                }
                else
                {
                    _logger.LogInformation("[{key}] Subscribed to topic: {topic}", prefix, joined);
                }
            }
            else
            {
                if (string.IsNullOrWhiteSpace(prefix))
                {
                    _logger.LogInformation("Subscribed to topics: {topics}", joined);
                }
                else
                {
                    _logger.LogInformation("[{key}] Subscribed to topics: {topics}", prefix, joined);
                }
            }
        }


        /// <summary>
        /// Initializes the Kafka consumer with the configured options.
        /// </summary>
    private IConsumer<string, byte[]> InitializeConsumer()
        {
            // Initialization without verbose logging

            ConsumerConfig config = BuildConsumerConfig();

            ConsumerBuilder<string, byte[]> consumerBuilder = new ConsumerBuilder<string, byte[]>(config)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetValueDeserializer(Deserializers.ByteArray)
                .SetErrorHandler((_, e) =>
                {
                    _logger.LogError("Kafka consumer error: {reason} - {error}", e.Reason, e.ToString());
                })
                .SetLogHandler((_, logMessage) =>
                {
                    LogLevel level = logMessage.Level switch
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
                .SetPartitionsRevokedHandler((_, partitions) => { });

            // Only attach OAuth refresh handler when OAuth mode is selected.
            if (_oauthRefreshHandlerEnabled)
            {
                consumerBuilder.SetOAuthBearerTokenRefreshHandler((consumer, _) =>
                {
                    try
                    {
                        AccessToken token = _securityProvider.GetAccessTokenAsync(CancellationToken.None)
                            .GetAwaiter()
                            .GetResult();

                        Dictionary<string, string> extensions = _securityProvider.GetExtensions() ?? new Dictionary<string, string>();
                        long nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                        long expMs = token.ExpiresOn.ToUnixTimeMilliseconds();
                        if (expMs <= nowMs)
                        {
                            throw new InvalidOperationException(
                                $"OAuth token is already expired (nowMs={nowMs}, expMs={expMs}).");
                        }

                        // librdkafka expects an absolute expiry timestamp (ms since epoch).
                        consumer.OAuthBearerSetToken(token.AccessTokenValue, expMs, null, extensions);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Failed to refresh OAuth token for Kafka consumer");
                        try
                        {
                            consumer.OAuthBearerSetTokenFailure($"Token refresh failed: {ex.Message}");
                        }
                        catch
                        {
                        }
                    }
                });
            }

            return consumerBuilder.Build();
        }

        /// <summary>
        /// Builds the Kafka consumer configuration.
        /// </summary>
        private ConsumerConfig BuildConsumerConfig()
        {
            _oauthRefreshHandlerEnabled = false;

            ConsumerConfig config = new ConsumerConfig
            {
                BootstrapServers = _kafkaConsumerOpts.BootstrapServers,
                GroupId = _kafkaConsumerOpts.GroupId,
                AutoOffsetReset = Enum.TryParse<AutoOffsetReset>(_kafkaConsumerOpts.AutoOffsetReset, out AutoOffsetReset offsetReset)
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

            // Apply optional global configs and per-consumer overrides
            ApplyConfigDictionary(config, _globalConfig);
            ApplyConfigDictionary(config, _consumerOverrides);

            Dictionary<string, string>? saslFromProvider = null;
            KafkaConsumerSecurityMode mode = _kafkaConsumerOpts.SecurityMode;

            if (mode == KafkaConsumerSecurityMode.Auto)
            {
                // Prefer api_key/api_secret when present; otherwise fall back to OAuth when configured.
                if (!string.IsNullOrWhiteSpace(_kafkaConsumerOpts.ApiKey)
                    || !string.IsNullOrWhiteSpace(_kafkaConsumerOpts.ApiSecret))
                {
                    mode = KafkaConsumerSecurityMode.ApiKeySecret;
                }
                else
                {
                    saslFromProvider = _securityProvider.GetKafkaSaslConfig();
                    mode = (saslFromProvider is not null && saslFromProvider.Count > 0)
                        ? KafkaConsumerSecurityMode.OAuth
                        : KafkaConsumerSecurityMode.None;
                }
            }

            if (mode == KafkaConsumerSecurityMode.ApiKeySecret)
            {
                if (string.IsNullOrWhiteSpace(_kafkaConsumerOpts.ApiKey)
                    || string.IsNullOrWhiteSpace(_kafkaConsumerOpts.ApiSecret))
                {
                    throw new InvalidOperationException(
                        "Consumer SecurityMode is ApiKeySecret, but ApiKey/ApiSecret is missing.");
                }

                _oauthRefreshHandlerEnabled = false;

                config.SecurityProtocol = SecurityProtocol.SaslSsl;
                config.SaslMechanism = SaslMechanism.Plain;
                config.SaslUsername = _kafkaConsumerOpts.ApiKey;
                config.SaslPassword = _kafkaConsumerOpts.ApiSecret;
            }
            else if (mode == KafkaConsumerSecurityMode.OAuth)
            {
                _oauthRefreshHandlerEnabled = true;

                saslFromProvider ??= _securityProvider.GetKafkaSaslConfig();
                if (saslFromProvider is not null && saslFromProvider.Count > 0)
                {
                    config.SecurityProtocol = SecurityProtocol.SaslSsl;
                    ApplyConfigDictionary(config, saslFromProvider);

                    if (saslFromProvider.TryGetValue("sasl.mechanism", out string? mech)
                        && mech.Equals("oauthbearer", StringComparison.OrdinalIgnoreCase))
                    {
                        config.SaslMechanism = SaslMechanism.OAuthBearer;

                        // Only set OIDC if token endpoint URL is supplied
                        if (saslFromProvider.ContainsKey("sasl.oauthbearer.token.endpoint.url"))
                        {
                            config.SaslOauthbearerMethod = SaslOauthbearerMethod.Oidc;
                        }
                    }
                }
            }
            else
            {
                // None: do not apply OAuth even if globally configured.
                _oauthRefreshHandlerEnabled = false;
            }

            return config;
        }

        private static void ApplyConfigDictionary(ClientConfig config, IDictionary<string, string>? configDictionary)
        {
            if (configDictionary is null)
            {
                return;
            }

            foreach (KeyValuePair<string, string> kvp in configDictionary)
            {
                if (!string.IsNullOrWhiteSpace(kvp.Value))
                {
                    config.Set(kvp.Key, kvp.Value);
                }
            }
        }

        /// <summary>
        /// Subscribes to the specified topics.
        /// </summary>
        /// <param name="topics">The topics to subscribe to.</param>
        public void Subscribe(IEnumerable<string> topics)
        {
            ThrowIfDisposed();
            if (topics == null)
            {
                throw new ArgumentNullException(nameof(topics));
            }

            List<string> topicsList = topics.ToList();
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
            ThrowIfDisposed();
            try
            {
                // bounded poll with cancellation
                TimeSpan timeout = TimeSpan.FromSeconds(5);
                ConsumeResult<string, byte[]>? result = null;

                CancellationTokenSource source = CancellationTokenSource.CreateLinkedTokenSource(ct);
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
                byte[]? bytes = result.Message.Value;
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
                    Models.SchemaType schemaType = await DetermineSchemaTypeAsync(bytes, result.Topic);
                    
                    // Use appropriate deserializer
                    IMessageDeserializer<T> deserializer = _deserializerFactory.Create<T>(schemaType);
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
                TimeSpan timeout = TimeSpan.FromSeconds(5);
                ConsumeResult<string, byte[]>? result = null;

                // Try to consume with cancellation token support
                CancellationTokenSource source = CancellationTokenSource.CreateLinkedTokenSource(ct);
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
                byte[]? bytes = result.Message.Value;
                if (bytes is null)
                {
                    // tombstone
                    return null;
                }

                // Determine schema type and create deserializer
                Models.SchemaType schemaType = await DetermineSchemaTypeAsync(bytes, result.Topic);
                IMessageDeserializer<T> deserializer = _deserializerFactory.Create<T>(schemaType);

                // Deserialize the bytes
                T value = await deserializer.DeserializeAsync(
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
            ThrowIfDisposed();
            // Simplified: only generic/POCO and raw types supported. Avro GenericRecord/SpecificRecord paths removed.
            return await ConsumeBatchWithDeserializer<T>(maxMessages, timeoutMs, ct);
        }

        /// <summary>
        /// Consumes a batch of messages with a specific deserializer.
        /// </summary>
        private async Task<List<ConsumeResult<string, T>>> ConsumeBatchWithDeserializer<T>(int maxMessages, int timeoutMs, CancellationToken ct)
        {
            List<ConsumeResult<string, T>> results = new List<ConsumeResult<string, T>>();
            System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();

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
                    int remainingTimeout = Math.Max(1, timeoutMs - (int)stopwatch.ElapsedMilliseconds);
                    int perIterationTimeoutMs = Math.Min(1000, Math.Max(200, remainingTimeout)); // 200ms-1s window
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
                    string? keyStr = result.Message?.Key;
                    //
                    string headersStr = FormatHeaders(result.Message?.Headers);
                    //
                    int valLen = result.Message?.Value?.Length ?? 0;
                    //
                    string preview = result.Message?.Value is null ? "null" : PreviewBytes(result.Message.Value, 64);
                    //
                    // Message is guaranteed non-null here due to early guard above
                    Message<string, byte[]> msg = result.Message!;

                    //
                    // Get the raw bytes
                    byte[]? bytes = msg.Value;
                    if (bytes is null)
                    {
                        // tombstone
                        continue;
                    }
                    
                    // Determine schema type and create deserializer
                    Models.SchemaType schemaType = await DetermineSchemaTypeAsync(bytes, result.Topic);
                    IMessageDeserializer<T> deserializer = _deserializerFactory.Create<T>(schemaType);
                    
                    //                    
                    // Deserialize the bytes
                    T value = await deserializer.DeserializeAsync(
                        bytes,
                        new SerializationContext(MessageComponentType.Value, result.Topic));
                    //
                    try
                    {
                        string formatted = FormatValue(value, 2000);
                        //
                    }
                    catch { }
                    ConsumeResult<string, T> typedResult = new ConsumeResult<string, T>
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
            ThrowIfDisposed();
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
            ThrowIfDisposed();
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
            ThrowIfDisposed();
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
            ThrowIfDisposed();
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
        public List<TopicPartition> Assignment
        {
            get
            {
                ThrowIfDisposed();
                return _consumer.Assignment;
            }
        }

        /// <summary>
        /// Gets the current consumer subscription (list of subscribed topics).
        /// </summary>
        public List<string> Subscription
        {
            get
            {
                ThrowIfDisposed();
                return _consumer.Subscription;
            }
        }

        /// <summary>
        /// Disposes the consumer resources.
        /// </summary>
        public void Dispose()
        {
            if (System.Threading.Interlocked.Exchange(ref _disposed, 1) != 0)
            {
                return;
            }

            try
            {
                _consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error during consumer close");
            }

            try
            {
                _consumer.Dispose();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error during consumer dispose");
            }

            try
            {
                if (_ownsSchemaRegistry)
                {
                    _schemaRegistry.Dispose();
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error during schema registry dispose");
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
                List<TopicPartition> assignment = _consumer?.Assignment ?? new List<TopicPartition>();
                if (assignment.Count == 0)
                {
                    // No partitions assigned yet
                    return;
                }

                foreach (TopicPartition tp in assignment)
                {
                    Offset pos = _consumer!.Position(tp);
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
            {
                return string.Empty;
            }
            try
            {
                return string.Join(", ", headers.Select(h =>
                {
                    byte[]? pv = h.GetValueBytes();
                    string prev = pv is null ? "null" : PreviewBytes(pv, 16);
                    return $"{h.Key}={prev}";
                }));
            }
            catch { return "<headers-error>"; }
        }

        private static string PreviewBytes(byte[]? data, int max)
        {
            if (data is null || data.Length == 0)
            {
                return string.Empty;
            }

            int len = Math.Min(max, data.Length);
            string hex = Convert.ToHexString(data.AsSpan(0, len));
            return data.Length > max ? hex + "..." : hex;
        }

        private static string FormatValue(object? value, int maxChars)
        {
            if (value is null)
            {
                return "null";
            }
            try
            {
                string s;
                try { s = System.Text.Json.JsonSerializer.Serialize(value); }
                catch { s = value.ToString() ?? value.GetType().Name; }
                if (s.Length > maxChars)
                {
                    s = s.Substring(0, maxChars) + "...";
                }
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
            if (_kafkaConsumerOpts.TopicSchemaTypes.TryGetValue(topic, out Models.SchemaType overrideType))
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