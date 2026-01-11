// KafkaProducerClient unit tests
using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Services;
using JohBloch.ConfluentKafka.Clients.Services.Serialization;
using JohBloch.ConfluentKafka.Clients.Security;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace JohBloch.ConfluentKafka.Clients.Tests;

/// <summary>
/// Unit tests for <see cref="KafkaProducerClient"/> covering success, failure, batch, and config helper paths.
/// </summary>
public class KafkaProducerClientTests
{
    private sealed class FakeTokenProvider : ISecurityTokenProvider
    {
        public Task<AccessToken> GetAccessTokenAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new AccessToken("fake-token", DateTimeOffset.UtcNow.AddMinutes(5)));
        public TokenStatus GetTokenStatus() => new TokenStatus(DateTime.UtcNow.AddMinutes(5));
        public Dictionary<string, string>? GetExtensions() => new();
        public Dictionary<string, string>? GetKafkaSaslConfig() => new() { { "sasl.mechanism", "OAUTHBEARER" } };
    }

    private sealed class FakeSchemaRegistryFactory : ISchemaRegistryFactory
    {
        public ISchemaRegistryClient CreateClient()
        {
            var cfg = new SchemaRegistryConfig { Url = "http://localhost:8081" };
            return new CachedSchemaRegistryClient(cfg);
        }
    }

    private KafkaProducerClient CreateProducerClient(KafkaProducerOptions? opts = null)
    {
        opts ??= new KafkaProducerOptions
        {
            BootstrapServers = "localhost:9092",
            ApplicationId = "app-id",
            Topic = "test-topic",
            BatchSizeKB = 32,
            QueueBufferMaxMessages = 10000,
            CompressionType = "gzip"
        };
        var dict = new Dictionary<string, KafkaProducerOptions> { { "default", opts } };
        return new KafkaProducerClient(dict, new FakeTokenProvider(), new FakeSchemaRegistryFactory(), NullLogger<KafkaProducerClient>.Instance);
    }

    /// <summary>
    /// SendMessageAsync: success path produces one message and returns KafkaResult with metadata.
    /// </summary>
    [Fact]
    public async Task SendMessageAsync_Success()
    {
        var client = CreateProducerClient();
        var fake = new ConfigurableFakeProducer<byte[]>
        {
            OnProduceAsyncExt = (topic, msg, ct) => Task.FromResult(new DeliveryResult<string, byte[]>
            {
                Topic = "topic-1",
                Partition = new Partition(0),
                Offset = new Offset(1),
                Message = msg,
                Key = msg.Key
            })
        };
        SeedProducer(client, "default", false, fake);
        var res = await client.SendMessageAsync(new byte[] { 1, 2 }, key: "k1", producerKey: "default");
        Assert.True(res.Success);
        Assert.Equal("topic-1", res.Topic);
        Assert.Equal("k1", res.Key);
    }

    /// <summary>
    /// SendMessageAsync: ProduceException returns failure with error reason.
    /// </summary>
    [Fact]
    public async Task SendMessageAsync_ProduceException_Fails()
    {
        var client = CreateProducerClient();
        var fake = new ConfigurableFakeProducer<byte[]>
        {
            OnProduceAsyncExt = (topic, msg, ct) => Task.FromException<DeliveryResult<string, byte[]>>(new ProduceException<string, byte[]>(new Error(ErrorCode.BrokerNotAvailable), new DeliveryResult<string, byte[]>() { Topic = topic }))
        };
        SeedProducer(client, "default", false, fake);
        var res = await client.SendMessageAsync(new byte[] { 1 }, key: "k", producerKey: "default");
        Assert.False(res.Success);
        Assert.False(string.IsNullOrEmpty(res.ErrorMessage));
    }

    /// <summary>
    /// SendMessageAsync: general exception returns failure and logs.
    /// </summary>
    [Fact]
    public async Task SendMessageAsync_GeneralException_Fails()
    {
        var client = CreateProducerClient();
        var fake = new ConfigurableFakeProducer<byte[]>
        {
            OnProduceAsyncExt = (topic, msg, ct) => Task.FromException<DeliveryResult<string, byte[]>>(new InvalidOperationException("boom"))
        };
        SeedProducer(client, "default", false, fake);
        var res = await client.SendMessageAsync(new byte[] { 1 }, key: "k", producerKey: "default");
        Assert.False(res.Success);
        Assert.NotNull(res.ErrorMessage);
    }

    /// <summary>
    /// SendBatchAsync: empty input returns success with zero counts.
    /// </summary>
    [Fact]
    public async Task SendBatchAsync_Empty_ReturnsEmptySuccess()
    {
        var client = CreateProducerClient();
        var res = await client.SendBatchAsync(Array.Empty<byte[]>(), _ => "k", "default");
        Assert.Equal(0, res.TotalMessages);
        Assert.Equal(0, res.SuccessCount);
        Assert.Equal(0, res.FailureCount);
    }

    /// <summary>
    /// SendBatchAsync: all success, batch-id header present.
    /// </summary>
    [Fact]
    public async Task SendBatchAsync_AllSuccess_FlushCalled()
    {
        var client = CreateProducerClient();
        var fake = new ConfigurableFakeProducer<byte[]>
        {
            OnProduceAsync = (msg) => Task.FromResult(new DeliveryResult<string, byte[]>
            {
                Topic = "test-topic",
                Partition = new Partition(1),
                Offset = new Offset(5),
                Message = msg,
                Key = msg.Key
            })
        };
        SeedProducer(client, "default", true, fake);
        var msgs = new[] { new byte[] { 1 }, new byte[] { 2 } };
        var res = await client.SendBatchAsync(msgs, m => m[0].ToString(), "default");
        Assert.Equal(2, res.TotalMessages);
        Assert.Equal(2, res.SuccessCount);
        Assert.Equal(0, res.FailureCount);
    }

    /// <summary>
    /// SendBatchAsync: mixed failures produce exception from fake producer.
    /// </summary>
    [Fact]
    public async Task SendBatchAsync_MixedFailuresAndCanceled()
    {
        var client = CreateProducerClient();
        var failing = new ConfigurableFakeProducer<byte[]> { OnProduceAsync = (msg) => Task.FromException<DeliveryResult<string, byte[]>>(new ProduceException<string, byte[]>(new Error(ErrorCode.BrokerNotAvailable), new DeliveryResult<string, byte[]>() { Topic = "t" })) };
        SeedProducer(client, "default", true, failing);
        var msgs = new[] { new byte[] { 1 }, new byte[] { 2 }, new byte[] { 3 } };
        var res = await client.SendBatchAsync(msgs, m => m[0].ToString(), "default");
        Assert.Equal(msgs.Length, res.TotalMessages);
        Assert.Equal(0, res.SuccessCount);
        Assert.Equal(msgs.Length, res.FailureCount);
    }

    /// <summary>
    /// KafkaConfigHelper.CreateBaseConfig applies SASL and basic fields.
    /// </summary>
    [Fact]
    public void KafkaConfigHelper_CreateBaseConfig_AppliesSasl()
    {
        var opts = new KafkaProducerOptions { BootstrapServers = "b:9092", ApplicationId = "app" };
        var sasl = new Dictionary<string, string> { { "sasl.mechanism", "OAUTHBEARER" }, { "sasl.oauthbearer.method", "oidc" } };
        var cfg = KafkaProducerClient.KafkaConfigHelper.CreateBaseConfig(opts, sasl);
        Assert.Equal("b:9092", cfg.BootstrapServers);
        Assert.Equal(SecurityProtocol.SaslSsl, cfg.SecurityProtocol);
        Assert.Equal(SaslMechanism.OAuthBearer, cfg.SaslMechanism);
        Assert.Equal("app", cfg.ClientId);
        Assert.Equal("oidc", cfg.Get("sasl.oauthbearer.method"));
    }

    /// <summary>
    /// KafkaConfigHelper.CreateBaseConfig applies SASL OAUTHBEARER settings.
    /// </summary>
    [Fact]
    public void KafkaConfigHelper_CreateBaseConfig_AppliesSaslOAuthBearer()
    {
        var opts = new KafkaProducerOptions { BootstrapServers = "localhost:9092", ApplicationId = "app-id" };
        var sasl = new Dictionary<string, string> { { "sasl.mechanism", "OAUTHBEARER" }, { "sasl.oauthbearer.config", "scope=foo" } };
        var cfg = KafkaProducerClient.KafkaConfigHelper.CreateBaseConfig(opts, sasl);
        Assert.Equal("localhost:9092", cfg.BootstrapServers);
        Assert.Equal(SecurityProtocol.SaslSsl, cfg.SecurityProtocol);
        Assert.Equal(SaslMechanism.OAuthBearer, cfg.SaslMechanism);
        Assert.Equal("app-id", cfg.ClientId);
        Assert.Equal("OAUTHBEARER", cfg.Get("sasl.mechanism"));
        Assert.Equal("scope=foo", cfg.Get("sasl.oauthbearer.config"));
    }

    /// <summary>
    /// KafkaConfigHelper.ApplyBatchOptimizedSettings sets expected values.
    /// </summary>
    [Fact]
    public void KafkaConfigHelper_ApplyBatchOptimizedSettings_SetsValues()
    {
        var cfg = new ProducerConfig();
        var opts = new KafkaProducerOptions { BatchSizeKB = 2, QueueBufferMaxMessages = 123, CompressionType = "gzip" };
        KafkaProducerClient.KafkaConfigHelper.ApplyBatchOptimizedSettings(cfg, opts);
        Assert.Equal(2 * 1024, cfg.BatchSize);
        Assert.Equal(100, cfg.LingerMs);
        Assert.Equal(123, cfg.QueueBufferingMaxMessages);
        Assert.Equal(CompressionType.Gzip, cfg.CompressionType);
        Assert.True(cfg.EnableIdempotence);
        Assert.Equal(Acks.All, cfg.Acks);
        Assert.Equal(3, cfg.MessageSendMaxRetries);
        Assert.Equal(10000, cfg.RequestTimeoutMs);
        Assert.Equal(30000, cfg.MessageTimeoutMs);
    }

    private sealed class ConfigurableFakeProducer<T> : IProducer<string, T>
    {
        public Func<string, Message<string, T>, CancellationToken, Task<DeliveryResult<string, T>>>? OnProduceAsyncExt { get; set; }
        public Func<Message<string, T>, Task<DeliveryResult<string, T>>>? OnProduceAsync { get; set; }
        public int FlushCalled { get; private set; }
        public string Name => "ConfigurableFakeProducer";
        public Handle Handle => null!;

        public Task<DeliveryResult<string, T>> ProduceAsync(string topic, Message<string, T> message, CancellationToken cancellationToken = default)
        {
            if (OnProduceAsyncExt != null) return OnProduceAsyncExt(topic, message, cancellationToken);
            if (OnProduceAsync != null) return OnProduceAsync(message);
            return Task.FromResult(new DeliveryResult<string, T>
            {
                Topic = topic,
                Partition = new Partition(0),
                Offset = new Offset(1),
                Message = message,
                Key = message.Key
            });
        }

        public Task<DeliveryResult<string, T>> ProduceAsync(TopicPartition topicPartition, Message<string, T> message, CancellationToken cancellationToken = default)
            => ProduceAsync(topicPartition.Topic, message, cancellationToken);

        public void Produce(string topic, Message<string, T> message, Action<DeliveryReport<string, T>> deliveryHandler)
        {
            var dr = new DeliveryReport<string, T>
            {
                TopicPartitionOffset = new TopicPartitionOffset(new TopicPartition(topic, new Partition(0)), new Offset(1)),
                Message = message,
                Status = PersistenceStatus.Persisted
            };
            deliveryHandler?.Invoke(dr);
        }

        public void Produce(TopicPartition topicPartition, Message<string, T> message, Action<DeliveryReport<string, T>> deliveryHandler)
            => Produce(topicPartition.Topic, message, deliveryHandler);

        public int Flush(TimeSpan timeout) { FlushCalled++; return 0; }
        public void Flush(CancellationToken cancellationToken = default) { FlushCalled++; }
        public int Poll(TimeSpan timeout) { return 0; }
        public int AddBrokers(string brokers) { return 0; }
        public void SetSaslCredentials(string username, string password) { }
        public void InitTransactions(TimeSpan timeout) { }
        public void BeginTransaction() { }
        public void CommitTransaction() { }
        public void CommitTransaction(TimeSpan timeout) { }
        public void AbortTransaction() { }
        public void AbortTransaction(TimeSpan timeout) { }
        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout) { }
        public void Dispose() { }
    }

    private static void SeedProducer<K>(KafkaProducerClient client, string producerKey, bool batch, IProducer<string, K> producer, ISerializer<K>? serializer = null)
    {
        var field = typeof(KafkaProducerClient).GetField("_producers", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(field);
        var dict = (System.Collections.Concurrent.ConcurrentDictionary<(string ProducerKey, Type Type, bool Batch), object>)field!.GetValue(client)!;
        var serializerKey = serializer?.GetType().FullName ?? string.Empty;
        dict[(producerKey + serializerKey, typeof(K), batch)] = producer;
    }

    /// <summary>
    /// SendMessageAsync maps provided headers into the produced message.
    /// </summary>
    [Fact]
    public async Task SendMessageAsync_MapsHeaders()
    {
        var client = CreateProducerClient();
        var capturedHeaders = (Headers?)null;
        var fake = new ConfigurableFakeProducer<byte[]>
        {
            OnProduceAsync = (msg) =>
            {
                capturedHeaders = msg.Headers;
                return Task.FromResult(new DeliveryResult<string, byte[]>
                {
                    Topic = "test-topic",
                    Partition = new Partition(1),
                    Offset = new Offset(5),
                    Message = msg
                });
            }
        };
        var headers = new Headers { new Header("h1", new byte[] { 1, 2 }), new Header("h2", new byte[] { 3 }) };
        var result = await InvokeProduceMessageAsync(fake, new byte[] { 9 }, "k", "test-topic", headers);
        Assert.True(result.Success);
        Assert.NotNull(capturedHeaders);
        Assert.Equal(2, capturedHeaders!.Count);
        Assert.Equal(new byte[] { 1, 2 }, capturedHeaders.GetLastBytes("h1"));
        Assert.Equal(new byte[] { 3 }, capturedHeaders.GetLastBytes("h2"));
    }

    /// <summary>
    /// SendMessageAsync respects a pre-cancelled token and throws OperationCanceledException.
    /// </summary>
    [Fact]
    public async Task SendMessageAsync_CancelledToken_Throws()
    {
        var client = CreateProducerClient();
        var producer = new ConfigurableFakeProducer<byte[]> { OnProduceAsyncExt = (topic, msg, ct) => Task.FromCanceled<DeliveryResult<string, byte[]>>(ct) };
        SeedProducer(client, producerKey: "default", batch: false, producer);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        // Client returns failure result instead of throwing
        var res = await client.SendMessageAsync<byte[]>(new byte[] { 1 }, "k", "default", headers: null, serializer: null, cts.Token);
        Assert.False(res.Success);
    }

    /// <summary>
    /// SendMessageAsync surfaces ProduceException when delivery result contains BrokerNotAvailable error.
    /// </summary>
    [Fact]
    public async Task SendMessageAsync_BrokerNotAvailable_ThrowsProduceException()
    {
        var client = CreateProducerClient();
        var producer = new ConfigurableFakeProducer<byte[]> { OnProduceAsync = (msg) => Task.FromException<DeliveryResult<string, byte[]>>(new ProduceException<string, byte[]>(new Error(ErrorCode.BrokerNotAvailable), new DeliveryResult<string, byte[]>() { Topic = "t" })) };
        SeedProducer(client, producerKey: "default", batch: false, producer);
        var res = await client.SendMessageAsync<byte[]>(new byte[] { 1 }, "k", "default");
        Assert.False(res.Success);
        Assert.Contains("Broker not available", res.ErrorMessage!, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// SendBatchAsync with null input throws ArgumentNullException.
    /// </summary>
    [Fact]
    public async Task SendBatchAsync_NullInput_Throws()
    {
        var client = CreateProducerClient();
        await Assert.ThrowsAsync<ArgumentNullException>(async () =>
            await client.SendBatchAsync<byte[]>(null!, _ => "k", producerKey: "default"));
    }

    /// <summary>
    /// SendBatchAsync mixed successes and failures returns correct counts.
    /// </summary>
    [Fact]
    public async Task SendBatchAsync_MixedSuccessFailure_Counts()
    {
        var client = CreateProducerClient();
        var producer = new ConfigurableFakeProducer<byte[]>();
        int i = 0;
        producer.OnProduceAsync = (msg) =>
        {
            if (i++ % 2 == 1)
            {
                return Task.FromException<DeliveryResult<string, byte[]>>(new ProduceException<string, byte[]>(new Error(ErrorCode.BrokerNotAvailable), new DeliveryResult<string, byte[]>() { Topic = "t" }));
            }
            return Task.FromResult(new DeliveryResult<string, byte[]>
            {
                Topic = "test-topic",
                Partition = new Partition(0),
                Offset = new Offset(i),
                Message = msg,
                Key = msg.Key
            });
        };
        SeedProducer(client, producerKey: "default", batch: true, producer);
        var payloads = Enumerable.Range(0, 10).Select(i => i % 2 == 0 ? new byte[] { 1 } : new byte[] { 2 }).ToList();
        var result = await client.SendBatchAsync<byte[]>(payloads, _ => "k", producerKey: "default");
        Assert.Equal(5, result.SuccessCount);
        Assert.Equal(5, result.FailureCount);
    }

    /// <summary>
    /// SendBatchAsync large batch exercises state machine branches; returns aggregated counts.
    /// </summary>
    [Fact]
    public async Task SendBatchAsync_LargeBatch_ExercisesStateMachine_Basic()
    {
        var client = CreateProducerClient();
        var producer = new ConfigurableFakeProducer<byte[]>
        {
            OnProduceAsync = (msg) => Task.FromResult(new DeliveryResult<string, byte[]>
            {
                Topic = "test-topic",
                Partition = new Partition(0),
                Offset = new Offset(1),
                Message = msg,
                Key = msg.Key
            })
        };
        SeedProducer(client, producerKey: "default", batch: true, producer);
        var payloads = Enumerable.Range(0, 100).Select(i => new byte[] { (byte)(i % 3) }).ToList();
        var result = await client.SendBatchAsync<byte[]>(payloads, _ => "k", producerKey: "default");
        Assert.Equal(100, result.SuccessCount + result.FailureCount);
    }

    /// <summary>
    /// ProcessBatchTasks aggregates success, failure, and cancelled tasks correctly.
    /// </summary>
    [Fact]
    public async Task ProcessBatchTasks_Aggregates_AllStates()
    {
        var client = CreateProducerClient();
        var topic = "test-topic";
        var success = Task.FromResult(new DeliveryResult<string, byte[]>
        {
            Topic = topic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, byte[]> { Key = "k1", Value = new byte[] { 1 } }
        });
        var failure = Task.FromException<DeliveryResult<string, byte[]>>(new ProduceException<string, byte[]>(new Error(ErrorCode.BrokerNotAvailable), new DeliveryResult<string, byte[]>() { Topic = topic }));
        var cts = new CancellationTokenSource(); cts.Cancel();
        var canceled = Task.FromCanceled<DeliveryResult<string, byte[]>>(cts.Token);

        var mi = typeof(KafkaProducerClient).GetMethod("ProcessBatchTasks", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);
        var batch = new BatchResult(3);
        var tasks = new List<Task<DeliveryResult<string, byte[]>>> { success, failure, canceled };
        await (Task)mi!.MakeGenericMethod(typeof(byte[])).Invoke(client, new object[] { tasks, batch, "bid", CancellationToken.None })!;
        Assert.Equal(1, batch.SuccessCount);
        Assert.Equal(2, batch.FailureCount);
    }

    private static async Task<KafkaResult> InvokeProduceMessageAsync<T>(ConfigurableFakeProducer<T> prod, T value, string key, string topic, Headers? headers = null, CancellationToken ct = default)
    {
        var client = new KafkaProducerClient(new Dictionary<string, KafkaProducerOptions> { { "default", new KafkaProducerOptions { BootstrapServers = "localhost:9092", ApplicationId = "app-id", Topic = topic } } }, new FakeTokenProvider(), new FakeSchemaRegistryFactory(), NullLogger<KafkaProducerClient>.Instance);
        var mi = typeof(KafkaProducerClient).GetMethod("ProduceMessageAsync", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);
        var task = (Task<KafkaResult>)mi.MakeGenericMethod(typeof(T)).Invoke(client, new object?[] { prod, value, key, topic, headers, ct })!;
        return await task;
    }

    /// <summary>
    /// SendMessageAsync cancelled token returns KafkaResult failure from helper.
    /// </summary>
    [Fact]
    public async Task SendMessageAsync_CancelledToken_ReturnsFailure()
    {
        var cts = new CancellationTokenSource();
        cts.Cancel();
        var fake = new ConfigurableFakeProducer<byte[]>
        {
            OnProduceAsync = (msg) => Task.FromCanceled<DeliveryResult<string, byte[]>>(cts.Token)
        };
        var result = await InvokeProduceMessageAsync(fake, new byte[] { 9 }, "k", "test-topic", null, cts.Token);
        Assert.False(result.Success);
        Assert.False(string.IsNullOrEmpty(result.ErrorMessage));
    }

    /// <summary>
    /// SendMessageAsync BrokerNotAvailable returns failure result from helper path.
    /// </summary>
    [Fact]
    public async Task SendMessageAsync_BrokerNotAvailable_ReturnsProduceError()
    {
        var fake = new ConfigurableFakeProducer<byte[]>
        {
            OnProduceAsync = (msg) => Task.FromException<DeliveryResult<string, byte[]>>(new ProduceException<string, byte[]>(new Error(ErrorCode.BrokerNotAvailable), new DeliveryResult<string, byte[]>() { Topic = "t" }))
        };
        var result = await InvokeProduceMessageAsync(fake, new byte[] { 1 }, "k", "test-topic");
        Assert.False(result.Success);
    }

    /// <summary>
    /// SendBatchAsync mixed success and failures returns correct counts via configurable fake.
    /// </summary>
    [Fact]
    public async Task SendBatchAsync_MixedSuccessAndFailures_ReturnsCorrectCounts()
    {
        var client = CreateProducerClient();
        var fake = new ConfigurableFakeProducer<byte[]>();
        int i = 0;
        fake.OnProduceAsync = (msg) =>
        {
            if (i++ % 3 == 0)
            {
                return Task.FromException<DeliveryResult<string, byte[]>>(new ProduceException<string, byte[]>(new Error(ErrorCode.BrokerNotAvailable), new DeliveryResult<string, byte[]>() { Topic = "t" }));
            }
            return Task.FromResult(new DeliveryResult<string, byte[]>
            {
                Topic = "test-topic",
                Partition = new Partition(0),
                Offset = new Offset(i),
                Message = msg,
                Key = msg.Key
            });
        };

        var getProducer = typeof(KafkaProducerClient).GetMethod("GetProducer", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        var producer = (IProducer<string, byte[]>)getProducer!.MakeGenericMethod(typeof(byte[])).Invoke(client, new object[] { "default", true, (ISerializer<byte[]>)null! })!;
        var dictField = typeof(KafkaProducerClient).GetField("_producers", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        var dict = (System.Collections.Concurrent.ConcurrentDictionary<(string, Type, bool), object>)dictField!.GetValue(client)!;
        dict[("default", typeof(byte[]), true)] = fake;

        var payloads = Enumerable.Range(0, 9).Select(n => new byte[] { (byte)n }).ToArray();
        var res = await client.SendBatchAsync(payloads, _ => "k", "default");
        Assert.Equal(payloads.Length, res.TotalMessages);
        Assert.True(res.SuccessCount > 0);
        Assert.True(res.FailureCount > 0);
    }

    /// <summary>
    /// SendBatchAsync null input throws ArgumentNullException via API with keySelector.
    /// </summary>
    [Fact]
    public async Task SendBatchAsync_NullInput_ThrowsArgumentNullException()
    {
        var client = CreateProducerClient();
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SendBatchAsync<byte[]>(null!, _ => "k", "default"));
    }

    /// <summary>
    /// SendBatchAsync large batch exercises state machine via configurable producer.
    /// </summary>
    [Fact]
    public async Task SendBatchAsync_LargeBatch_ExercisesStateMachine_Configurable()
    {
        var client = CreateProducerClient();
        var fake = new ConfigurableFakeProducer<byte[]>();
        int i = 0;
        fake.OnProduceAsync = async (msg) =>
        {
            await Task.Delay(1);
            return new DeliveryResult<string, byte[]>
            {
                Topic = "test-topic",
                Partition = new Partition(0),
                Offset = new Offset(i++),
                Message = msg,
                Key = msg.Key
            };
        };
        var getProducer = typeof(KafkaProducerClient).GetMethod("GetProducer", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        var producer = (IProducer<string, byte[]>)getProducer!.MakeGenericMethod(typeof(byte[])).Invoke(client, new object[] { "default", true, (ISerializer<byte[]>)null! })!;
        var dictField = typeof(KafkaProducerClient).GetField("_producers", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        var dict = (System.Collections.Concurrent.ConcurrentDictionary<(string, Type, bool), object>)dictField!.GetValue(client)!;
        dict[("default", typeof(byte[]), true)] = fake;

        var payloads = Enumerable.Range(0, 120).Select(n => new byte[] { (byte)n }).ToArray();
        var res = await client.SendBatchAsync(payloads, _ => "k", "default");
        Assert.Equal(payloads.Length, res.TotalMessages);
        Assert.True(res.SuccessCount > 0);
    }

    /// <summary>
    /// ProcessBatchTasks aggregates success, failure, canceled correctly.
    /// </summary>
    [Fact]
    public async Task ProcessBatchTasks_Aggregates_Success_Failure_Canceled()
    {
        var client = CreateProducerClient();
        var topic = "test-topic";
        var success = Task.FromResult(new DeliveryResult<string, byte[]>
        {
            Topic = topic,
            Partition = new Partition(0),
            Offset = new Offset(1),
            Message = new Message<string, byte[]> { Key = "k1", Value = new byte[] { 1 } }
        });
        var failure = Task.FromException<DeliveryResult<string, byte[]>>(new ProduceException<string, byte[]>(new Error(ErrorCode.BrokerNotAvailable), new DeliveryResult<string, byte[]>() { Topic = topic }));
        var cts = new CancellationTokenSource(); cts.Cancel();
        var canceled = Task.FromCanceled<DeliveryResult<string, byte[]>>(cts.Token);
        var otherEx = Task.FromException<DeliveryResult<string, byte[]>>(new InvalidOperationException("boom"));

        var tasks = new List<Task<DeliveryResult<string, byte[]>>> { success, failure, canceled, otherEx };
        var batch = new BatchResult(tasks.Count);
        var mi = typeof(KafkaProducerClient).GetMethod("ProcessBatchTasks", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        await (Task)mi!.MakeGenericMethod(typeof(byte[])).Invoke(client, new object[] { tasks, batch, "bid", CancellationToken.None })!;

        Assert.Equal(1, batch.SuccessCount);
        Assert.Equal(3, batch.FailureCount);
    }
}
