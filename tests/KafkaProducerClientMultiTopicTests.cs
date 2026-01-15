using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Security;
using JohBloch.ConfluentKafka.Clients.Services;
using JohBloch.ConfluentKafka.Clients.Services.Serialization;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace JohBloch.ConfluentKafka.Clients.Tests
{
    /// <summary>
    /// Fake in-memory producer used to capture produced messages during tests.
    /// </summary>
    internal sealed class FakeProducer<T> : IProducer<string, T>
    {
        /// <summary>
        /// Gets the list of messages produced by this fake producer along with the target topic.
        /// </summary>
        public readonly List<(string Topic, Message<string, T> Message)> Produced = new();
        /// <summary>Gets the client name.</summary>
        public string Name => "fake";
        /// <summary>Gets the underlying client handle. Not used in tests.</summary>
        public Handle Handle => null!;
        /// <summary>Disposes resources. No-op in tests.</summary>
        public void Dispose() { }
        /// <summary>Polls the client. No-op in tests.</summary>
        public int Poll(TimeSpan timeout) => 0;
        /// <summary>Flush with timeout. No-op in tests.</summary>
        public int Flush(TimeSpan timeout) => 0;
        /// <summary>Flush with cancellation. No-op in tests.</summary>
        public void Flush(CancellationToken cancellationToken = default) { }
        /// <summary>Not implemented for tests.</summary>
        public void Produce(string topic, Message<string, T> message, Action<DeliveryReport<string, T>> deliveryHandler = null!)
            => throw new NotImplementedException();
        /// <summary>Not implemented for tests.</summary>
        public void Produce(TopicPartition topicPartition, Message<string, T> message, Action<DeliveryReport<string, T>> deliveryHandler = null!)
            => throw new NotImplementedException();
        /// <summary>
        /// Records a produced message and returns a successful delivery result.
        /// </summary>
        public Task<DeliveryResult<string, T>> ProduceAsync(string topic, Message<string, T> message, CancellationToken cancellationToken = default)
        {
            Produced.Add((topic, message));
            var dr = new DeliveryResult<string, T>
            {
                Topic = topic,
                Partition = new Partition(0),
                Offset = new Offset(Produced.Count - 1),
                Message = message
            };
            return Task.FromResult(dr);
        }
        /// <summary>
        /// Records a produced message using topic-partition and returns a successful delivery result.
        /// </summary>
        public Task<DeliveryResult<string, T>> ProduceAsync(TopicPartition topicPartition, Message<string, T> message, CancellationToken cancellationToken = default)
        {
            return ProduceAsync(topicPartition.Topic, message, cancellationToken);
        }
        /// <summary>Initializes transactions. No-op in tests.</summary>
        public void InitTransactions(TimeSpan timeout) { }
        /// <summary>Begins a transaction. No-op in tests.</summary>
        public void BeginTransaction() { }
        /// <summary>Commits a transaction with timeout. No-op in tests.</summary>
        public void CommitTransaction(TimeSpan timeout) { }
        /// <summary>Commits a transaction. No-op in tests.</summary>
        public void CommitTransaction() { }
        /// <summary>Aborts a transaction with timeout. No-op in tests.</summary>
        public void AbortTransaction(TimeSpan timeout) { }
        /// <summary>Aborts a transaction. No-op in tests.</summary>
        public void AbortTransaction() { }
        /// <summary>Sends offsets to a transaction. No-op in tests.</summary>
        public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout) { }
        /// <summary>Adds brokers. No-op in tests.</summary>
        public int AddBrokers(string brokers) => 0;
        /// <summary>Sets SASL credentials. No-op in tests.</summary>
        public void SetSaslCredentials(string username, string password) { }
    }

    /// <summary>
    /// Security provider stub returning a short-lived token and empty SASL config for tests.
    /// </summary>
    internal sealed class SecStub : ISecurityTokenProvider
    {
        /// <summary>Returns a dummy access token.</summary>
        public Task<AccessToken> GetAccessTokenAsync(CancellationToken cancellationToken = default) => Task.FromResult(new AccessToken("token", DateTimeOffset.UtcNow.AddMinutes(5)));
        /// <summary>Returns token status with a future expiry.</summary>
        public TokenStatus GetTokenStatus() => new TokenStatus(DateTime.UtcNow.AddMinutes(5));
        /// <summary>Returns an empty extensions dictionary.</summary>
        public Dictionary<string, string>? GetExtensions() => new();
        /// <summary>Returns an empty SASL config to avoid OIDC requirements in tests.</summary>
        public Dictionary<string, string>? GetKafkaSaslConfig() => new();
    }

    /// <summary>
    /// Schema registry factory stub creating a client pointed at localhost.
    /// </summary>
    internal sealed class SrStub : ISchemaRegistryFactory
    {
        private readonly Action<IDisposable>? _track;

        public SrStub(Action<IDisposable>? track = null)
        {
            _track = track;
        }

        /// <summary>Creates a cached schema registry client.</summary>
        public ISchemaRegistryClient CreateClient()
        {
            var client = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = "http://localhost:8081" });
            _track?.Invoke(client);
            return client;
        }
    }

    /// <summary>
    /// Tests verifying routing to default, retry, and DLQ topics for single and batch sends.
    /// </summary>
    public class KafkaProducerClientMultiTopicTests : DisposableTestBase
    {
        /// <summary>
        /// Creates a producer client configured with three logical producer keys: default, retry, and dlq.
        /// </summary>
        private KafkaProducerClient CreateClient()
        {
            var opts = new Dictionary<string, KafkaProducerOptions>
            {
                ["default"] = new KafkaProducerOptions { BootstrapServers = "localhost:9092", ApplicationId = "app", Topic = "topic-a", BatchSizeKB = 1, QueueBufferMaxMessages = 1000, CompressionType = "none" },
                ["retry"]   = new KafkaProducerOptions { BootstrapServers = "localhost:9092", ApplicationId = "app", Topic = "topic-a-retry", BatchSizeKB = 1, QueueBufferMaxMessages = 1000, CompressionType = "none" },
                ["dlq"]     = new KafkaProducerOptions { BootstrapServers = "localhost:9092", ApplicationId = "app", Topic = "topic-a-dlq", BatchSizeKB = 1, QueueBufferMaxMessages = 1000, CompressionType = "none" }
            };
            ILogger<KafkaProducerClient> logger = NullLogger<KafkaProducerClient>.Instance;
            return Track(new KafkaProducerClient(opts, new SecStub(), new SrStub(TrackDisposable), logger));
        }

        /// <summary>
        /// Retrieves the internal producer cache via reflection to enable injecting a fake producer.
        /// </summary>
        private static ConcurrentDictionary<(string ProducerKey, Type Type, bool Batch), object> GetCache(KafkaProducerClient client)
        {
            var fi = typeof(KafkaProducerClient).GetField("_producers", BindingFlags.Instance | BindingFlags.NonPublic);
            return (ConcurrentDictionary<(string, Type, bool), object>)fi!.GetValue(client)!;
        }

        /// <summary>
        /// Injects a fake producer for a specific logical producer key and mode (single or batch).
        /// </summary>
        private static void InjectFake(KafkaProducerClient client, string producerKey, bool batch, object fake)
        {
            var cache = GetCache(client);
            cache[(producerKey, typeof(string), batch)] = fake;
        }

        /// <summary>
        /// Verifies that single SendMessageAsync calls route to the configured topics for default, retry, and DLQ.
        /// </summary>
        [Fact]
        public async Task SendMessageAsync_Routes_To_Configured_Topics()
        {
            var client = CreateClient();
            var fakeDefault = new FakeProducer<string>();
            var fakeRetry = new FakeProducer<string>();
            var fakeDlq = new FakeProducer<string>();
            InjectFake(client, "default", batch: false, fakeDefault);
            InjectFake(client, "retry", batch: false, fakeRetry);
            InjectFake(client, "dlq", batch: false, fakeDlq);

            await client.SendMessageAsync("v1", "k1", producerKey: "default");
            await client.SendMessageAsync("v2", "k2", producerKey: "retry");
            await client.SendMessageAsync("v3", "k3", producerKey: "dlq");

            Assert.Single(fakeDefault.Produced);
            Assert.Equal("topic-a", fakeDefault.Produced[0].Topic);
            Assert.Single(fakeRetry.Produced);
            Assert.Equal("topic-a-retry", fakeRetry.Produced[0].Topic);
            Assert.Single(fakeDlq.Produced);
            Assert.Equal("topic-a-dlq", fakeDlq.Produced[0].Topic);
        }

        /// <summary>
        /// Verifies that SendBatchAsync routes batches to retry and DLQ topics and returns success counts.
        /// </summary>
        [Fact]
        public async Task SendBatchAsync_Routes_To_Retry_And_DLQ()
        {
            var client = CreateClient();
            var fakeRetry = new FakeProducer<string>();
            var fakeDlq = new FakeProducer<string>();
            InjectFake(client, "retry", batch: true, fakeRetry);
            InjectFake(client, "dlq", batch: true, fakeDlq);

            var retryMessages = new[] { "a", "b", "c" };
            var dlqMessages = new[] { "dead1", "dead2" };

            var retryResult = await client.SendBatchAsync(retryMessages, m => $"k-{m}", producerKey: "retry");
            var dlqResult = await client.SendBatchAsync(dlqMessages, m => $"k-{m}", producerKey: "dlq");

            Assert.Equal(retryMessages.Length, fakeRetry.Produced.Count);
            Assert.All(fakeRetry.Produced, p => Assert.Equal("topic-a-retry", p.Topic));
            Assert.Equal(retryMessages.Length, retryResult.TotalMessages);
            Assert.Equal(retryMessages.Length, retryResult.SuccessCount);

            Assert.Equal(dlqMessages.Length, fakeDlq.Produced.Count);
            Assert.All(fakeDlq.Produced, p => Assert.Equal("topic-a-dlq", p.Topic));
            Assert.Equal(dlqMessages.Length, dlqResult.TotalMessages);
            Assert.Equal(dlqMessages.Length, dlqResult.SuccessCount);
        }
    }
}
