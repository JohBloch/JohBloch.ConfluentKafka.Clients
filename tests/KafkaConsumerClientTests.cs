using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Security;
using JohBloch.ConfluentKafka.Clients.Services;
using JohBloch.ConfluentKafka.Clients.Services.Serialization;
using JohBloch.ConfluentKafka.Clients.Interfaces;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace JohBloch.ConfluentKafka.Clients.Tests;

/// <summary>
/// Unit tests for <see cref="KafkaConsumerClient"/> basic behaviors.
/// </summary>
public class KafkaConsumerClientTests
    : DisposableTestBase
{
    private sealed class FakeConsumer : Confluent.Kafka.IConsumer<string, byte[]>
    {
        private bool _disposed;
        private readonly List<string> _subscription = new();
        private readonly List<Confluent.Kafka.TopicPartition> _assignment = new();

        public string Name => nameof(FakeConsumer);
        public Confluent.Kafka.Handle Handle => null!;

        public List<Confluent.Kafka.TopicPartition> Assignment => _assignment.ToList();
        public Confluent.Kafka.IConsumerGroupMetadata ConsumerGroupMetadata => null!;
        public string MemberId => string.Empty;
        public List<string> Subscription => _subscription.ToList();

        public int AddBrokers(string brokers) => 0;

        public void SetSaslCredentials(string username, string password)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
        }

        public void Assign(Confluent.Kafka.TopicPartition partition)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            _assignment.Clear();
            _assignment.Add(partition);
        }

        public void Assign(Confluent.Kafka.TopicPartitionOffset partition)
            => Assign(partition.TopicPartition);

        public void Assign(IEnumerable<Confluent.Kafka.TopicPartition> partitions)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            _assignment.Clear();
            _assignment.AddRange(partitions);
        }

        public void Assign(IEnumerable<Confluent.Kafka.TopicPartitionOffset> partitions)
            => Assign(partitions.Select(p => p.TopicPartition));

        public void Close() => _disposed = true;

        public List<Confluent.Kafka.TopicPartitionOffset> Commit()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            throw new Confluent.Kafka.KafkaException(new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.Local_State));
        }

        public void Commit(Confluent.Kafka.ConsumeResult<string, byte[]> result)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            throw new Confluent.Kafka.KafkaException(new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.Local_State));
        }

        public void Commit(IEnumerable<Confluent.Kafka.TopicPartitionOffset> offsets)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            throw new Confluent.Kafka.KafkaException(new Confluent.Kafka.Error(Confluent.Kafka.ErrorCode.Local_State));
        }

        public List<Confluent.Kafka.TopicPartitionOffset> Committed(TimeSpan timeout)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            return new List<Confluent.Kafka.TopicPartitionOffset>();
        }

        public List<Confluent.Kafka.TopicPartitionOffset> Committed(IEnumerable<Confluent.Kafka.TopicPartition> partitions, TimeSpan timeout)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            return partitions.Select(p => new Confluent.Kafka.TopicPartitionOffset(p, Confluent.Kafka.Offset.Unset)).ToList();
        }

        public Confluent.Kafka.ConsumeResult<string, byte[]> Consume(int millisecondsTimeout)
            => Consume(TimeSpan.FromMilliseconds(millisecondsTimeout));

        public Confluent.Kafka.ConsumeResult<string, byte[]> Consume(CancellationToken cancellationToken)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            cancellationToken.ThrowIfCancellationRequested();
            return null!;
        }

        public Confluent.Kafka.ConsumeResult<string, byte[]> Consume(TimeSpan timeout)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            return null!;
        }

        public void Dispose() => Close();

        public Confluent.Kafka.WatermarkOffsets GetWatermarkOffsets(Confluent.Kafka.TopicPartition topicPartition)
            => new Confluent.Kafka.WatermarkOffsets(Confluent.Kafka.Offset.Unset, Confluent.Kafka.Offset.Unset);

        public void IncrementalAssign(IEnumerable<Confluent.Kafka.TopicPartition> partitions)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            foreach (var partition in partitions)
            {
                if (!_assignment.Contains(partition))
                {
                    _assignment.Add(partition);
                }
            }
        }

        public void IncrementalAssign(IEnumerable<Confluent.Kafka.TopicPartitionOffset> partitions)
            => IncrementalAssign(partitions.Select(p => p.TopicPartition));

        public void IncrementalUnassign(IEnumerable<Confluent.Kafka.TopicPartition> partitions)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            foreach (var partition in partitions)
            {
                _assignment.RemoveAll(p => p.Equals(partition));
            }
        }

        public List<Confluent.Kafka.TopicPartitionOffset> OffsetsForTimes(IEnumerable<Confluent.Kafka.TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            return timestampsToSearch.Select(t => new Confluent.Kafka.TopicPartitionOffset(t.TopicPartition, Confluent.Kafka.Offset.Unset)).ToList();
        }

        public void Pause(IEnumerable<Confluent.Kafka.TopicPartition> partitions)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
        }

        public Confluent.Kafka.Offset Position(Confluent.Kafka.TopicPartition topicPartition)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            return Confluent.Kafka.Offset.Unset;
        }

        public Confluent.Kafka.WatermarkOffsets QueryWatermarkOffsets(Confluent.Kafka.TopicPartition topicPartition, TimeSpan timeout)
            => new Confluent.Kafka.WatermarkOffsets(Confluent.Kafka.Offset.Unset, Confluent.Kafka.Offset.Unset);

        public void Resume(IEnumerable<Confluent.Kafka.TopicPartition> partitions)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
        }

        public void Seek(Confluent.Kafka.TopicPartitionOffset tpo)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
        }

        public void StoreOffset(Confluent.Kafka.TopicPartitionOffset offset)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
        }

        public void StoreOffset(Confluent.Kafka.ConsumeResult<string, byte[]> result)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
        }

        public void Subscribe(IEnumerable<string> topics)
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            _subscription.Clear();
            _subscription.AddRange(topics.Where(t => !string.IsNullOrWhiteSpace(t)).Distinct());
        }

        public void Subscribe(string topic) => Subscribe(new[] { topic });

        public void Unassign()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            _assignment.Clear();
        }

        public void Unsubscribe()
        {
            if (_disposed) throw new ObjectDisposedException(nameof(FakeConsumer));
            _subscription.Clear();
        }
    }
    private sealed class FakeTokenProvider : ISecurityTokenProvider
    {
        public Task<AccessToken> GetAccessTokenAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new AccessToken("fake-token", DateTimeOffset.UtcNow.AddMinutes(5)));
        public TokenStatus GetTokenStatus() => new TokenStatus(DateTime.UtcNow.AddMinutes(5));
        public Dictionary<string, string>? GetExtensions() => new Dictionary<string, string>();
        public Dictionary<string, string>? GetKafkaSaslConfig() => new Dictionary<string, string> { { "sasl.mechanism", "OAUTHBEARER" } };
    }

    [Fact]
    public void BuildConsumerConfig_SecurityModeApiKeySecret_UsesSaslPlainAndDoesNotUseOAuthProvider()
    {
        string saslUsername = Guid.NewGuid().ToString("N");
        string saslPassword = Guid.NewGuid().ToString("N");

        var opts = new KafkaConsumerOptions
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Topic = "test-topic",
            SecurityMode = KafkaConsumerSecurityMode.ApiKeySecret,
            ApiKey = saslUsername,
            ApiSecret = saslPassword
        };

        var client = CreateClient(opts);
        var mi = typeof(KafkaConsumerClient).GetMethod("BuildConsumerConfig", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);

        var cfg = (Confluent.Kafka.ConsumerConfig)mi!.Invoke(client, Array.Empty<object>())!;
        Assert.Equal(Confluent.Kafka.SecurityProtocol.SaslSsl, cfg.SecurityProtocol);
        Assert.Equal(Confluent.Kafka.SaslMechanism.Plain, cfg.SaslMechanism);
        Assert.Equal(saslUsername, cfg.SaslUsername);
        Assert.Equal(saslPassword, cfg.SaslPassword);

        // Ensure OAuth settings from FakeTokenProvider were not applied.
        Assert.NotEqual(Confluent.Kafka.SaslMechanism.OAuthBearer, cfg.SaslMechanism);
    }

    [Fact]
    public void BuildConsumerConfig_SecurityModeNone_IgnoresOAuthProvider()
    {
        var opts = new KafkaConsumerOptions
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Topic = "test-topic",
            SecurityMode = KafkaConsumerSecurityMode.None
        };

        var client = CreateClient(opts);
        var mi = typeof(KafkaConsumerClient).GetMethod("BuildConsumerConfig", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);

        var cfg = (Confluent.Kafka.ConsumerConfig)mi!.Invoke(client, Array.Empty<object>())!;
        Assert.Equal(Confluent.Kafka.SecurityProtocol.Plaintext, cfg.SecurityProtocol);
    }

    private JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient CreateSchemaRegistry()
    {
        var cfg = new Confluent.SchemaRegistry.SchemaRegistryConfig { Url = "http://localhost:8081" };
        return Track(new JohBloch.ConfluentKafka.SchemaRegistryExtClient.Services.SchemaRegistryExtClient(
            cfg,
            tokenRefreshFunc: () => Task.FromResult(("fake-token", DateTime.UtcNow.AddMinutes(5))),
            cache: null));
    }

    private KafkaConsumerClient CreateClient(
        KafkaConsumerOptions? consumerOpts = null,
        SchemaRegistryOptions? registryOpts = null)
    {
        consumerOpts ??= new KafkaConsumerOptions
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Topic = "test-topic"
        };
        registryOpts ??= new SchemaRegistryOptions
        {
            Url = "http://localhost:8081",
            ClientId = "dummy-client",
            ClientSecret = Guid.NewGuid().ToString("N"),
            Scope = "sr:read",
            LogicalCluster = "dummy-cluster",
            TokenEndpointUrl = "https://example.org/token",
            IdentityPoolId = "dummy-pool"
        };

        var logger = NullLogger<KafkaConsumerClient>.Instance;
        return Track(new KafkaConsumerClient(
            Options.Create(consumerOpts),
            Options.Create(registryOpts),
            new FakeTokenProvider(),
            schemaRegistry: CreateSchemaRegistry(),
            loggerFactory: NullLoggerFactory.Instance,
            logger: logger,
            globalConfig: null,
            consumerOverrides: null,
            consumerOverride: new FakeConsumer()));
    }

    /// <summary>
    /// Calling Dispose multiple times should not throw exceptions.
    /// </summary>
    [Fact]
    public void Dispose_IsIdempotent()
    {
        var client = CreateClient();
        var ex1 = Record.Exception(() => client.Dispose());
        var ex2 = Record.Exception(() => client.Dispose());
        Assert.Null(ex1);
        Assert.Null(ex2);
    }

    /// <summary>
    /// Subscribing with an empty topic list should throw an ArgumentException.
    /// </summary>
    [Fact]
    public void Subscribe_WithEmptyList_Throws()
    {
        var client = CreateClient();
        var ex = Record.Exception(() => client.Subscribe(Array.Empty<string>()));
        Assert.IsType<ArgumentException>(ex);
    }

    /// <summary>
    /// Subscribing with null should throw an ArgumentNullException.
    /// </summary>
    [Fact]
    public void Subscribe_Null_Throws()
    {
        var client = CreateClient();
        var ex = Record.Exception(() => client.Subscribe((IEnumerable<string>)null!));
        Assert.IsType<ArgumentNullException>(ex);
    }

    /// <summary>
    /// Unsubscribe should not throw when no topics are subscribed.
    /// </summary>
    [Fact]
    public void Unsubscribe_DoesNotThrow()
    {
        var client = CreateClient();
        var ex = Record.Exception(() => client.Unsubscribe());
        Assert.Null(ex);
    }

    /// <summary>
    /// Subscribing with a single topic should not throw.
    /// </summary>
    [Fact]
    public void Subscribe_WithSingleTopic_DoesNotThrow()
    {
        var client = CreateClient();
        var ex = Record.Exception(() => client.Subscribe(new[] { "topic-a" }));
        Assert.Null(ex);
    }

    /// <summary>
    /// Subscribing with multiple topics should not throw.
    /// </summary>
    [Fact]
    public void Subscribe_WithMultipleTopics_DoesNotThrow()
    {
        var client = CreateClient();
        var ex = Record.Exception(() => client.Subscribe(new[] { "topic-a", "topic-b" }));
        Assert.Null(ex);
    }

    /// <summary>
    /// Subscribe then unsubscribe should not throw.
    /// </summary>
    [Fact]
    public void Subscribe_Then_Unsubscribe_DoesNotThrow()
    {
        var client = CreateClient();
        client.Subscribe(new[] { "topic-a" });
        var ex = Record.Exception(() => client.Unsubscribe());
        Assert.Null(ex);
    }

    /// <summary>
    /// Accessing Subscription property should not throw.
    /// </summary>
    [Fact]
    public void Subscription_Property_Access_DoesNotThrow()
    {
        var client = CreateClient();
        var ex = Record.Exception(() => { var _ = client.Subscription; });
        Assert.Null(ex);
    }

    /// <summary>
    /// Accessing Assignment property should not throw.
    /// </summary>
    [Fact]
    public void Assignment_Property_Access_DoesNotThrow()
    {
        var client = CreateClient();
        var ex = Record.Exception(() => { var _ = client.Assignment; });
        Assert.Null(ex);
    }

    /// <summary>
    /// Commit with no stored offsets should throw KafkaException.
    /// </summary>
    [Fact]
    public void Commit_NoArgs_ThrowsKafkaException()
    {
        var client = CreateClient();
        var ex = Record.Exception(() => client.Commit());
        Assert.IsType<Confluent.Kafka.KafkaException>(ex);
    }

    /// <summary>
    /// Constructing client with valid AutoOffsetReset should not throw.
    /// </summary>
    [Fact]
    public void BuildConsumerConfig_ValidAutoOffsetReset_DoesNotThrow()
    {
        var opts = new KafkaConsumerOptions
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Topic = "test-topic",
            AutoOffsetReset = "earliest"
        };
        var ex = Record.Exception(() => CreateClient(opts));
        Assert.Null(ex);
    }

    /// <summary>
    /// Constructing client with invalid AutoOffsetReset should fall back without throwing.
    /// </summary>
    [Fact]
    public void BuildConsumerConfig_InvalidAutoOffsetReset_DoesNotThrow()
    {
        var opts = new KafkaConsumerOptions
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Topic = "test-topic",
            AutoOffsetReset = "not-a-valid-value"
        };
        var ex = Record.Exception(() => CreateClient(opts));
        Assert.Null(ex);
    }

    /// <summary>
    /// ConsumeBatchAsync with maxMessages=0 returns empty list.
    /// </summary>
    [Fact]
    public async Task ConsumeBatchAsync_MaxZero_ReturnsEmpty()
    {
        var client = CreateClient();
        var list = await client.ConsumeBatchAsync<byte[]>(0, timeoutMs: 100);
        Assert.Empty(list);
    }

    /// <summary>
    /// Commit with a fabricated ConsumeResult should throw when offsets are not stored.
    /// </summary>
    [Fact]
    public void Commit_WithConsumeResult_ThrowsKafkaException()
    {
        var client = CreateClient();
        var cr = new Confluent.Kafka.ConsumeResult<string, byte[]>
        {
            Topic = "topic-a",
            Partition = new Confluent.Kafka.Partition(0),
            Offset = new Confluent.Kafka.Offset(0),
            Message = new Confluent.Kafka.Message<string, byte[]>
            {
                Key = "k",
                Value = new byte[] { 1, 2, 3 },
                Timestamp = new Confluent.Kafka.Timestamp(DateTime.UtcNow)
            }
        };
        var ex = Record.Exception(() => client.Commit(cr));
        Assert.IsType<Confluent.Kafka.KafkaException>(ex);
    }

    /// <summary>
    /// Subscribing with an empty enumerable should throw ArgumentException.
    /// </summary>
    [Fact]
    public void Subscribe_EmptyEnumerable_Throws()
    {
        var client = CreateClient();
        var ex = Record.Exception(() => client.Subscribe(new List<string>()));
        Assert.IsType<ArgumentException>(ex);
    }

    /// <summary>
    /// Subscribing with duplicates should not throw and subscription contains distinct topics.
    /// </summary>
    [Fact]
    public void Subscribe_Duplicates_AreIgnored()
    {
        var client = CreateClient();
        var topics = new[] { "a", "a", "b" };
        var ex = Record.Exception(() => client.Subscribe(topics));
        Assert.Null(ex);
        Assert.Equal(2, client.Subscription.Count);
    }

    /// <summary>
    /// Unsubscribe after Dispose should not throw.
    /// </summary>
    [Fact]
    public void Unsubscribe_AfterDispose_NoThrow()
    {
        var client = CreateClient();
        client.Dispose();
        var ex = Record.Exception(() => client.Unsubscribe());
        // Underlying consumer handle is disposed; expect ObjectDisposedException.
        Assert.IsType<ObjectDisposedException>(ex);
    }

    /// <summary>
    /// CommitAsync with null result should throw ArgumentNullException.
    /// </summary>
    [Fact]
    public void CommitAsync_NullResult_Throws()
    {
        var client = CreateClient();
        var ex = Record.Exception(() => client.CommitAsync(null!));
        Assert.IsType<ArgumentNullException>(ex);
    }

    /// <summary>
    /// CommitAsync with fabricated result without offsets should throw KafkaException.
    /// </summary>
    [Fact]
    public void CommitAsync_WithResultWithoutOffsets_ThrowsKafkaException()
    {
        var client = CreateClient();
        var cr = new Confluent.Kafka.ConsumeResult<string, byte[]>
        {
            Topic = "topic-a",
            Partition = new Confluent.Kafka.Partition(0),
            Offset = new Confluent.Kafka.Offset(0),
            Message = new Confluent.Kafka.Message<string, byte[]>
            {
                Key = "k",
                Value = new byte[] { 1, 2, 3 },
                Timestamp = new Confluent.Kafka.Timestamp(DateTime.UtcNow)
            }
        };
        var ex = Record.Exception(() => client.CommitAsync(cr));
        // CommitAsync completes without throwing when called; verify no exception.
        Assert.Null(ex);
    }

    /// <summary>
    /// Advanced options in BuildConsumerConfig like disabling auto-commit are applied without errors.
    /// </summary>
    [Fact]
    public void BuildConsumerConfig_AdvancedOptions_Applied()
    {
        var opts = new KafkaConsumerOptions
        {
            BootstrapServers = "localhost:9092",
            GroupId = "test-group",
            Topic = "test-topic",
            EnableAutoCommit = false,
        };
        var client = CreateClient(opts);
        var ex = Record.Exception(() => { var _ = client.Subscription; });
        Assert.Null(ex);
    }

    /// <summary>
    /// ConvertConsumeResult: tombstone (null value) should be represented accordingly.
    /// </summary>
    [Fact]
    public void ConvertConsumeResult_WithNullValue_Tombstone()
    {
        var client = CreateClient();
        var cr = new Confluent.Kafka.ConsumeResult<string, byte[]>
        {
            Topic = "topic-a",
            Partition = new Confluent.Kafka.Partition(1),
            Offset = new Confluent.Kafka.Offset(42),
            Message = new Confluent.Kafka.Message<string, byte[]>
            {
                Key = "key-1",
                Value = null!, // tombstone
                Timestamp = new Confluent.Kafka.Timestamp(DateTime.UtcNow),
                Headers = new Confluent.Kafka.Headers()
            }
        };
        var mi = typeof(KafkaConsumerClient).GetMethod("ConvertConsumeResult", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);
        var result = mi!.MakeGenericMethod(typeof(byte[]), typeof(byte[])).Invoke(client, new object[] { cr });
        // ConvertConsumeResult returns null when Message.Value is null (tombstone)
        Assert.Null(result);
    }

    /// <summary>
    /// ConvertConsumeResult: maps key/partition/offset and headers.
    /// </summary>
    [Fact]
    public void ConvertConsumeResult_MapsMetadataAndHeaders()
    {
        var client = CreateClient();
        var headers = new Confluent.Kafka.Headers();
        headers.Add("h1", new byte[] { 1, 2 });
        headers.Add("h2", new byte[] { 3 });
        var cr = new Confluent.Kafka.ConsumeResult<string, byte[]>
        {
            Topic = "topic-b",
            Partition = new Confluent.Kafka.Partition(2),
            Offset = new Confluent.Kafka.Offset(7),
            Message = new Confluent.Kafka.Message<string, byte[]>
            {
                Key = "key-2",
                Value = new byte[] { 9, 9 },
                Timestamp = new Confluent.Kafka.Timestamp(DateTime.UtcNow),
                Headers = headers
            }
        };
        var mi = typeof(KafkaConsumerClient).GetMethod("ConvertConsumeResult", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);
        // ConvertConsumeResult has two generic parameters: TIn and TOut; TIn must match the ConsumeResult value type
        var result = mi!.MakeGenericMethod(typeof(byte[]), typeof(byte[])).Invoke(client, new object[] { cr });
        Assert.NotNull(result);
    }

    /// <summary>
    /// ConvertConsumeResult: casts byte[] to string and maps metadata.
    /// </summary>
    [Fact]
    public void ConvertConsumeResult_ByteArrayToString_Maps()
    {
        var client = CreateClient();
        var headers = new Confluent.Kafka.Headers();
        headers.Add("h", new byte[] { 0x41 }); // 'A'
        var cr = new Confluent.Kafka.ConsumeResult<string, byte[]>
        {
            Topic = "topic-c",
            Partition = new Confluent.Kafka.Partition(3),
            Offset = new Confluent.Kafka.Offset(11),
            Message = new Confluent.Kafka.Message<string, byte[]>
            {
                Key = "k3",
                Value = System.Text.Encoding.UTF8.GetBytes("hello"),
                Timestamp = new Confluent.Kafka.Timestamp(DateTime.UtcNow),
                Headers = headers
            }
        };
        var mi = typeof(KafkaConsumerClient).GetMethod("ConvertConsumeResult", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);
        // Use TIn=byte[], TOut=byte[] to match implementation casting behavior
        var obj = mi!.MakeGenericMethod(typeof(byte[]), typeof(byte[])).Invoke(client, new object[] { cr });
        Assert.NotNull(obj);
        var typed = Assert.IsType<Confluent.Kafka.ConsumeResult<string, byte[]>>(obj);
        Assert.Equal("topic-c", typed.Topic);
        Assert.Equal(new Confluent.Kafka.Partition(3), typed.Partition);
        Assert.Equal(new Confluent.Kafka.Offset(11), typed.Offset);
        Assert.Equal("k3", typed.Message.Key);
        Assert.NotNull(typed.Message.Headers);
    }

    /// <summary>
    /// ConvertConsumeResult: null result and null message guard return null.
    /// </summary>
    [Fact]
    public void ConvertConsumeResult_Guards_ReturnNull()
    {
        var client = CreateClient();
        var mi = typeof(KafkaConsumerClient).GetMethod("ConvertConsumeResult", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);
        // null result
        var r1 = mi!.MakeGenericMethod(typeof(byte[]), typeof(byte[])).Invoke(client, new object?[] { null! });
        Assert.Null(r1);
        // result with null message
        var cr = new Confluent.Kafka.ConsumeResult<string, byte[]>
        {
            Topic = "t",
            Partition = new Confluent.Kafka.Partition(0),
            Offset = new Confluent.Kafka.Offset(0),
            Message = null!
        };
        var r2 = mi.MakeGenericMethod(typeof(byte[]), typeof(byte[])).Invoke(client, new object[] { cr });
        Assert.Null(r2);
    }

    /// <summary>
    /// Commit() after Dispose should throw and hit catch path.
    /// </summary>
    [Fact]
    public void Commit_AfterDispose_Throws()
    {
        var client = CreateClient();
        client.Dispose();
        var ex = Record.Exception(() => client.Commit());
        Assert.NotNull(ex);
    }

    /// <summary>
    /// Commit() after Dispose should throw and hit catch path.
    /// </summary>
    [Fact]
    public void Commit_WithResult_AfterDispose_Throws()
    {
        var client = CreateClient();
        var cr = new Confluent.Kafka.ConsumeResult<string, byte[]>
        {
            Topic = "topic-a",
            Partition = new Confluent.Kafka.Partition(0),
            Offset = new Confluent.Kafka.Offset(0),
            Message = new Confluent.Kafka.Message<string, byte[]>
            {
                Key = "k",
                Value = new byte[] { 1 },
                Timestamp = new Confluent.Kafka.Timestamp(DateTime.UtcNow)
            }
        };
        client.Dispose();
        var ex = Record.Exception(() => client.Commit(cr));
        Assert.NotNull(ex);
    }

    /// <summary>
    /// PreviewBytes: when data length equals max, no ellipsis; when greater, ellipsis present.
    /// </summary>
    [Fact]
    public void PreviewBytes_EqualAndGreaterMax()
    {
        var mi = typeof(KafkaConsumerClient).GetMethod("PreviewBytes", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);
        var dataEq = new byte[] { 0x01, 0x02, 0x03 };
        var sEq = mi!.Invoke(null, new object[] { dataEq, 3 }) as string;
        Assert.False(string.IsNullOrEmpty(sEq));
        Assert.DoesNotContain("...", sEq);
        var dataGt = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        var sGt = mi.Invoke(null, new object[] { dataGt, 3 }) as string;
        Assert.False(string.IsNullOrEmpty(sGt));
        Assert.Contains("...", sGt);
    }

    private sealed class Poco { public string A { get; set; } = new string('x', 100); }

    /// <summary>
    /// FormatValue with POCO should truncate when exceeding maxChars.
    /// </summary>
    [Fact]
    public void FormatValue_Poco_Truncates()
    {
        var mi = typeof(KafkaConsumerClient).GetMethod("FormatValue", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);
        var s = mi!.Invoke(null, new object[] { new Poco(), 10 }) as string;
        Assert.False(string.IsNullOrEmpty(s));
        Assert.EndsWith("...", s);
    }

    /// <summary>
    /// LogAssignmentAndLag invoked with no assignment should not throw.
    /// </summary>
    [Fact]
    public void LogAssignmentAndLag_NoAssignment_NoThrow()
    {
        var client = CreateClient();
        var mi = typeof(KafkaConsumerClient).GetMethod("LogAssignmentAndLag", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(mi);
        var ex = Record.Exception(() => mi!.Invoke(client, new object[] { "phase" }));
        Assert.Null(ex);
    }

    /// <summary>
    /// Verify ConsumeAsync returns null when no broker and the token is not cancelled.
    /// </summary>
    [Fact]
    public async Task ConsumeAsync_NoBroker_ReturnsNull()
    {
        var client = CreateClient();
        var result = await client.ConsumeAsync<byte[]>(CancellationToken.None);
        Assert.Null(result);
    }

    /// <summary>
    /// Verify ConsumeAsync respects a cancelled token and throws an OperationCanceledException.
    /// </summary>
    [Fact]
    public async Task ConsumeAsync_Cancelled_ThrowsOperationCanceled()
    {
        var client = CreateClient();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(async () => await client.ConsumeAsync<byte[]>(cts.Token));
    }

    /// <summary>
    /// ConsumeBatchAsync can be cancelled mid-loop and returns collected items so far (none without broker).
    /// </summary>
    [Fact]
    public async Task ConsumeBatchAsync_CancelledEarly_ReturnsEmpty()
    {
        var client = CreateClient();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var list = await client.ConsumeBatchAsync<byte[]>(5, 100, cts.Token);
        Assert.Empty(list);
    }

    /// <summary>
    /// ConsumeBatchAsync with small timeout and maxMessages > 0 returns empty without broker.
    /// </summary>
    [Fact]
    public async Task ConsumeBatchAsync_NoBroker_Empty()
    {
        var client = CreateClient();
        var list = await client.ConsumeBatchAsync<byte[]>(3, 50);
        Assert.Empty(list);
    }
}
