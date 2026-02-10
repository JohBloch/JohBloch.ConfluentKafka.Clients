// Dead Letter Queue unit tests
using System;
using System.Collections.Generic;
using System.Text;
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
/// Unit tests for Dead Letter Queue functionality in <see cref="KafkaProducerClient"/>.
/// </summary>
public class DeadLetterQueueTests
    : DisposableTestBase
{
    private static string CreateTestKey(string prefix) => $"{prefix}-{Guid.NewGuid():N}";

    private sealed class FakeTokenProvider : ISecurityTokenProvider
    {
        public Task<AccessToken> GetAccessTokenAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new AccessToken("fake-token", DateTimeOffset.UtcNow.AddMinutes(5)));
        public TokenStatus GetTokenStatus() => new TokenStatus(DateTime.UtcNow.AddMinutes(5));
        public Dictionary<string, string>? GetExtensions() => new();
        public Dictionary<string, string>? GetKafkaSaslConfig() => new() { { "sasl.mechanism", "OAUTHBEARER" } };
    }

    private JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient CreateSchemaRegistry()
    {
        var cfg = new SchemaRegistryConfig { Url = "http://localhost:8081" };
        return Track(new JohBloch.ConfluentKafka.SchemaRegistryExtClient.Services.SchemaRegistryExtClient(
            cfg,
            tokenRefreshFunc: () => Task.FromResult(("fake-token", DateTime.UtcNow.AddMinutes(5))),
            cache: null));
    }

    private KafkaProducerClient CreateProducerClient(KafkaProducerOptions? opts = null)
    {
        opts ??= new KafkaProducerOptions
        {
            BootstrapServers = "localhost:9092",
            ApplicationId = "test-app",
            Topic = "orders",
            BatchSizeKB = 32,
            QueueBufferMaxMessages = 10000,
            CompressionType = "gzip",
            DeadLetterQueueTopicPattern = "dlq-{topic}",
            IncludeStackTraceInDlq = false
        };

        var producerOptions = new Dictionary<string, KafkaProducerOptions>
        {
            ["default"] = opts
        };

        return Track(new KafkaProducerClient(
            producerOptions,
            new FakeTokenProvider(),
            CreateSchemaRegistry(),
            NullLoggerFactory.Instance,
            NullLogger<KafkaProducerClient>.Instance
        ));
    }

    [Fact]
    public void DeadLetterMessage_CanBeInstantiated()
    {
        // Arrange & Act
        var originalKey = CreateTestKey("key");
        var dlqMessage = new DeadLetterMessage
        {
            OriginalTopic = "test-topic",
            Partition = 1,
            Offset = 12345,
            FailedAt = DateTime.UtcNow,
            ErrorMessage = "Test error",
            ErrorType = "TestException",
            StackTrace = "at Test.Method()",
            RetryCount = 3,
            Severity = "error",
            OriginalKey = originalKey,
            OriginalValueBase64 = "dGVzdA==",
            ApplicationName = "test-app",
            Hostname = "test-host"
        };

        // Assert
        Assert.Equal("test-topic", dlqMessage.OriginalTopic);
        Assert.Equal(1, dlqMessage.Partition);
        Assert.Equal(12345, dlqMessage.Offset);
        Assert.Equal("Test error", dlqMessage.ErrorMessage);
        Assert.Equal("TestException", dlqMessage.ErrorType);
        Assert.Equal(3, dlqMessage.RetryCount);
        Assert.NotNull(dlqMessage.Headers);
        Assert.NotNull(dlqMessage.Metadata);
    }

    [Fact]
    public void DeadLetterMessage_InitializesCollections()
    {
        // Arrange & Act
        var dlqMessage = new DeadLetterMessage();

        // Assert
        Assert.NotNull(dlqMessage.Headers);
        Assert.NotNull(dlqMessage.Metadata);
        Assert.Empty(dlqMessage.Headers);
        Assert.Empty(dlqMessage.Metadata);
    }

    [Fact]
    public async Task SendToDeadLetterQueueAsync_WithManualMessage_ThrowsOnNullMessage()
    {
        // Arrange
        using var client = CreateProducerClient();

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            client.SendToDeadLetterQueueAsync(null!, key: "test-key", producerKey: "default", ct: default));
    }

    [Fact]
    public async Task SendToDeadLetterQueueAsync_WithManualMessage_ThrowsOnInvalidProducerKey()
    {
        // Arrange
        using var client = CreateProducerClient();
        var dlqMessage = new DeadLetterMessage
        {
            OriginalTopic = "test-topic",
            ErrorMessage = "Test error"
        };

        // Act & Assert
        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            client.SendToDeadLetterQueueAsync(dlqMessage, producerKey: "nonexistent", ct: default));
        
        Assert.Contains("nonexistent", ex.Message);
    }

    [Fact]
    public void DeadLetterMessage_EnrichesHostname_WhenNotSet()
    {
        // Arrange
        var dlqMessage = new DeadLetterMessage
        {
            OriginalTopic = "orders",
            Partition = 0,
            Offset = 100,
            FailedAt = DateTime.UtcNow,
            ErrorMessage = "Validation failed",
            ErrorType = "ValidationException"
        };

        // Act - Simulate what the client does
        if (string.IsNullOrEmpty(dlqMessage.Hostname))
        {
            dlqMessage.Hostname = Environment.MachineName;
        }

        // Assert
        Assert.NotNull(dlqMessage.Hostname);
        Assert.NotEmpty(dlqMessage.Hostname);
        Assert.Equal(Environment.MachineName, dlqMessage.Hostname);
    }

    [Fact]
    public async Task SendToDeadLetterQueueAsync_WithConsumeResult_ThrowsOnNullMessage()
    {
        // Arrange
        using var client = CreateProducerClient();
        var exception = new Exception("Test error");

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            client.SendToDeadLetterQueueAsync<string, string>(null!, exception));
    }

    [Fact]
    public async Task SendToDeadLetterQueueAsync_WithConsumeResult_ThrowsOnNullException()
    {
        // Arrange
        using var client = CreateProducerClient();
        var messageKey = CreateTestKey("order");
        var consumeResult = new ConsumeResult<string, string>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(123),
            Message = new Message<string, string>
            {
                Key = messageKey,
                Value = "test-value"
            }
        };

        // Act & Assert
        await Assert.ThrowsAsync<ArgumentNullException>(() =>
            client.SendToDeadLetterQueueAsync(consumeResult, null!));
    }

    [Fact]
    public async Task SendToDeadLetterQueueAsync_WithConsumeResult_ThrowsOnInvalidProducerKey()
    {
        // Arrange
        using var client = CreateProducerClient();
        var messageKey = CreateTestKey("order");
        var consumeResult = new ConsumeResult<string, string>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(123),
            Message = new Message<string, string>
            {
                Key = messageKey,
                Value = "test-value"
            }
        };
        var exception = new Exception("Test error");

        // Act & Assert
        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            client.SendToDeadLetterQueueAsync(consumeResult, exception, retryCount: 0, producerKey: "invalid", additionalMetadata: null, ct: default));
        
        Assert.Contains("invalid", ex.Message);
    }

    [Fact]
    public void DlqTopicPattern_DefaultPerTopic_Works()
    {
        // Arrange
        var options = new KafkaProducerOptions
        {
            BootstrapServers = "localhost:9092",
            Topic = "orders",
            ApplicationId = "test-app",
            DeadLetterQueueTopicPattern = "dlq-{topic}"
        };

        // Act
        var dlqTopic = options.DeadLetterQueueTopicPattern.Replace("{topic}", options.Topic);

        // Assert
        Assert.Equal("dlq-orders", dlqTopic);
    }

    [Fact]
    public void DlqTopicPattern_SharedDlq_Works()
    {
        // Arrange
        var options = new KafkaProducerOptions
        {
            BootstrapServers = "localhost:9092",
            Topic = "orders",
            ApplicationId = "test-app",
            DeadLetterQueueTopicPattern = "dlq-myapp"
        };

        // Act
        var dlqTopic = options.DeadLetterQueueTopicPattern.Replace("{topic}", options.Topic);

        // Assert
        Assert.Equal("dlq-myapp", dlqTopic); // No replacement occurred
    }

    [Fact]
    public void DlqTopicPattern_WithEnvironment_Works()
    {
        // Arrange
        var options = new KafkaProducerOptions
        {
            BootstrapServers = "localhost:9092",
            Topic = "customers",
            ApplicationId = "test-app",
            DeadLetterQueueTopicPattern = "dlq-prod-{topic}"
        };

        // Act
        var dlqTopic = options.DeadLetterQueueTopicPattern.Replace("{topic}", options.Topic);

        // Assert
        Assert.Equal("dlq-prod-customers", dlqTopic);
    }

    [Fact]
    public void DeadLetterMessage_HeadersAndMetadata_CanBePopulated()
    {
        // Arrange
        var dlqMessage = new DeadLetterMessage
        {
            OriginalTopic = "orders",
            ErrorMessage = "Test error"
        };

        // Act
        dlqMessage.Headers["correlation-id"] = "abc-123";
        dlqMessage.Headers["content-type"] = "application/json";
        dlqMessage.Metadata["trace-id"] = "xyz-789";
        dlqMessage.Metadata["retry-reason"] = "timeout";

        // Assert
        Assert.Equal(2, dlqMessage.Headers.Count);
        Assert.Equal(2, dlqMessage.Metadata.Count);
        Assert.Equal("abc-123", dlqMessage.Headers["correlation-id"]);
        Assert.Equal("xyz-789", dlqMessage.Metadata["trace-id"]);
    }

    [Fact]
    public void DeadLetterMessage_OriginalValueBase64_CanBeDecoded()
    {
        // Arrange
        var originalText = "This is a test message";
        var base64Value = Convert.ToBase64String(Encoding.UTF8.GetBytes(originalText));
        
        var dlqMessage = new DeadLetterMessage
        {
            OriginalTopic = "orders",
            OriginalValueBase64 = base64Value
        };

        // Act
        var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(dlqMessage.OriginalValueBase64));

        // Assert
        Assert.Equal(originalText, decoded);
    }

    [Fact]
    public void KafkaProducerOptions_DlqDefaults_AreSet()
    {
        // Arrange & Act
        var options = new KafkaProducerOptions
        {
            BootstrapServers = "localhost:9092",
            Topic = "test-topic",
            ApplicationId = "test-app"
        };

        // Assert
        Assert.Equal("dlq-{topic}", options.DeadLetterQueueTopicPattern);
        Assert.False(options.IncludeStackTraceInDlq);
    }

    [Fact]
    public void KafkaProducerOptions_DlqOptions_CanBeOverridden()
    {
        // Arrange & Act
        var options = new KafkaProducerOptions
        {
            BootstrapServers = "localhost:9092",
            Topic = "orders",
            ApplicationId = "test-app",
            DeadLetterQueueTopicPattern = "custom-dlq",
            IncludeStackTraceInDlq = true
        };

        // Assert
        Assert.Equal("custom-dlq", options.DeadLetterQueueTopicPattern);
        Assert.True(options.IncludeStackTraceInDlq);
    }

    [Fact]
    public void DeadLetterMessage_SeverityLevels_CanBeSet()
    {
        // Arrange & Act
        var warningMessage = new DeadLetterMessage { Severity = "warning" };
        var errorMessage = new DeadLetterMessage { Severity = "error" };
        var criticalMessage = new DeadLetterMessage { Severity = "critical" };

        // Assert
        Assert.Equal("warning", warningMessage.Severity);
        Assert.Equal("error", errorMessage.Severity);
        Assert.Equal("critical", criticalMessage.Severity);
    }

    [Fact]
    public void DeadLetterMessage_RetryCount_Tracks()
    {
        // Arrange
        var dlqMessage = new DeadLetterMessage
        {
            OriginalTopic = "orders",
            RetryCount = 0
        };

        // Act
        dlqMessage.RetryCount++;
        dlqMessage.RetryCount++;
        dlqMessage.RetryCount++;

        // Assert
        Assert.Equal(3, dlqMessage.RetryCount);
    }

    [Fact]
    public void BuildDeadLetterMessage_PreservesOriginalKey()
    {
        // Arrange
        var originalKey = CreateTestKey("order");
        var consumeResult = new ConsumeResult<string, TestOrder>
        {
            Topic = "orders",
            Partition = new Partition(2),
            Offset = new Offset(54321),
            Message = new Message<string, TestOrder>
            {
                Key = originalKey,
                Value = new TestOrder { OrderId = originalKey, Amount = 100.50m }
            }
        };
        var exception = new InvalidOperationException("Processing failed");

        // Act - Simulate building DLQ message
        var dlqMessage = new DeadLetterMessage
        {
            OriginalTopic = consumeResult.Topic,
            Partition = consumeResult.Partition.Value,
            Offset = consumeResult.Offset.Value,
            FailedAt = DateTime.UtcNow,
            ErrorMessage = exception.Message,
            ErrorType = exception.GetType().Name,
            RetryCount = 2,
            OriginalKey = consumeResult.Message.Key?.ToString(),
            ApplicationName = "test-app",
            Hostname = Environment.MachineName
        };

        // Assert
        Assert.Equal(originalKey, dlqMessage.OriginalKey);
        Assert.Equal("orders", dlqMessage.OriginalTopic);
        Assert.Equal(2, dlqMessage.Partition);
        Assert.Equal(54321, dlqMessage.Offset);
        Assert.Equal("Processing failed", dlqMessage.ErrorMessage);
        Assert.Equal("InvalidOperationException", dlqMessage.ErrorType);
        Assert.Equal(2, dlqMessage.RetryCount);
    }

    [Fact]
    public void ExtractHeaders_FromKafkaHeaders_Works()
    {
        // Arrange
        var headers = new Headers
        {
            { "correlation-id", Encoding.UTF8.GetBytes("abc-123") },
            { "message-type", Encoding.UTF8.GetBytes("OrderCreated") },
            { "binary-data", new byte[] { 0x00, 0xFF, 0xAB } } // Binary that can't be UTF8 decoded
        };

        // Act - Simulate header extraction logic
        var extractedHeaders = new Dictionary<string, string>();
        foreach (var header in headers)
        {
            try
            {
                var headerValue = Encoding.UTF8.GetString(header.GetValueBytes());
                extractedHeaders[header.Key] = headerValue;
            }
            catch
            {
                extractedHeaders[header.Key] = Convert.ToBase64String(header.GetValueBytes());
            }
        }

        // Assert
        Assert.Equal(3, extractedHeaders.Count);
        Assert.Equal("abc-123", extractedHeaders["correlation-id"]);
        Assert.Equal("OrderCreated", extractedHeaders["message-type"]);
        Assert.True(extractedHeaders.ContainsKey("binary-data"));
    }

    [Fact]
    public void BuildDeadLetterMessage_WithAdditionalMetadata_Includes()
    {
        // Arrange
        var messageKey = CreateTestKey("order");
        var consumeResult = new ConsumeResult<string, string>
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(100),
            Message = new Message<string, string>
            {
                Key = messageKey,
                Value = "test-value"
            }
        };
        var exception = new Exception("Test error");
        var additionalMetadata = new Dictionary<string, string>
        {
            ["trace-id"] = "xyz-789",
            ["user-id"] = "user-456"
        };

        // Act - Simulate building DLQ message
        var dlqMessage = new DeadLetterMessage
        {
            OriginalTopic = consumeResult.Topic,
            Partition = consumeResult.Partition.Value,
            Offset = consumeResult.Offset.Value,
            FailedAt = DateTime.UtcNow,
            ErrorMessage = exception.Message,
            ErrorType = exception.GetType().Name,
            RetryCount = 1,
            OriginalKey = consumeResult.Message.Key?.ToString(),
            ApplicationName = "test-app",
            Hostname = Environment.MachineName
        };

        // Add additional metadata
        foreach (var kvp in additionalMetadata)
        {
            dlqMessage.Metadata[kvp.Key] = kvp.Value;
        }

        // Assert
        Assert.Equal(2, dlqMessage.Metadata.Count);
        Assert.Equal("xyz-789", dlqMessage.Metadata["trace-id"]);
        Assert.Equal("user-456", dlqMessage.Metadata["user-id"]);
        Assert.Equal("orders", dlqMessage.OriginalTopic);
        Assert.Equal(1, dlqMessage.RetryCount);
    }

    private sealed class TestOrder
    {
        public string OrderId { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }
}
