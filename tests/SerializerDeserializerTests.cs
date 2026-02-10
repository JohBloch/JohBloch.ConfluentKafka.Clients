using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Services;
using JohBloch.ConfluentKafka.Clients.Services.Serialization;
using JohBloch.ConfluentKafka.Clients.Services.Serialization.Avro;
using JohBloch.ConfluentKafka.Clients.Services.Serialization.Json;
using JohBloch.ConfluentKafka.Clients.Services.Serialization.Protobuf;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

using SchemaType = JohBloch.ConfluentKafka.Clients.Models.SchemaType;

namespace JohBloch.ConfluentKafka.Clients.Tests;

/// <summary>
/// Unit tests for serializers and deserializers (Avro, JSON, Protobuf).
/// </summary>
public class SerializerDeserializerTests : DisposableTestBase
{
    private JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient CreateFakeSchemaRegistry()
    {
        var config = new SchemaRegistryConfig { Url = "http://localhost:8081" };
        var ext = Track(new JohBloch.ConfluentKafka.SchemaRegistryExtClient.Services.SchemaRegistryExtClient(
            config,
            tokenRefreshFunc: () => Task.FromResult(("fake-token", DateTime.UtcNow.AddMinutes(5))),
            cache: null));
        return ext;
    }

    #region JSON Tests

    public class TestMessage
    {
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public string Email { get; set; } = string.Empty;
    }

    [Fact]
    public async Task JsonSerializer_SerializesObjectCorrectly()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var serializer = new JsonSerializer<TestMessage>(schemaRegistry, NullLogger<JsonSerializer<TestMessage>>.Instance);
        var message = new TestMessage { Name = "John Doe", Age = 30, Email = "john@example.com" };
        var context = new SerializationContext(MessageComponentType.Value, "test-topic");

        // Act
        var bytes = await serializer.SerializeAsync(message, context);

        // Assert
        Assert.NotNull(bytes);
        Assert.NotEmpty(bytes);
        
        var json = Encoding.UTF8.GetString(bytes);
        Assert.Contains("John Doe", json);
        Assert.Contains("john@example.com", json);
    }

    [Fact]
    public async Task JsonDeserializer_DeserializesObjectCorrectly()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var deserializer = new JsonDeserializer<TestMessage>(schemaRegistry, NullLogger<JsonDeserializer<TestMessage>>.Instance);
        var original = new TestMessage { Name = "Jane Smith", Age = 25, Email = "jane@example.com" };
        var json = JsonSerializer.Serialize(original, new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });
        var bytes = Encoding.UTF8.GetBytes(json);
        var context = new SerializationContext(MessageComponentType.Value, "test-topic");

        // Act
        var result = await deserializer.DeserializeAsync(bytes, context);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(original.Name, result.Name);
        Assert.Equal(original.Age, result.Age);
        Assert.Equal(original.Email, result.Email);
    }

    [Fact]
    public async Task JsonSerializer_AndDeserializer_RoundTrip()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var serializer = new JsonSerializer<TestMessage>(schemaRegistry, NullLogger<JsonSerializer<TestMessage>>.Instance);
        var deserializer = new JsonDeserializer<TestMessage>(schemaRegistry, NullLogger<JsonDeserializer<TestMessage>>.Instance);
        var original = new TestMessage { Name = "Test User", Age = 42, Email = "test@example.com" };
        var context = new SerializationContext(MessageComponentType.Value, "test-topic");

        // Act
        var bytes = await serializer.SerializeAsync(original, context);
        var result = await deserializer.DeserializeAsync(bytes, context);

        // Assert
        Assert.NotNull(result);
        Assert.Equal(original.Name, result.Name);
        Assert.Equal(original.Age, result.Age);
        Assert.Equal(original.Email, result.Email);
    }

    #endregion

    #region SerializerFactory Tests

    [Fact]
    public void SerializerFactory_CreatesJsonSerializer()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new SerializerFactory(schemaRegistry, loggerFactory);

        // Act
        var serializer = factory.Create<TestMessage>(SchemaType.Json);

        // Assert
        Assert.NotNull(serializer);
        Assert.IsType<JsonSerializer<TestMessage>>(serializer);
    }

    [Fact]
    public void SerializerFactory_CreatesAvroSerializer()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new SerializerFactory(schemaRegistry, loggerFactory);

        // Act
        var serializer = factory.Create<TestMessage>(SchemaType.Avro);

        // Assert
        Assert.NotNull(serializer);
        Assert.IsType<AvroSerializer<TestMessage>>(serializer);
    }

    [Fact]
    public void SerializerFactory_ThrowsForUnsupportedSchemaType()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new SerializerFactory(schemaRegistry, loggerFactory);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => factory.Create<TestMessage>((SchemaType)999));
    }

    #endregion

    #region DeserializerFactory Tests

    [Fact]
    public void DeserializerFactory_CreatesJsonDeserializer()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new DeserializerFactory(schemaRegistry, loggerFactory);

        // Act
        var deserializer = factory.Create<TestMessage>(SchemaType.Json);

        // Assert
        Assert.NotNull(deserializer);
        Assert.IsType<JsonDeserializer<TestMessage>>(deserializer);
    }

    [Fact]
    public void DeserializerFactory_CreatesAvroDeserializer()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new DeserializerFactory(schemaRegistry, loggerFactory);

        // Act
        var deserializer = factory.Create<TestMessage>(SchemaType.Avro);

        // Assert
        Assert.NotNull(deserializer);
        Assert.IsType<AvroDeserializer<TestMessage>>(deserializer);
    }

    [Fact]
    public void DeserializerFactory_ThrowsForUnsupportedSchemaType()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new DeserializerFactory(schemaRegistry, loggerFactory);

        // Act & Assert
        Assert.Throws<NotSupportedException>(() => factory.Create<TestMessage>((SchemaType)999));
    }

    [Fact]
    public async Task DeserializerFactory_DetectSchemaType_ReturnsJsonForNoMagicByte()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new DeserializerFactory(schemaRegistry, loggerFactory);
        var plainJsonBytes = Encoding.UTF8.GetBytes("{\"name\":\"test\"}");

        // Act
        var schemaType = await factory.DetectSchemaTypeAsync(plainJsonBytes, "test-topic");

        // Assert
        Assert.Equal(SchemaType.Json, schemaType);
    }

    [Fact]
    public async Task DeserializerFactory_DetectSchemaType_ReturnsAvroForInvalidSchemaId()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new DeserializerFactory(schemaRegistry, loggerFactory);
        
        // Create invalid schema registry format (magic byte but invalid schema ID)
        var bytes = new byte[] { 0x00, 0xFF, 0xFF, 0xFF, 0xFF };

        // Act
        var schemaType = await factory.DetectSchemaTypeAsync(bytes, "test-topic");

        // Assert - Should default to Avro when detection fails
        Assert.Equal(SchemaType.Avro, schemaType);
    }

    #endregion

    #region Protobuf Tests

    // Note: These tests demonstrate that the factory now automatically detects whether to use
    // Google.Protobuf (for IMessage<T> types) or protobuf-net (for POCOs with [ProtoContract])

    [Fact]
    public void SerializerFactory_UsesProtobufForPOCO()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new SerializerFactory(schemaRegistry, loggerFactory);

        // Act - TestMessage is a POCO with [ProtoContract], should use protobuf-net
        var serializer = factory.Create<TestMessage>(SchemaType.Protobuf);

        // Assert
        Assert.NotNull(serializer);
        Assert.IsType<ProtobufSerializer<TestMessage>>(serializer);
    }

    [Fact]
    public void DeserializerFactory_UsesProtobufForPOCO()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new DeserializerFactory(schemaRegistry, loggerFactory);

        // Act - TestMessage is a POCO with [ProtoContract], should use protobuf-net
        var deserializer = factory.Create<TestMessage>(SchemaType.Protobuf);
        
        // Assert
        Assert.NotNull(deserializer);
        Assert.IsType<ProtobufDeserializer<TestMessage>>(deserializer);
    }

    #endregion

    #region Protobuf POCO Tests

    [ProtoBuf.ProtoContract]
    public class ProtobufTestMessage
    {
        [ProtoBuf.ProtoMember(1)]
        public string Name { get; set; } = string.Empty;
        
        [ProtoBuf.ProtoMember(2)]
        public int Age { get; set; }
        
        [ProtoBuf.ProtoMember(3)]
        public string Email { get; set; } = string.Empty;
    }

    [Fact]
    public async Task ProtobufSerializer_SerializesPOCOCorrectly()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var serializer = new ProtobufSerializer<ProtobufTestMessage>(schemaRegistry, NullLogger.Instance);
        var message = new ProtobufTestMessage { Name = "Alice", Age = 25, Email = "alice@example.com" };
        var context = new SerializationContext(MessageComponentType.Value, "test-topic");

        // Act
        var bytes = await serializer.SerializeAsync(message, context);

        // Assert
        Assert.NotNull(bytes);
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public async Task ProtobufDeserializer_DeserializesPOCOCorrectly()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var serializer = new ProtobufSerializer<ProtobufTestMessage>(schemaRegistry, NullLogger.Instance);
        var deserializer = new ProtobufDeserializer<ProtobufTestMessage>(schemaRegistry, NullLogger.Instance);
        var originalMessage = new ProtobufTestMessage { Name = "Bob", Age = 35, Email = "bob@example.com" };
        var serContext = new SerializationContext(MessageComponentType.Value, "test-topic");

        // Act
        var bytes = await serializer.SerializeAsync(originalMessage, serContext);
        var deserContext = new SerializationContext(MessageComponentType.Value, "test-topic");
        var deserializedMessage = await deserializer.DeserializeAsync(bytes, deserContext);

        // Assert
        Assert.NotNull(deserializedMessage);
        Assert.Equal(originalMessage.Name, deserializedMessage.Name);
        Assert.Equal(originalMessage.Age, deserializedMessage.Age);
        Assert.Equal(originalMessage.Email, deserializedMessage.Email);
    }

    [Fact]
    public void SerializerFactory_CreateProtobufSerializer_ForPOCO()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new SerializerFactory(schemaRegistry, loggerFactory);

        // Act - ProtobufTestMessage is a POCO with [ProtoContract], should use protobuf-net
        var serializer = factory.Create<ProtobufTestMessage>(SchemaType.Protobuf);

        // Assert
        Assert.NotNull(serializer);
        Assert.IsType<ProtobufSerializer<ProtobufTestMessage>>(serializer);
    }

    [Fact]
    public void DeserializerFactory_CreateProtobufDeserializer_ForPOCO()
    {
        // Arrange
        var schemaRegistry = CreateFakeSchemaRegistry();
        var loggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => { });
        var factory = new DeserializerFactory(schemaRegistry, loggerFactory);

        // Act - ProtobufTestMessage is a POCO with [ProtoContract], should use protobuf-net
        var deserializer = factory.Create<ProtobufTestMessage>(SchemaType.Protobuf);

        // Assert
        Assert.NotNull(deserializer);
        Assert.IsType<ProtobufDeserializer<ProtobufTestMessage>>(deserializer);
    }

    #endregion

    #region Schema Type Enum Tests

    [Fact]
    public void SchemaType_HasCorrectValues()
    {
        // Verify all schema types are defined
        Assert.True(Enum.IsDefined(typeof(SchemaType), SchemaType.Avro));
        Assert.True(Enum.IsDefined(typeof(SchemaType), SchemaType.Protobuf));
        Assert.True(Enum.IsDefined(typeof(SchemaType), SchemaType.Json));
    }

    #endregion
}
