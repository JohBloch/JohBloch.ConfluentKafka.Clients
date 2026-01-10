# Protobuf-net POCO Support

The client library now supports deserialization of Protobuf messages to POCOs using protobuf-net. This makes it possible to work with plain C# classes instead of having to generate classes from `.proto` files.

## Difference between Google.Protobuf and protobuf-net

### Google.Protobuf (standard)
- Requires you to generate classes from `.proto` files
- Classes implement `IMessage<T>`
- Used when you have generated Protobuf classes

### protobuf-net (POCO support)
- Works with plain C# classes (POCOs)
- Uses `[ProtoContract]` and `[ProtoMember]` attributes
- Automatically selected if the class does NOT implement `IMessage<T>`

## Example: Using POCOs with protobuf-net

### 1. Define your POCO class with protobuf-net attributes

```csharp
using ProtoBuf;

[ProtoContract]
public class CustomerMessage
{
    [ProtoMember(1)]
    public string CustomerId { get; set; } = string.Empty;
    
    [ProtoMember(2)]
    public string Name { get; set; } = string.Empty;
    
    [ProtoMember(3)]
    public string Email { get; set; } = string.Empty;
    
    [ProtoMember(4)]
    public DateTime CreatedAt { get; set; }
}
```

### 2. Use SerializerFactory to serialize

```csharp
using JohBloch.ConfluentKafka.Clients.Services;
using JohBloch.ConfluentKafka.Clients.Models;

// Create factory (normally via dependency injection)
var serializerFactory = new SerializerFactory(schemaRegistry, loggerFactory);

// Create serializer - automatically uses protobuf-net because CustomerMessage does not implement IMessage<T>
var serializer = serializerFactory.Create<CustomerMessage>(SchemaType.Protobuf);

// Serialize
var message = new CustomerMessage 
{ 
    CustomerId = "CUST123", 
    Name = "John Doe",
    Email = "john@example.com",
    CreatedAt = DateTime.UtcNow
};

var context = new SerializationContext(MessageComponentType.Value, "customers");
var bytes = await serializer.SerializeAsync(message, context);
```

### 3. Use DeserializerFactory to deserialize

```csharp
// Create deserializer - automatically uses protobuf-net
var deserializerFactory = new DeserializerFactory(schemaRegistry, loggerFactory);
var deserializer = deserializerFactory.Create<CustomerMessage>(SchemaType.Protobuf);

// Deserialize
var deserializedMessage = await deserializer.DeserializeAsync(bytes, context);

Console.WriteLine($"Customer: {deserializedMessage.Name}");
```

### 4. Or use KafkaProducerClient directly

```csharp
var producer = new KafkaProducerClient(producerOptions, schemaRegistryOptions, loggerFactory);

// Send message with Protobuf schema
await producer.SendMessageWithSchemaAsync(
    message: new CustomerMessage { /* ... */ },
    key: "CUST123",
    producerKey: "default",
    schemaType: SchemaType.Protobuf
);
```

## Important notes

1. **Automatic selection**: Factories automatically choose between Google.Protobuf and protobuf-net based on whether the type implements `IMessage<T>`

2. **ProtoMember numbers**: Must be unique and consistent with your Protobuf schema in Schema Registry

3. **Data types**: protobuf-net supports most C# data types including DateTime, decimal, etc.

4. **Compatibility**: protobuf-net messages are compatible with standard Protobuf wire format

## Example with KafkaConsumerClient

```csharp
var consumer = new KafkaConsumerClient(consumerOptions, schemaRegistryOptions, loggerFactory);

// Get deserializer for POCO
var deserializerFactory = new DeserializerFactory(schemaRegistry, loggerFactory);
var deserializer = deserializerFactory.Create<CustomerMessage>(SchemaType.Protobuf);

// Consume messages
var result = await consumer.ConsumeAsync<CustomerMessage>(
    topics: new[] { "customers" },
    deserializer: deserializer,
    cancellationToken: cancellationToken
);

foreach (var message in result.Messages)
{
    Console.WriteLine($"Received: {message.Name} ({message.Email})");
}
```

## See also

- [protobuf-net documentation](https://github.com/protobuf-net/protobuf-net)
- [Protobuf Language Guide](https://developers.google.com/protocol-buffers/docs/proto3)
