using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using JohBloch.ConfluentKafka.Clients.Models;
using JohBloch.ConfluentKafka.Clients.Security;
using JohBloch.ConfluentKafka.Clients.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace JohBloch.ConfluentKafka.Clients.Tests;

public class KafkaProducerClientDetectedSchemaTests : DisposableTestBase
{
    private sealed class SecStub : ISecurityTokenProvider
    {
        public Task<AccessToken> GetAccessTokenAsync(CancellationToken cancellationToken = default)
            => Task.FromResult(new AccessToken("token", DateTimeOffset.UtcNow.AddMinutes(5)));

        public TokenStatus GetTokenStatus() => new TokenStatus(DateTime.UtcNow.AddMinutes(5));

        public Dictionary<string, string>? GetExtensions() => new();

        public Dictionary<string, string>? GetKafkaSaslConfig() => new();
    }

    private sealed class CountingSchemaRegistryClient : ISchemaRegistryClient
    {
        public int GetLatestSchemaCalls { get; private set; }
        public string? LastSubject { get; private set; }

        public void Dispose() { }

        public Task<RegisteredSchema> GetLatestSchemaAsync(string subject)
        {
            GetLatestSchemaCalls++;
            LastSubject = subject;

            // We don't need a successful Schema Registry response for this test.
            // The production code falls back to Avro on failures, but the lookup must be invoked.
            return Task.FromException<RegisteredSchema>(new InvalidOperationException("Stubbed schema registry client"));
        }

        public Task<int> RegisterSchemaAsync(string subject, string schema, bool normalize = false)
            => throw new NotImplementedException();

        public Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize = false)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> RegisterSchemaWithResponseAsync(string subject, Schema schema, bool normalize = false)
            => throw new NotImplementedException();

        public Task<int> GetSchemaIdAsync(string subject, string schema, bool normalize = false)
            => throw new NotImplementedException();

        public Task<int> GetSchemaIdAsync(string subject, Schema schema, bool normalize = false)
            => throw new NotImplementedException();

        public Task<Schema> GetSchemaAsync(int id, string subject)
            => throw new NotImplementedException();

        public Task<string> GetSchemaAsync(string subject, int version)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> GetRegisteredSchemaAsync(string subject, int version, bool format = false)
            => throw new NotImplementedException();

        public Task<Schema> GetSchemaBySubjectAndIdAsync(string subject, int id, string format)
            => throw new NotImplementedException();

        public Task<Schema> GetSchemaByGuidAsync(string subject, string guid)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool normalize = false, bool format = false)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> GetLatestWithMetadataAsync(string subject, IDictionary<string, string> metadata, bool deleted)
            => throw new NotImplementedException();

        public Task<List<string>> GetAllSubjectsAsync()
            => throw new NotImplementedException();

        public Task<List<int>> GetSubjectVersionsAsync(string subject)
            => throw new NotImplementedException();

        public Task<bool> IsCompatibleAsync(string subject, string schema)
            => throw new NotImplementedException();

        public Task<bool> IsCompatibleAsync(string subject, Schema schema)
            => throw new NotImplementedException();

        public string ConstructKeySubjectName(string topic, string recordType)
            => throw new NotImplementedException();

        public string ConstructValueSubjectName(string topic, string recordType)
            => throw new NotImplementedException();

        public Task<Compatibility> GetCompatibilityAsync(string subject)
            => throw new NotImplementedException();

        public Task<Compatibility> UpdateCompatibilityAsync(Compatibility compatibility, string subject)
            => throw new NotImplementedException();

        public void ClearLatestCaches()
            => throw new NotImplementedException();

        public void ClearCaches()
            => throw new NotImplementedException();

        public IEnumerable<KeyValuePair<string, string>> Config => throw new NotImplementedException();

        public IAuthenticationHeaderValueProvider AuthHeaderProvider => throw new NotImplementedException();

        public System.Net.IWebProxy? Proxy { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        // Everything else is not used by this test.
        public Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<int> RegisterSchemaAsync(string subject, string schema, bool normalize = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<int> RegisterSchemaAsync(string subject, string schema, IList<SchemaReference> references, bool normalize = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<int> GetSchemaIdAsync(string subject, Schema schema, bool normalize = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<int> GetSchemaIdAsync(string subject, string schema, bool normalize = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<int> GetSchemaIdAsync(string subject, string schema, IList<SchemaReference> references, bool normalize = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<Schema> GetSchemaAsync(int id, string? subject = null, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<Schema> GetSchemaAsync(string subject, int version, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> GetRegisteredSchemaAsync(string subject, int version, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> GetRegisteredSchemaAsync(int id, string? subject = null, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<string> GetCompatibilityAsync(string subject, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<string> UpdateCompatibilityAsync(string subject, string compatibility, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<bool> TestCompatibilityAsync(string subject, int version, Schema schema, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<bool> TestCompatibilityAsync(string subject, int version, string schema, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<List<string>> GetAllSubjectsAsync(CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<List<int>> GetSubjectVersionsAsync(string subject, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool normalize = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> LookupSchemaAsync(string subject, string schema, bool normalize = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> LookupSchemaAsync(string subject, string schema, IList<SchemaReference> references, bool normalize = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<bool> DeleteSubjectAsync(string subject, bool permanent = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<bool> DeleteSchemaVersionAsync(string subject, int version, bool permanent = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<List<int>> GetDeletedSchemaVersionAsync(string subject, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<List<string>> GetDeletedSubjectsAsync(CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> GetLatestSchemaAsync(string subject, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<int> RegisterSchemaAsync(string subject, Schema schema, bool normalize = false, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<int> RegisterSchemaAsync(string subject, string schema, bool normalize = false, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<int> RegisterSchemaAsync(string subject, string schema, IList<SchemaReference> references, bool normalize = false, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> LookupSchemaAsync(string subject, Schema schema, bool normalize = false, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> LookupSchemaAsync(string subject, string schema, bool normalize = false, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> LookupSchemaAsync(string subject, string schema, IList<SchemaReference> references, bool normalize = false, bool format = false, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public int MaxCachedSchemas { get => 0; set { } }

        public bool DisableSslCertificateVerification { get => false; set { } }

        public string? BasicAuthUserInfo { get => null; set { } }

        public Task<bool> DeleteSubjectAsync(string subject, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<bool> DeleteSchemaVersionAsync(string subject, int version, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<List<string>> GetSubjectsAsync(CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<List<int>> GetVersionsAsync(string subject, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        public Task<RegisteredSchema> GetLatestSchemaAsync(string subject, CancellationToken cancellationToken = default)
            => GetLatestSchemaAsync(subject);
    }

    private sealed class CountingSchemaRegistryExtClient : JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces.ISchemaRegistryExtClient
    {
        private readonly ISchemaRegistryClient _client;

        public CountingSchemaRegistryExtClient(ISchemaRegistryClient client)
        {
            _client = client;
        }

        public void Dispose() => _client.Dispose();

        public Task<ISchemaRegistryClient> GetClientAsync() => Task.FromResult(_client);

        public Task<string?> GetSchemaAsync(string subject, int id)
            => throw new NotImplementedException();

        public Task<string?> GetSchema(byte[] schemaBytes)
            => throw new NotImplementedException();

        public Task<int> RegisterSchemaAsync(string topic, string schema, string schemaType, string? name, string? namespaceValue)
            => throw new NotImplementedException();

        public Task<int> RegisterValueSchemaAsync(string topic, string schema, string schemaType, string? name)
            => throw new NotImplementedException();

        public Task<int> RegisterKeySchemaAsync(string topic, string schema, string schemaType, string? name)
            => throw new NotImplementedException();
    }

    private static ConcurrentDictionary<(string ProducerKey, Type Type, bool Batch), object> GetCache(KafkaProducerClient client)
    {
        FieldInfo? fi = typeof(KafkaProducerClient).GetField("_producers", BindingFlags.Instance | BindingFlags.NonPublic);
        return (ConcurrentDictionary<(string, Type, bool), object>)fi!.GetValue(client)!;
    }

    private static void InjectFake(KafkaProducerClient client, string producerKey, bool batch, object fake, string? serializerTypeFullName)
    {
        ConcurrentDictionary<(string ProducerKey, Type Type, bool Batch), object> cache = GetCache(client);
        cache[(producerKey + (serializerTypeFullName ?? ""), typeof(string), batch)] = fake;
    }

    [Fact]
    public async Task SendMessageWithDetectedSchemaAsync_LooksUp_Subject_And_Produces()
    {
        var schemaClient = new CountingSchemaRegistryClient();
        var schemaExtClient = new CountingSchemaRegistryExtClient(schemaClient);

        var opts = new Dictionary<string, KafkaProducerOptions>
        {
            ["default"] = new KafkaProducerOptions { BootstrapServers = "localhost:9092", ApplicationId = "app", Topic = "topic-a", BatchSizeKB = 1, QueueBufferMaxMessages = 1000, CompressionType = "none" }
        };

        ILogger<KafkaProducerClient> logger = NullLogger<KafkaProducerClient>.Instance;
        KafkaProducerClient client = Track(new KafkaProducerClient(opts, new SecStub(), schemaExtClient, NullLoggerFactory.Instance, logger));

        var fakeDefault = new FakeProducer<string>();
        Type? wrapperOpenType = typeof(KafkaProducerClient).Assembly.GetType("JohBloch.ConfluentKafka.Clients.Services.AsyncSerializerWrapper`1", throwOnError: true);
        Type wrapperType = wrapperOpenType!.MakeGenericType(typeof(string));
        InjectFake(client, "default", batch: false, fakeDefault, wrapperType.FullName);

        await client.SendMessageWithDetectedSchemaAsync("v1", "k1", producerKey: "default");

        Assert.Equal(1, schemaClient.GetLatestSchemaCalls);
        Assert.Equal("topic-a-value", schemaClient.LastSubject);

        Assert.Single(fakeDefault.Produced);
        Assert.Equal("topic-a", fakeDefault.Produced[0].Topic);
    }
}
