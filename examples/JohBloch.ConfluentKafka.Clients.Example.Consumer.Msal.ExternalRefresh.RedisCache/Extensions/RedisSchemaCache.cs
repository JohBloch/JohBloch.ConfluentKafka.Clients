using System.Collections.Concurrent;
using System.Text.Json;
using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Interfaces;
using JohBloch.ConfluentKafka.SchemaRegistryExtClient.Models;
using StackExchange.Redis;

namespace JohBloch.ConfluentKafka.Clients;

public sealed class RedisSchemaCache : ISchemaCache
{
    private static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions
    {
        PropertyNameCaseInsensitive = true
    };

    private readonly IConnectionMultiplexer _multiplexer;
    private readonly IDatabase _database;
    private readonly string _keyPrefix;
    private readonly TimeSpan? _defaultTtl;
    private readonly string _indexKey;
    private readonly ConcurrentDictionary<string, byte> _localIndex;

    private int _hitCount;
    private int _missCount;

    public RedisSchemaCache(
        IConnectionMultiplexer multiplexer,
        string keyPrefix,
        TimeSpan? defaultTtl)
    {
        _multiplexer = multiplexer ?? throw new ArgumentNullException(nameof(multiplexer));
        _database = _multiplexer.GetDatabase();

        _keyPrefix = string.IsNullOrWhiteSpace(keyPrefix) ? "schema-registry-cache:" : keyPrefix;
        _defaultTtl = defaultTtl;
        _indexKey = _keyPrefix + "__index";
        _localIndex = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);
    }

    public int HitCount => _hitCount;

    public int MissCount => _missCount;

    public int Count
    {
        get
        {
            long length = _database.SetLength(_indexKey);
            if (length > int.MaxValue)
            {
                return int.MaxValue;
            }

            return (int)length;
        }
    }

    public event EventHandler<string?>? CacheHit;

    public event EventHandler<string?>? CacheMiss;

    public bool TryGet(string? key, out CachedSchemaInfo? schema)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            schema = null;
            OnMiss(key);
            return false;
        }

        RedisValue value = _database.StringGet(FormatKey(key));
        if (!value.HasValue)
        {
            schema = null;
            OnMiss(key);
            return false;
        }

        schema = Deserialize(value);
        if (schema == null)
        {
            Remove(key);
            OnMiss(key);
            return false;
        }

        OnHit(key);
        return true;
    }

    public void Set(string? key, CachedSchemaInfo? schema)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return;
        }

        if (schema == null)
        {
            Remove(key);
            return;
        }

        string json = JsonSerializer.Serialize(schema, SerializerOptions);
        RedisKey redisKey = FormatKey(key);

        _database.StringSet(redisKey, json, _defaultTtl);
        _database.SetAdd(_indexKey, key);
        _localIndex.TryAdd(key, 0);
    }

    public void Remove(string? key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            return;
        }

        _database.KeyDelete(FormatKey(key));
        _database.SetRemove(_indexKey, key);
        _localIndex.TryRemove(key, out _);
    }

    public void Clear()
    {
        RedisValue[] keys = _database.SetMembers(_indexKey);
        foreach (RedisValue key in keys)
        {
            if (!key.HasValue)
            {
                continue;
            }

            string? originalKey = (string?)key;
            if (string.IsNullOrWhiteSpace(originalKey))
            {
                continue;
            }

            _database.KeyDelete(FormatKey(originalKey));
        }

        _database.KeyDelete(_indexKey);
        _localIndex.Clear();
    }

    public IEnumerable<string> KeysMatchingPrefix(string prefix)
    {
        if (string.IsNullOrEmpty(prefix))
        {
            return Array.Empty<string>();
        }

        List<string> matches = new List<string>();

        RedisValue[] keys = _database.SetMembers(_indexKey);
        foreach (RedisValue key in keys)
        {
            if (!key.HasValue)
            {
                continue;
            }

            string? originalKey = (string?)key;
            if (string.IsNullOrWhiteSpace(originalKey))
            {
                continue;
            }

            if (originalKey.StartsWith(prefix, StringComparison.Ordinal))
            {
                matches.Add(originalKey);
            }
        }

        return matches;
    }

    public void Dispose()
    {
        // Intentionally no-op: the connection multiplexer is owned by DI.
    }

    private RedisKey FormatKey(string key)
    {
        return _keyPrefix + key;
    }

    private CachedSchemaInfo? Deserialize(RedisValue value)
    {
        try
        {
            return JsonSerializer.Deserialize<CachedSchemaInfo>(value.ToString(), SerializerOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private void OnHit(string? key)
    {
        Interlocked.Increment(ref _hitCount);
        CacheHit?.Invoke(this, key);
    }

    private void OnMiss(string? key)
    {
        Interlocked.Increment(ref _missCount);
        CacheMiss?.Invoke(this, key);
    }
}
