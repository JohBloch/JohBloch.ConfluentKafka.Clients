# Consumer.Msal.ExternalRefresh.RedisCache example

This example is **consumer-only** and uses:

- **MSAL device code flow** to fetch/refresh OAuth tokens externally
- **Redis** for schema registry cache

## Configuration

The app reads configuration from these sources (in this order):

1. `local.settings.sample.json` (optional)
2. `local.settings.json` (optional, overrides sample)
3. `Values` entries inside `local.settings*.json` (Azure Functions format)
4. Environment variables (override JSON)

This example supports **two** `local.settings.json` formats:

- **Azure Functions-style**: `{ "IsEncrypted": false, "Values": { "Kafka__OAuth__TenantId": "..." } }`
- **Plain JSON sections**: `{ "Kafka": { "OAuth": { "TenantId": "..." } } }`

### Recommended setup

- Copy `local.settings.sample.json` to `local.settings.json`
- Fill in the required values

Example:

```powershell
Copy-Item local.settings.sample.json local.settings.json
```

### Environment variables

Environment variables use the standard `.NET` mapping where `__` becomes `:`.

Examples:

- `Msal__TenantId` -> `Msal:TenantId`
- `Msal__ClientId` -> `Msal:ClientId`
- `SchemaRegistry__Cache__Redis__ConnectionString` -> `SchemaRegistry:Cache:Redis:ConnectionString`

Notes:
- Subscriptions are created automatically by the library when `IKafkaConsumerClient` is constructed, based on `Consumer:Topics` (or `Consumer:Topic`).

## Running

```powershell
dotnet run --project .\JohBloch.ConfluentKafka.Clients.Example.Consumer.Msal.ExternalRefresh.RedisCache.csproj
```
