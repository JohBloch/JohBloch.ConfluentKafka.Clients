# Clients.InternalSecurity example

This example configures **both** a producer client and a consumer client and contains **no external cache**.

## Configuration

The app reads configuration from these sources (in this order):

1. `local.settings.sample.json` (optional)
2. `local.settings.json` (optional, overrides sample)
3. `Values` entries inside `local.settings*.json` (Azure Functions format)
4. Environment variables (override JSON)

This example supports **two** `local.settings.json` formats:

- **Azure Functions-style**: `{ "IsEncrypted": false, "Values": { "Kafka__OAuth__ClientId": "..." } }`
- **Plain JSON sections**: `{ "Kafka": { "OAuth": { "ClientId": "..." } } }`

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

- `SchemaRegistry__Url` -> `SchemaRegistry:Url`
- `Kafka__BootstrapServers` -> `Kafka:BootstrapServers`
- `Producer__Producers__orders__Topic` -> `Producer:Producers:orders:Topic`
- `Consumer__Topics` -> `Consumer:Topics`

Notes:
- Subscriptions are created automatically by the library when `IKafkaConsumerClient` is constructed, based on `Consumer:Topics` (or `Consumer:Topic`).

## Running

```powershell
dotnet run --project .\JohBloch.ConfluentKafka.Clients.Example.Clients.InternalSecurity.csproj
```
