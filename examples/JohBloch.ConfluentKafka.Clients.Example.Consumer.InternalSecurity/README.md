# Consumer.InternalSecurity example

This example is **consumer-only**, contains **no external cache**, and contains **no producer logic**.

## Configuration

The app reads configuration from these sources (in this order):

1. `local.settings.sample.json` (optional)
2. `local.settings.json` (optional, overrides sample)
3. `Values` entries inside `local.settings*.json` (Azure Functions format)
4. Environment variables (override JSON)

This example supports **two** `local.settings.json` formats:

- **Azure Functions-style**: `{ "IsEncrypted": false, "Values": { "Consumer__Topics": "topic-a,topic-b" } }`
- **Plain JSON sections**: `{ "Consumer": { "Topics": "topic-a,topic-b" } }`

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
- `Consumer__Topics` -> `Consumer:Topics`

## Running

```powershell
dotnet run --project .\JohBloch.ConfluentKafka.Clients.Example.Consumer.InternalSecurity.csproj
```
