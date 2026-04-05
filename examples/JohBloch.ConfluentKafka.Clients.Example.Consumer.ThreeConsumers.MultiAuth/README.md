# Example: 3 consumers with mixed auth (2x OAuth, 1x ApiKey/Secret)

This example runs **three independent consumers in one process**:

- **OAuthA**: Kafka OAuth settings A
- **OAuthB**: Kafka OAuth settings B
- **ApiKey**: Kafka **api_key/api_secret** using SASL/PLAIN over SSL

Because the library’s `OAuthSecurityTokenProvider` reads OAuth settings from `KafkaClientOptions` (bound via DI), the two OAuth consumers are created from **two separate DI containers** to keep their OAuth settings isolated.

## Configure

- Copy `local.settings.sample.json` → `local.settings.json`
- Fill in the values for your environment.

The app reads configuration from these sources (in this order):

1. `local.settings.sample.json` (optional)
2. `local.settings.json` (optional, overrides sample)
3. `Values` entries inside `local.settings*.json` (Azure Functions format)
4. Environment variables (override JSON)

This example supports **two** `local.settings.json` formats:

- **Azure Functions-style**: `{ "IsEncrypted": false, "Values": { "Consumers__OAuthA__Kafka__OAuth__ClientId": "..." } }`
- **Plain JSON sections**: `{ "Consumers": { "OAuthA": { "Kafka": { "OAuth": { "ClientId": "..." } } } } }`

Notes:
- `SchemaRegistryUrl` is required by the DI setup, but this example consumes `string` payloads, so it won’t call Schema Registry.
- For Confluent Cloud API key/secret auth to Kafka brokers you typically need `SaslSsl + Plain`.
- Subscriptions are created automatically by the library when `IKafkaConsumerClient` is constructed, based on `Consumer:Topics` (or `Consumer:Topic`).

## Run

From repo root:

```powershell
dotnet run --project .\examples\JohBloch.ConfluentKafka.Clients.Example.Consumer.ThreeConsumers.MultiAuth\JohBloch.ConfluentKafka.Clients.Example.Consumer.ThreeConsumers.MultiAuth.csproj -c Release
```

Press Ctrl+C to stop.
