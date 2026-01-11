# API Stability

This project follows semantic versioning (SemVer):

- **Patch**: bug fixes, no API changes.
- **Minor**: backwards compatible features.
- **Major**: breaking changes.

## Supported Public Surface

The following namespaces are considered **supported** and are the primary integration points for consumers:

- `JohBloch.ConfluentKafka.Clients` (DI entry points like `AddKafkaClients`)
- `JohBloch.ConfluentKafka.Clients.Configuration` (high-level configuration like `KafkaClientOptions`)
- `JohBloch.ConfluentKafka.Clients.Interfaces` (interfaces such as `IKafkaProducerClient`, `IKafkaConsumerClient`)
- `JohBloch.ConfluentKafka.Clients.Models` (DTOs and option types used by the supported API)

Anything outside these namespaces may change more frequently.

## Implementation Details

Namespaces such as `JohBloch.ConfluentKafka.Clients.Services.*` and `JohBloch.ConfluentKafka.Clients.Security.*` contain implementation types.
Some of these types are public for historical/technical reasons, but they are **not guaranteed stable** unless explicitly documented.
