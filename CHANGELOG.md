# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial release of JohBloch.ConfluentKafka.Clients
- Support for multiple schema types: Avro, JSON, Protobuf
- Producer client with single and batch message support
- Consumer client with configurable polling and offset management
- OAuth bearer authentication for Kafka brokers
- Schema Registry integration
- Dead Letter Queue (DLQ) functionality with JSON serialization
- Configurable DLQ topic patterns (per-topic or shared)
- protobuf-net support for POCO serialization/deserialization
- Comprehensive error handling and logging
- Advanced configuration passthrough for librdkafka
- Multi-topic producer support
- Automatic schema type detection

### Changed
- Reorganized serialization code into schema-type folders (Avro, Json, Protobuf)

### Fixed
- Improved test performance by mocking DLQ operations (24x speedup)

## [1.0.0] - YYYY-MM-DD

### Added
- First stable release

[Unreleased]: https://github.com/JohBloch/JohBloch.ConfluentKafka.Clients/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/JohBloch/JohBloch.ConfluentKafka.Clients/releases/tag/v1.0.0
