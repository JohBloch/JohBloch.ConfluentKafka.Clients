# Contributing to JohBloch.ConfluentKafka.Clients

First off, thank you for considering contributing to JohBloch.ConfluentKafka.Clients! It's people like you that make this library a great tool.

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check the existing issues as you might find out that you don't need to create one. When you are creating a bug report, please include as many details as possible:

* **Use a clear and descriptive title**
* **Describe the exact steps which reproduce the problem**
* **Provide specific examples to demonstrate the steps**
* **Describe the behavior you observed after following the steps**
* **Explain which behavior you expected to see instead and why**
* **Include code samples and stack traces**

### Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please include:

* **Use a clear and descriptive title**
* **Provide a step-by-step description of the suggested enhancement**
* **Provide specific examples to demonstrate the steps**
* **Describe the current behavior and explain which behavior you expected to see instead**
* **Explain why this enhancement would be useful**

### Pull Requests

* Fill in the required template
* Do not include issue numbers in the PR title
* Follow the C# coding style used throughout the project
* Include thoughtfully-worded, well-structured tests
* Document new code based on the XML documentation style
* End all files with a newline

## Development Setup

### Prerequisites

* .NET 10.0 SDK or later
* Git

### Building

```bash
git clone https://github.com/JohBloch/JohBloch.ConfluentKafka.Clients.git
cd JohBloch.ConfluentKafka.Clients
dotnet build
```

### Running Tests

```bash
dotnet test
```

All tests should pass before submitting a PR.

### Code Style

* Follow standard C# conventions
* Use meaningful variable and method names
* Add XML documentation comments for public APIs
* Keep methods focused and small
* Write unit tests for new functionality

### Commit Messages

* Use the present tense ("Add feature" not "Added feature")
* Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
* Limit the first line to 72 characters or less
* Reference issues and pull requests liberally after the first line

Example:
```
Add DLQ support for failed messages

- Implement DeadLetterMessage model
- Add SendToDeadLetterQueueAsync methods
- Include JSON serialization for Grafana/Loki

Closes #123
```

## Project Structure

```
├── src/
│   └── JohBloch.ConfluentKafka.Clients/     # Main library
│       ├── Interfaces/                       # Public interfaces
│       ├── Models/                           # Data models
│       ├── Services/                         # Core implementations
│       │   └── Serialization/                # Serializers organized by schema
│       │       ├── Avro/
│       │       ├── Json/
│       │       └── Protobuf/
│       └── Security/                         # Security providers
├── tests/                                    # Unit tests
└── docs/                                     # Documentation
```

## Testing Guidelines

* Write unit tests for all new functionality
* Aim for high code coverage (>80%)
* Use descriptive test names that explain what is being tested
* Follow the Arrange-Act-Assert pattern
* Mock external dependencies (Kafka, Schema Registry)
* Keep tests fast (<100ms per test when possible)

## Documentation

* Update README.md if you change functionality
* Add XML comments to all public APIs
* Update relevant documentation in the `docs/` folder
* Include code examples for new features

## Questions?

Feel free to open an issue with your question or reach out to the maintainers.

Thank you for contributing! 🎉
