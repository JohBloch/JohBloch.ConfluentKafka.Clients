---
name: Bug Report
about: Create a report to help us improve
title: '[BUG] '
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Configure producer/consumer with '...'
2. Send/receive message '....'
3. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

**Code Sample**
```csharp
// Minimal code sample that reproduces the issue
var producer = new KafkaProducerClient(...);
await producer.SendMessageAsync(...);
```

**Error Message/Stack Trace**
```
If applicable, paste the error message or stack trace here
```

**Environment:**
 - OS: [e.g. Windows 11, Ubuntu 22.04]
 - .NET Version: [e.g. 10.0.1]
 - Library Version: [e.g. 1.0.0]
 - Kafka Version: [e.g. 3.6.0]
 - Schema Registry Version: [if applicable]

**Additional context**
Add any other context about the problem here.

**Logs**
If applicable, include relevant log output (redact sensitive information).
