# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible for receiving such patches depends on the CVSS v3.0 Rating:

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

Please report (suspected) security vulnerabilities to **[INSERT SECURITY EMAIL]**. You will receive a response from us within 48 hours. If the issue is confirmed, we will release a patch as soon as possible depending on complexity but historically within a few days.

Please include the following information in your report:

* Type of issue (e.g. buffer overflow, SQL injection, cross-site scripting, etc.)
* Full paths of source file(s) related to the manifestation of the issue
* The location of the affected source code (tag/branch/commit or direct URL)
* Any special configuration required to reproduce the issue
* Step-by-step instructions to reproduce the issue
* Proof-of-concept or exploit code (if possible)
* Impact of the issue, including how an attacker might exploit the issue

This information will help us triage your report more quickly.

## Security Best Practices

When using this library, please follow these security best practices:

### OAuth Bearer Tokens

* Store OAuth tokens securely
* Use short-lived tokens with appropriate refresh mechanisms
* Never commit tokens or secrets to version control
* Use environment variables or secure secret management systems

### Schema Registry

* Use HTTPS for Schema Registry connections in production
* Implement proper authentication for Schema Registry
* Validate schema compatibility before deployment

### Kafka Configuration

* Always use SSL/TLS for production Kafka connections
* Enable SASL authentication
* Use ACLs to restrict topic access
* Regularly rotate credentials

### Dead Letter Queue

* Set `IncludeStackTraceInDlq = false` in production to avoid exposing sensitive information
* Implement access controls for DLQ topics
* Monitor DLQ for anomalous patterns that might indicate security issues

### General

* Keep all dependencies up to date
* Regularly scan for known vulnerabilities
* Follow the principle of least privilege
* Implement proper logging and monitoring

## Disclosure Policy

When we receive a security bug report, we will:

1. Confirm the problem and determine the affected versions
2. Audit code to find any potential similar problems
3. Prepare fixes for all releases still under maintenance
4. Release new security fix versions as soon as possible

## Comments on this Policy

If you have suggestions on how this process could be improved, please submit a pull request.
