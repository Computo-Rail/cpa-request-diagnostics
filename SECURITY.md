# Security Policy

## Reporting a vulnerability

Do not open a public issue for a suspected secret disclosure or request-path
vulnerability. Use GitHub's private vulnerability reporting for this repository.

Include the affected release, CLIProxyAPI version, configuration, reproduction,
and whether request or credential material may have been exposed.

## Data boundary

This plugin is designed to ignore request bodies, response bodies, stream
payloads, raw errors, API keys, cookies, authorization headers, and arbitrary
metadata. The CLIProxyAPI ABI still transfers callback JSON through the plugin
process. The plugin minimizes exposure by not decoding body or metadata fields
and by decoding only values from its fixed set of supported identifier headers.
