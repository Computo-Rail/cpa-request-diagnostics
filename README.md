# CPA Request Diagnostics

A body-blind, provider-neutral request lifecycle diagnostics plugin for
[CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI).

The plugin emits bounded structured lifecycle records through CLIProxyAPI's host
logger. It does not register usage, management, auth, scheduler, executor, or
storage capabilities, and it never modifies proxy requests or responses.

## What it observes

- CLIProxyAPI request ID plus the plugin lifecycle and trace IDs
- configured external correlation headers
- requested and selected model names when explicitly enabled
- credential selection count
- response-header, first-stream-event, and total durations
- terminal status and outcome
- configured upstream response request-ID headers

`selection_count` is an observation count. It is not an exact retry count or a
provider-attempt result trail because CLIProxyAPI v7.2.159 does not expose those
events through the plugin ABI.

## What it does not collect

- request, response, or stream payload content
- raw error messages or failure bodies
- API keys, authorization, cookies, or credential files
- token usage or billing information
- query strings or arbitrary metadata

Only the supported request, trace, and correlation ID header names shown in the
configuration example are accepted. Arbitrary names are rejected rather than
classified heuristically.

## Compatibility

- CLIProxyAPI v7.2.159
- native plugin ABI 1
- RPC schema 5 or newer
- Go 1.26 or newer for building

Schema 5 is required so payload stream callbacks omit request bodies and stream
history. The plugin's minimal wire decoders also omit all body fields.

## Build and test

```bash
make check
make host-smoke
```

`host-smoke` builds the shared library and loads it through the exact pinned
CLIProxyAPI v7.2.159 commit. Set `CLIPROXY_SOURCE` to seed an isolated temporary
clone from a local checkout; the source checkout is never modified.

The shared library is written to
`bin/<goos>/<goarch>/cpa-request-diagnostics.<extension>`. Its filename must
remain the plugin ID.

Build a versioned release artifact with one authoritative version input:

```bash
make release VERSION=0.1.0
```

This embeds `0.1.0` in plugin metadata and writes
`cpa-request-diagnostics-v0.1.0.<extension>`.

## Configuration

The shared-library basename must match the plugin ID
`cpa-request-diagnostics`.

```yaml
plugins:
  enabled: true
  dir: /absolute/path/to/bin
  configs:
    cpa-request-diagnostics:
      enabled: true
      priority: 1
      correlation_request_headers:
        - X-Request-Id
        - Traceparent
        - X-Oneapi-Request-Id
        - X-Correlation-Id
        - X-Cpa-Trace-Id
        - X-Trace-Id
      upstream_response_headers:
        - Request-Id
        - X-Request-Id
        - X-Goog-Request-Id
        - X-Amzn-RequestId
        - X-OpenAI-Request-Id
      include_model: true
      max_active_requests: 10000
      request_state_ttl: 10m
      max_field_bytes: 256
      sample_rate: 1.0
      log_level: info
```

Safe defaults collect standard correlation headers, omit model details, retain
at most 10,000 active request states for 10 minutes, and sample all requests.

`lifecycle_id` is the plugin's CLIProxy lifecycle key. `request_id` is not
emitted by the plugin; CLIProxyAPI's host logger injects it from the callback
context and may overwrite a same-named plugin field. Expiry housekeeping logs
have no callback context, so they contain neither identifier.

CLIProxyAPI searches `<dir>/<goos>/<goarch>/` before the directory root. The host
recognizes the release target's `-v<version>` suffix while preserving the plugin
ID.

## Record examples

Selection:

```json
{
  "event": "request_diagnostics_selection",
  "schema_version": 1,
  "request_id": "20260914123000-2-a1b2c3d4",
  "lifecycle_id": "a1b2c3d4",
  "trace_id": "20260914123000-2-a1b2c3d4",
  "selection_index": 0,
  "selection_elapsed_ms": 3
}
```

Completion:

```json
{
  "event": "request_diagnostics_complete",
  "schema_version": 1,
  "request_id": "20260914123000-2-a1b2c3d4",
  "lifecycle_id": "a1b2c3d4",
  "trace_id": "20260914123000-2-a1b2c3d4",
  "correlation": {
    "X-Request-Id": "external-safe-id"
  },
  "selection_count": 1,
  "response_headers_ms": 1260,
  "first_stream_event_ms": 1480,
  "total_ms": 3860,
  "status": 200,
  "outcome": "succeeded",
  "upstream_request_ids": {
    "Request-Id": "provider-safe-id"
  }
}
```

Durations are measured by the plugin's process-local monotonic clock. Records
from different hosts must not be subtracted to manufacture network latency.

## License

MIT
