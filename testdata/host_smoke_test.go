package pluginhost

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

func TestExternalDiagnosticsPluginSmoke(t *testing.T) {
	pluginDir := os.Getenv("CPA_DIAGNOSTICS_PLUGIN_DIR")
	if pluginDir == "" {
		t.Fatal("CPA_DIAGNOSTICS_PLUGIN_DIR is required")
	}
	rawConfig := fmt.Sprintf(`
plugins:
  enabled: true
  dir: %q
  configs:
    cpa-request-diagnostics:
      enabled: true
      priority: 7
      correlation_request_headers: [X-Oneapi-Request-Id]
      upstream_response_headers: [Request-Id]
      include_model: true
`, pluginDir)
	var cfg config.Config
	if err := yaml.Unmarshal([]byte(rawConfig), &cfg); err != nil {
		t.Fatal(err)
	}
	host := New()
	t.Cleanup(func() { host.ShutdownAll() })
	host.ApplyConfig(context.Background(), &cfg)
	plugins := host.RegisteredPlugins()
	if len(plugins) != 1 || plugins[0].ID != "cpa-request-diagnostics" || plugins[0].Priority != 7 {
		t.Fatalf("unexpected registration: %#v", plugins)
	}
	if host.StreamChunkPayloadIncludesRequestBody() || host.StreamChunkPayloadIncludesHistory() {
		t.Fatal("plugin requested stream payload request bodies or history")
	}

	request := pluginapi.RequestInterceptRequest{
		RequestID: "smoke-lifecycle",
		TraceID:   "smoke-trace",
		Model:     "smoke-model",
		Stream:    true,
		Headers:   map[string][]string{"X-Oneapi-Request-Id": {"external-id"}},
		Body:      []byte("must-not-be-observed"),
	}
	before := host.InterceptRequestBeforeAuth(context.Background(), request)
	if before.Terminate || string(before.Body) != string(request.Body) {
		t.Fatalf("request was modified: %#v", before)
	}
	host.InterceptRequestAfterAuth(context.Background(), request)
	stream := host.InterceptStreamChunk(context.Background(), pluginapi.StreamChunkInterceptRequest{
		RequestID:       request.RequestID,
		ResponseHeaders: map[string][]string{"Request-Id": {"provider-id"}},
		ChunkIndex:      0,
		Body:            []byte("must-not-be-observed"),
	})
	if stream.DropChunk || string(stream.Body) != "must-not-be-observed" {
		t.Fatalf("stream was modified: %#v", stream)
	}
	host.CompleteRequest(context.Background(), pluginapi.RequestCompletion{
		RequestID:  request.RequestID,
		TraceID:    request.TraceID,
		Model:      request.Model,
		Stream:     true,
		Outcome:    "succeeded",
		StatusCode: 200,
	})
	time.Sleep(100 * time.Millisecond)

	rawConfig = fmt.Sprintf(`
plugins:
  enabled: true
  dir: %q
  configs:
    cpa-request-diagnostics:
      enabled: true
      priority: 3
      correlation_request_headers: []
      upstream_response_headers: []
      include_model: false
`, pluginDir)
	if err := yaml.Unmarshal([]byte(rawConfig), &cfg); err != nil {
		t.Fatal(err)
	}
	host.ApplyConfig(context.Background(), &cfg)
	plugins = host.RegisteredPlugins()
	if len(plugins) != 1 || plugins[0].Priority != 3 {
		t.Fatalf("unexpected reconfiguration: %#v", plugins)
	}

	configured := cfg
	disabled := false
	item := cfg.Plugins.Configs["cpa-request-diagnostics"]
	item.Enabled = &disabled
	cfg.Plugins.Configs["cpa-request-diagnostics"] = item
	host.ApplyConfig(context.Background(), &cfg)
	if plugins := host.RegisteredPlugins(); len(plugins) != 0 {
		t.Fatalf("plugin remained registered after disable: %#v", plugins)
	}
	enabled := true
	item = configured.Plugins.Configs["cpa-request-diagnostics"]
	item.Enabled = &enabled
	configured.Plugins.Configs["cpa-request-diagnostics"] = item
	host.ApplyConfig(context.Background(), &configured)
	plugins = host.RegisteredPlugins()
	if len(plugins) != 1 || plugins[0].Priority != 3 {
		t.Fatalf("plugin did not reinitialize after disable: %#v", plugins)
	}

	host.ShutdownAll()
	host = New()
	host.ApplyConfig(context.Background(), &configured)
	plugins = host.RegisteredPlugins()
	if len(plugins) != 1 || plugins[0].Priority != 3 {
		t.Fatalf("plugin did not reinitialize after native shutdown: %#v", plugins)
	}
}
