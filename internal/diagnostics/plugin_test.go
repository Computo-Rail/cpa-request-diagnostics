package diagnostics

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
)

type capturedLog struct {
	callbackID string
	level      string
	message    string
	fields     map[string]any
}

type captureLogger struct {
	mu   sync.Mutex
	logs []capturedLog
}

type blockingLogger struct {
	captureLogger
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (l *blockingLogger) Log(callbackID, level, message string, fields map[string]any) {
	l.once.Do(func() { close(l.entered) })
	<-l.release
	l.captureLogger.Log(callbackID, level, message, fields)
}

type testClock struct {
	nanos atomic.Int64
}

func newTestClock(now time.Time) *testClock {
	clock := &testClock{}
	clock.nanos.Store(now.UnixNano())
	return clock
}

func (c *testClock) Now() time.Time {
	return time.Unix(0, c.nanos.Load()).UTC()
}

func (c *testClock) Advance(duration time.Duration) {
	c.nanos.Add(int64(duration))
}

func activeCount(plugin *Plugin) int {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	return len(plugin.active)
}

func pluginState(plugin *Plugin) (bool, int) {
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	return plugin.accepting, len(plugin.active)
}

func (l *captureLogger) Log(callbackID, level, message string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	copyFields := make(map[string]any, len(fields))
	for key, value := range fields {
		copyFields[key] = value
	}
	l.logs = append(l.logs, capturedLog{callbackID: callbackID, level: level, message: message, fields: copyFields})
}

func (l *captureLogger) snapshot() []capturedLog {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make([]capturedLog, len(l.logs))
	copy(result, l.logs)
	return result
}

func TestRegistrationDeclaresOnlyBodyBlindCapabilities(t *testing.T) {
	plugin := New(&captureLogger{})
	t.Cleanup(plugin.Shutdown)
	response := configureForTest(t, plugin, "enabled: true\npriority: 7\n")
	if !response.OK {
		t.Fatalf("registration failed: %+v", response.Error)
	}
	var registered map[string]any
	if err := json.Unmarshal(response.Result, &registered); err != nil {
		t.Fatalf("decode registration: %v", err)
	}
	capabilities, ok := registered["capabilities"].(map[string]any)
	if !ok {
		t.Fatalf("capabilities = %#v", registered["capabilities"])
	}
	want := []string{"request_interceptor", "request_lifecycle_plugin", "response_interceptor", "response_stream_interceptor"}
	if len(capabilities) != len(want) {
		t.Fatalf("capabilities = %#v, want only %v", capabilities, want)
	}
	for _, name := range want {
		if capabilities[name] != true {
			t.Errorf("capability %q = %#v, want true", name, capabilities[name])
		}
	}
	if _, exists := capabilities["usage_plugin"]; exists {
		t.Fatal("usage capability must not be registered")
	}
}

func TestRegistrationRejectsOldHostSchema(t *testing.T) {
	plugin := New(&captureLogger{})
	t.Cleanup(plugin.Shutdown)
	raw, err := json.Marshal(lifecycleRequest{SchemaVersion: minimumHostSchema - 1})
	if err != nil {
		t.Fatal(err)
	}
	response := decodeEnvelope(t, plugin.Call(pluginabi.MethodPluginRegister, raw))
	if response.OK || response.Error == nil || !strings.Contains(response.Error.Message, "unsupported") {
		t.Fatalf("response = %+v, want unsupported schema error", response)
	}
}

func TestConfigurationAcceptsOnlyDiagnosticIdentifierHeaders(t *testing.T) {
	for _, config := range []string{
		"correlation_request_headers: [Authorization]\n",
		"correlation_request_headers: [Cookie]\n",
		"upstream_response_headers: [Set-Cookie]\n",
		"upstream_response_headers: [X-Api-Key]\n",
		"upstream_response_headers: [X-Goog-Api-Key]\n",
		"upstream_response_headers: [X-Amz-Security-Token]\n",
		"upstream_response_headers: [Private-Token]\n",
		"upstream_response_headers: [X-Signature]\n",
		"upstream_response_headers: [X-Token]\n",
		"upstream_response_headers: [X-Key]\n",
		"upstream_response_headers: [X-Session]\n",
		"upstream_response_headers: [User-Id]\n",
		"upstream_response_headers: [X-Auth-Request-Id]\n",
		"upstream_response_headers: [X-Password-Request-Id]\n",
		"upstream_response_headers: [X-JWT-Trace-Id]\n",
		"upstream_response_headers: ['Bad Header']\n",
	} {
		t.Run(strings.TrimSpace(config), func(t *testing.T) {
			plugin := New(&captureLogger{})
			t.Cleanup(plugin.Shutdown)
			response := configureForTest(t, plugin, config)
			if response.OK || response.Error == nil || !strings.Contains(response.Error.Message, "only request, trace, or correlation ID") {
				t.Fatalf("response = %+v, want identifier-header error", response)
			}
		})
	}
	plugin := New(&captureLogger{})
	t.Cleanup(plugin.Shutdown)
	response := configureForTest(t, plugin, "correlation_request_headers: [Traceparent, X-Oneapi-Request-Id, X-Correlation-Id]\nupstream_response_headers: [Request-Id, X-Goog-Request-Id, X-Amzn-RequestId, X-OpenAI-Request-Id]\n")
	if !response.OK {
		t.Fatalf("supported identifier headers were rejected: %+v", response.Error)
	}
}

func TestStreamingLifecycleIgnoresBodiesSecretsAndRawErrors(t *testing.T) {
	logger := &captureLogger{}
	clock := newTestClock(time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC))
	plugin := newPlugin(logger, clock.Now)
	t.Cleanup(plugin.Shutdown)
	response := configureForTest(t, plugin, `
correlation_request_headers: [X-Oneapi-Request-Id]
upstream_response_headers: [Request-Id]
include_model: true
max_active_requests: 10
request_state_ttl: 1m
max_field_bytes: 64
sample_rate: 1
log_level: info
`)
	if !response.OK {
		t.Fatalf("configure failed: %+v", response.Error)
	}

	requestID := "abc12345"
	before := fmt.Sprintf(`{
  "RequestID":%q,
  "TraceID":"trace-safe",
  "SourceFormat":"openai",
  "Model":"alias-model",
  "RequestedModel":"alias-model",
  "Stream":true,
  "Headers":{"X-Oneapi-Request-Id":["external-safe"],"Authorization":["Bearer secret-header"]},
  "Body":"not-valid-base64%%secret-body",
  "host_callback_id":"callback-before"
}`, requestID)
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(before)))

	clock.Advance(3 * time.Millisecond)
	after := fmt.Sprintf(`{
  "RequestID":%q,
  "TraceID":"trace-safe",
  "SourceFormat":"openai",
  "ToFormat":"claude",
  "Model":"claude-sonnet",
  "RequestedModel":"alias-model",
  "Stream":true,
  "Headers":{"Authorization":["Bearer another-secret"]},
  "Body":"still-not-base64%%",
  "Metadata":{"selected_auth_index":"auth-safe-index","selected_auth_id":"secret-auth-path"},
  "host_callback_id":"callback-after"
}`, requestID)
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptAfter, []byte(after)))

	clock.Advance(7 * time.Millisecond)
	headerInit := fmt.Sprintf(`{
  "RequestID":%q,
  "ResponseHeaders":{"Request-Id":["provider-safe"],"Set-Cookie":["secret-cookie"]},
  "OriginalRequest":"invalid-base64%%secret-original",
  "RequestBody":"invalid-base64%%secret-upstream",
  "Body":"invalid-base64%%secret-stream",
  "HistoryChunks":["invalid-base64%%secret-history"],
  "ChunkIndex":-1,
  "host_callback_id":"callback-header"
}`, requestID)
	assertOK(t, plugin.Call(pluginabi.MethodResponseInterceptStreamChunk, []byte(headerInit)))

	clock.Advance(15 * time.Millisecond)
	chunk := fmt.Sprintf(`{"RequestID":%q,"Body":"invalid-base64%%secret-chunk","ChunkIndex":0,"host_callback_id":"callback-chunk"}`, requestID)
	assertOK(t, plugin.Call(pluginabi.MethodResponseInterceptStreamChunk, []byte(chunk)))

	clock.Advance(25 * time.Millisecond)
	completion := fmt.Sprintf(`{
  "RequestID":%q,
  "TraceID":"trace-safe",
  "Outcome":"failed",
  "StatusCode":429,
  "Error":"secret raw provider failure body",
  "Metadata":{"secret":"metadata-secret"},
  "host_callback_id":"callback-complete"
}`, requestID)
	assertOK(t, plugin.Call(pluginabi.MethodRequestComplete, []byte(completion)))

	logs := logger.snapshot()
	if len(logs) != 2 {
		t.Fatalf("logs = %#v, want selection and completion", logs)
	}
	selection := logs[0]
	if selection.callbackID != "callback-after" || selection.fields["event"] != "request_diagnostics_selection" {
		t.Fatalf("selection log = %#v", selection)
	}
	if selection.fields["selection_index"] != 0 || selection.fields["selection_elapsed_ms"] != int64(3) {
		t.Errorf("selection timing = %#v", selection.fields)
	}
	if selection.fields["model"] != "claude-sonnet" {
		t.Errorf("selection fields = %#v", selection.fields)
	}

	completionLog := logs[1]
	if completionLog.callbackID != "callback-complete" || completionLog.fields["event"] != "request_diagnostics_complete" {
		t.Fatalf("completion log = %#v", completionLog)
	}
	wantValues := map[string]any{
		"selection_count":       1,
		"response_headers_ms":   int64(10),
		"first_stream_event_ms": int64(25),
		"total_ms":              int64(50),
		"status":                429,
		"outcome":               "failed",
	}
	for key, want := range wantValues {
		if got := completionLog.fields[key]; got != want {
			t.Errorf("completion %s = %#v, want %#v", key, got, want)
		}
	}
	correlation, ok := completionLog.fields["correlation"].(map[string]string)
	if !ok || correlation["X-Oneapi-Request-Id"] != "external-safe" {
		t.Errorf("correlation = %#v", completionLog.fields["correlation"])
	}
	upstream, ok := completionLog.fields["upstream_request_ids"].(map[string]string)
	if !ok || upstream["Request-Id"] != "provider-safe" {
		t.Errorf("upstream_request_ids = %#v", completionLog.fields["upstream_request_ids"])
	}
	encoded, err := json.Marshal(logs)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret-header", "secret-body", "another-secret", "secret-auth-path", "secret-cookie", "secret-original", "secret-upstream", "secret-stream", "secret-history", "secret-chunk", "secret raw provider failure body", "metadata-secret"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Errorf("diagnostic logs leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestNonStreamingLifecycleCapturesHeadersWithoutModifyingResponse(t *testing.T) {
	logger := &captureLogger{}
	clock := newTestClock(time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC))
	plugin := newPlugin(logger, clock.Now)
	t.Cleanup(plugin.Shutdown)
	if response := configureForTest(t, plugin, "upstream_response_headers: [X-Goog-Request-Id]\n"); !response.OK {
		t.Fatalf("configure failed: %+v", response.Error)
	}
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{"RequestID":"req","Headers":{},"host_callback_id":"before"}`)))
	clock.Advance(12 * time.Millisecond)
	responseRaw := []byte(`{
  "RequestID":"req",
  "ResponseHeaders":{"X-Goog-Request-Id":["google-safe"]},
  "StatusCode":201,
  "OriginalRequest":"invalid-base64%%",
  "RequestBody":"invalid-base64%%",
  "Body":"invalid-base64%%secret",
  "host_callback_id":"response"
}`)
	responseEnvelope := decodeEnvelope(t, plugin.Call(pluginabi.MethodResponseInterceptAfter, responseRaw))
	if !responseEnvelope.OK || string(responseEnvelope.Result) != `{}` {
		t.Fatalf("response interceptor result = %s", responseEnvelope.Result)
	}
	clock.Advance(8 * time.Millisecond)
	assertOK(t, plugin.Call(pluginabi.MethodRequestComplete, []byte(`{"RequestID":"req","Outcome":"succeeded","StatusCode":201,"host_callback_id":"complete"}`)))
	logs := logger.snapshot()
	if len(logs) != 1 {
		t.Fatalf("logs = %#v, want one completion", logs)
	}
	if logs[0].fields["response_headers_ms"] != int64(12) || logs[0].fields["total_ms"] != int64(20) {
		t.Errorf("timings = %#v", logs[0].fields)
	}
	upstream := logs[0].fields["upstream_request_ids"].(map[string]string)
	if upstream["X-Goog-Request-Id"] != "google-safe" {
		t.Errorf("upstream IDs = %#v", upstream)
	}
}

func TestBoundsSamplingExpiryAndCapacityRemainFailOpen(t *testing.T) {
	logger := &captureLogger{}
	clock := newTestClock(time.Date(2026, 9, 14, 14, 0, 0, 0, time.UTC))
	plugin := newPlugin(logger, clock.Now)
	t.Cleanup(plugin.Shutdown)
	response := configureForTest(t, plugin, "max_active_requests: 1\nrequest_state_ttl: 1s\nsample_rate: 1\n")
	if !response.OK {
		t.Fatalf("configure failed: %+v", response.Error)
	}
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{"RequestID":"one","Headers":{}}`)))
	clock.Advance(100 * time.Millisecond)
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{"RequestID":"two","Headers":{},"host_callback_id":"capacity"}`)))
	assertOK(t, plugin.Call(pluginabi.MethodRequestComplete, []byte(`{"RequestID":"two","Outcome":"succeeded"}`)))
	if got := activeCount(plugin); got != 1 {
		t.Fatalf("active = %d, want first request only", got)
	}
	clock.Advance(2 * time.Second)
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{"RequestID":"three","Headers":{},"host_callback_id":"expiry"}`)))
	assertOK(t, plugin.Call(pluginabi.MethodRequestComplete, []byte(`{"RequestID":"three","Outcome":"succeeded","host_callback_id":"complete"}`)))

	logs := logger.snapshot()
	if len(logs) != 3 {
		t.Fatalf("logs = %#v, want capacity, expiry, completion", logs)
	}
	if logs[0].fields["event"] != "request_diagnostics_capacity_drop" {
		t.Errorf("first log = %#v", logs[0])
	}
	if logs[1].fields["event"] != "request_diagnostics_expired" || logs[1].fields["expired_count"] != 1 {
		t.Errorf("second log = %#v", logs[1])
	}

	unsampled := New(logger)
	t.Cleanup(unsampled.Shutdown)
	if response := configureForTest(t, unsampled, "sample_rate: 0\n"); !response.OK {
		t.Fatal(response.Error)
	}
	assertOK(t, unsampled.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{"RequestID":"never","Headers":{}}`)))
	assertOK(t, unsampled.Call(pluginabi.MethodRequestComplete, []byte(`{"RequestID":"never","Outcome":"succeeded"}`)))
	if activeCount(unsampled) != 0 {
		t.Fatal("sample_rate 0 retained request state")
	}
}

func TestInvalidAndOversizedValuesAreOmitted(t *testing.T) {
	logger := &captureLogger{}
	plugin := New(logger)
	t.Cleanup(plugin.Shutdown)
	response := configureForTest(t, plugin, "correlation_request_headers: [X-Request-Id]\ninclude_model: true\nmax_field_bytes: 16\n")
	if !response.OK {
		t.Fatal(response.Error)
	}
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{
  "RequestID":"request",
  "TraceID":"contains a space",
  "Model":"this-model-name-is-too-long",
  "Headers":{"X-Request-Id":["line\nbreak"]}
}`)))
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptAfter, []byte(`{
  "RequestID":"request",
  "Model":"still-too-long-for-limit",
  "Metadata":{"selected_auth_index":"also-too-long-for-limit"}
}`)))
	assertOK(t, plugin.Call(pluginabi.MethodRequestComplete, []byte(`{"RequestID":"request","Outcome":"unexpected","StatusCode":999}`)))
	logs := logger.snapshot()
	if len(logs) != 2 {
		t.Fatalf("logs = %#v", logs)
	}
	for _, log := range logs {
		for _, forbiddenKey := range []string{"trace_id", "model", "auth_index", "correlation", "status"} {
			if _, exists := log.fields[forbiddenKey]; exists {
				t.Errorf("field %q should be omitted: %#v", forbiddenKey, log.fields)
			}
		}
	}
	if logs[1].fields["outcome"] != "unknown" {
		t.Errorf("outcome = %#v", logs[1].fields["outcome"])
	}
}

func TestConcurrentLifecycles(t *testing.T) {
	logger := &captureLogger{}
	plugin := New(logger)
	t.Cleanup(plugin.Shutdown)
	response := configureForTest(t, plugin, "max_active_requests: 1000\n")
	if !response.OK {
		t.Fatal(response.Error)
	}
	const requests = 200
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			requestID := fmt.Sprintf("request-%d", index)
			assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(fmt.Sprintf(`{"RequestID":%q,"Headers":{}}`, requestID))))
			assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptAfter, []byte(fmt.Sprintf(`{"RequestID":%q,"Metadata":{"selected_auth_index":"auth"}}`, requestID))))
			assertOK(t, plugin.Call(pluginabi.MethodResponseInterceptAfter, []byte(fmt.Sprintf(`{"RequestID":%q,"ResponseHeaders":{},"StatusCode":200}`, requestID))))
			assertOK(t, plugin.Call(pluginabi.MethodRequestComplete, []byte(fmt.Sprintf(`{"RequestID":%q,"Outcome":"succeeded","StatusCode":200}`, requestID))))
		}(i)
	}
	wg.Wait()
	if got := len(logger.snapshot()); got != requests*2 {
		t.Fatalf("log count = %d, want %d", got, requests*2)
	}
	active := activeCount(plugin)
	if active != 0 {
		t.Fatalf("active requests = %d, want 0", active)
	}
}

func TestQuiesceAndShutdown(t *testing.T) {
	plugin := New(&captureLogger{})
	if response := configureForTest(t, plugin, ""); !response.OK {
		t.Fatal(response.Error)
	}
	assertOK(t, plugin.Call(pluginabi.MethodPluginQuiesce, nil))
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{"RequestID":"ignored","Headers":{}}`)))
	if activeCount(plugin) != 0 {
		t.Fatal("quiesced plugin accepted a request")
	}
	if response := configureForTest(t, plugin, ""); !response.OK {
		t.Fatal(response.Error)
	}
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{"RequestID":"active","Headers":{}}`)))
	plugin.Shutdown()
	accepting, active := pluginState(plugin)
	if accepting || active != 0 {
		t.Fatalf("shutdown state: accepting=%v active=%d", accepting, active)
	}
}

func TestExpiryWorkerRemovesIncompleteRequestsWithoutNewTraffic(t *testing.T) {
	logger := &captureLogger{}
	plugin := New(logger)
	t.Cleanup(plugin.Shutdown)
	if response := configureForTest(t, plugin, "request_state_ttl: 1s\n"); !response.OK {
		t.Fatal(response.Error)
	}
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{"RequestID":"abandoned","Headers":{}}`)))
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if activeCount(plugin) == 0 && len(logger.snapshot()) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if activeCount(plugin) != 0 {
		t.Fatal("expiry worker retained an incomplete request past its TTL")
	}
	logs := logger.snapshot()
	if len(logs) != 1 || logs[0].fields["event"] != "request_diagnostics_expired" {
		t.Fatalf("expiry logs = %#v", logs)
	}
}

func TestReconfigureRevokesPreviouslyCollectedOptionalFields(t *testing.T) {
	logger := &captureLogger{}
	plugin := New(logger)
	t.Cleanup(plugin.Shutdown)
	response := configureForTest(t, plugin, `
correlation_request_headers: [X-Request-Id]
upstream_response_headers: [Request-Id]
include_model: true
max_field_bytes: 64
`)
	if !response.OK {
		t.Fatal(response.Error)
	}
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{
  "RequestID":"request",
  "Model":"private-model",
  "Headers":{"X-Request-Id":["private-correlation"]}
}`)))
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptAfter, []byte(`{
  "RequestID":"request",
  "Model":"private-selected-model",
  "Metadata":{"selected_auth_index":"private-auth"}
}`)))
	assertOK(t, plugin.Call(pluginabi.MethodResponseInterceptAfter, []byte(`{
  "RequestID":"request",
  "ResponseHeaders":{"Request-Id":["private-provider-id"]},
  "StatusCode":200
}`)))

	raw, err := json.Marshal(lifecycleRequest{
		ConfigYAML:    []byte("enabled: true\npriority: 1\ncorrelation_request_headers: []\nupstream_response_headers: []\ninclude_model: false\n"),
		SchemaVersion: pluginabi.SchemaVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	reconfigured := decodeEnvelope(t, plugin.Call(pluginabi.MethodPluginReconfigure, raw))
	if !reconfigured.OK {
		t.Fatal(reconfigured.Error)
	}
	assertOK(t, plugin.Call(pluginabi.MethodRequestComplete, []byte(`{"RequestID":"request","Outcome":"succeeded","StatusCode":200}`)))

	logs := logger.snapshot()
	completion := logs[len(logs)-1]
	encoded, err := json.Marshal(completion.fields)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"private-model", "private-selected-model", "private-auth", "private-correlation", "private-provider-id"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Errorf("reconfigured completion retained %q: %s", forbidden, encoded)
		}
	}
}

func TestRestrictiveReconfigureWaitsForOldPolicyLogs(t *testing.T) {
	logger := &blockingLogger{entered: make(chan struct{}), release: make(chan struct{})}
	plugin := New(logger)
	t.Cleanup(plugin.Shutdown)
	if response := configureForTest(t, plugin, "include_model: true\n"); !response.OK {
		t.Fatal(response.Error)
	}
	assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(`{"RequestID":"request","Model":"private-model","Headers":{}}`)))

	selectionDone := make(chan struct{})
	go func() {
		plugin.Call(pluginabi.MethodRequestInterceptAfter, []byte(`{"RequestID":"request","Model":"private-model"}`))
		close(selectionDone)
	}()
	<-logger.entered

	reconfigureDone := make(chan []byte, 1)
	go func() {
		raw, err := json.Marshal(lifecycleRequest{
			ConfigYAML:    []byte("include_model: false\n"),
			SchemaVersion: pluginabi.SchemaVersion,
		})
		if err != nil {
			reconfigureDone <- nil
			return
		}
		reconfigureDone <- plugin.Call(pluginabi.MethodPluginReconfigure, raw)
	}()
	select {
	case <-reconfigureDone:
		close(logger.release)
		t.Fatal("reconfiguration returned while an old-policy log was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(logger.release)
	<-selectionDone
	if response := decodeEnvelope(t, <-reconfigureDone); !response.OK {
		t.Fatalf("reconfigure failed: %+v", response.Error)
	}
	assertOK(t, plugin.Call(pluginabi.MethodRequestComplete, []byte(`{"RequestID":"request","Outcome":"succeeded"}`)))
	logs := logger.snapshot()
	if len(logs) != 2 {
		t.Fatalf("logs = %#v, want selection before and completion after reconfiguration", logs)
	}
	if _, exists := logs[1].fields["model"]; exists {
		t.Fatalf("post-reconfiguration completion retained model: %#v", logs[1].fields)
	}
}

func TestReconfigureAppliesReducedSamplingAndCapacityToActiveState(t *testing.T) {
	plugin := New(&captureLogger{})
	t.Cleanup(plugin.Shutdown)
	if response := configureForTest(t, plugin, "max_active_requests: 10\nsample_rate: 1\n"); !response.OK {
		t.Fatal(response.Error)
	}
	for index := 0; index < 10; index++ {
		requestID := fmt.Sprintf("active-%d", index)
		assertOK(t, plugin.Call(pluginabi.MethodRequestInterceptBefore, []byte(fmt.Sprintf(`{"RequestID":%q,"Headers":{}}`, requestID))))
	}
	raw, err := json.Marshal(lifecycleRequest{
		ConfigYAML:    []byte("max_active_requests: 2\nsample_rate: 0.5\n"),
		SchemaVersion: pluginabi.SchemaVersion,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response := decodeEnvelope(t, plugin.Call(pluginabi.MethodPluginReconfigure, raw)); !response.OK {
		t.Fatal(response.Error)
	}
	plugin.mu.Lock()
	defer plugin.mu.Unlock()
	if len(plugin.active) > 2 {
		t.Fatalf("active requests = %d, want at most 2", len(plugin.active))
	}
	for requestID := range plugin.active {
		if !sampled(requestID, 0.5) {
			t.Errorf("request %q remained active after reduced sampling", requestID)
		}
	}
}

func configureForTest(t *testing.T, plugin *Plugin, yaml string) envelope {
	t.Helper()
	raw, err := json.Marshal(lifecycleRequest{ConfigYAML: []byte(yaml), SchemaVersion: pluginabi.SchemaVersion})
	if err != nil {
		t.Fatal(err)
	}
	return decodeEnvelope(t, plugin.Call(pluginabi.MethodPluginRegister, raw))
}

func assertOK(t *testing.T, raw []byte) {
	t.Helper()
	response := decodeEnvelope(t, raw)
	if !response.OK {
		t.Fatalf("plugin call failed: %+v", response.Error)
	}
	if string(response.Result) != `{}` {
		t.Fatalf("plugin modified callback result: %s", response.Result)
	}
}

func decodeEnvelope(t *testing.T, raw []byte) envelope {
	t.Helper()
	var response envelope
	if err := json.Unmarshal(raw, &response); err != nil {
		t.Fatalf("decode envelope %q: %v", raw, err)
	}
	return response
}
