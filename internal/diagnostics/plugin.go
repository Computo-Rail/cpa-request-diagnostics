package diagnostics

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

const (
	pluginName        = "cpa-request-diagnostics"
	recordSchema      = 1
	minimumHostSchema = pluginabi.SchemaVersionStreamChunkOmitHistory
)

var Version = "dev"

type HostLogger interface {
	Log(callbackID, level, message string, fields map[string]any)
}

type Plugin struct {
	mu                  sync.Mutex
	logMu               sync.Mutex
	config              config
	revision            uint64
	active              map[string]*requestState
	now                 func() time.Time
	logger              HostLogger
	accepting           bool
	lastCapacityWarning time.Time
	wake                chan struct{}
	stop                chan struct{}
	done                chan struct{}
	shutdownOnce        sync.Once
}

type config struct {
	Enabled                   bool     `yaml:"enabled"`
	Priority                  int      `yaml:"priority"`
	CorrelationRequestHeaders []string `yaml:"correlation_request_headers"`
	UpstreamResponseHeaders   []string `yaml:"upstream_response_headers"`
	IncludeModel              bool     `yaml:"include_model"`
	MaxActiveRequests         int      `yaml:"max_active_requests"`
	RequestStateTTL           string   `yaml:"request_state_ttl"`
	MaxFieldBytes             int      `yaml:"max_field_bytes"`
	SampleRate                float64  `yaml:"sample_rate"`
	LogLevel                  string   `yaml:"log_level"`

	ttl               time.Duration
	correlationSet    map[string]string
	upstreamHeaderSet map[string]string
}

type requestState struct {
	startedAt          time.Time
	traceID            string
	correlation        map[string]string
	sourceFormat       string
	requestedModel     string
	model              string
	toFormat           string
	stream             bool
	selectionCount     int
	responseHeadersMS  *int64
	firstStreamEventMS *int64
	upstreamRequestIDs map[string]string
	responseStatus     int
}

type lifecycleRequest struct {
	ConfigYAML    []byte `json:"config_yaml"`
	SchemaVersion uint32 `json:"schema_version"`
}

type registration struct {
	SchemaVersion uint32                   `json:"schema_version"`
	Metadata      pluginapi.Metadata       `json:"metadata"`
	Capabilities  registrationCapabilities `json:"capabilities"`
}

type registrationCapabilities struct {
	RequestInterceptor     bool `json:"request_interceptor"`
	RequestLifecyclePlugin bool `json:"request_lifecycle_plugin"`
	ResponseInterceptor    bool `json:"response_interceptor"`
	StreamChunkInterceptor bool `json:"response_stream_interceptor"`
}

type requestWire struct {
	RequestID      string
	TraceID        string
	SourceFormat   string
	ToFormat       string
	Model          string
	RequestedModel string
	Stream         bool
	Headers        json.RawMessage
	HostCallbackID string `json:"host_callback_id"`
}

type responseWire struct {
	RequestID       string
	ResponseHeaders json.RawMessage
	StatusCode      int
	HostCallbackID  string `json:"host_callback_id"`
}

type streamWire struct {
	RequestID       string
	ResponseHeaders json.RawMessage
	ChunkIndex      int
	HostCallbackID  string `json:"host_callback_id"`
}

type completionWire struct {
	RequestID      string
	TraceID        string
	SourceFormat   string
	Model          string
	RequestedModel string
	Stream         bool
	Outcome        string
	StatusCode     int
	HostCallbackID string `json:"host_callback_id"`
}

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func New(logger HostLogger) *Plugin {
	return newPlugin(logger, time.Now)
}

func newPlugin(logger HostLogger, now func() time.Time) *Plugin {
	plugin := &Plugin{
		config:    defaultConfig(),
		active:    make(map[string]*requestState),
		now:       now,
		logger:    logger,
		accepting: true,
		wake:      make(chan struct{}, 1),
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go plugin.expiryLoop()
	return plugin
}

func (p *Plugin) Call(method string, raw []byte) []byte {
	var result any
	var err error
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		result, err = p.configure(raw)
	case pluginabi.MethodPluginQuiesce:
		p.mu.Lock()
		p.accepting = false
		p.mu.Unlock()
		result = struct{}{}
	case pluginabi.MethodPluginShutdown:
		p.Shutdown()
		result = struct{}{}
	case pluginabi.MethodRequestInterceptBefore:
		result = p.interceptBefore(raw)
	case pluginabi.MethodRequestInterceptAfter:
		result = p.interceptAfter(raw)
	case pluginabi.MethodResponseInterceptAfter:
		result = p.interceptResponse(raw)
	case pluginabi.MethodResponseInterceptStreamChunk:
		result = p.interceptStream(raw)
	case pluginabi.MethodRequestComplete:
		result = p.complete(raw)
	default:
		err = fmt.Errorf("unknown method: %s", method)
	}
	if err != nil {
		return marshalEnvelope(envelope{OK: false, Error: &envelopeError{Code: "plugin_error", Message: err.Error()}})
	}
	resultJSON, errMarshal := json.Marshal(result)
	if errMarshal != nil {
		return marshalEnvelope(envelope{OK: false, Error: &envelopeError{Code: "plugin_error", Message: errMarshal.Error()}})
	}
	return marshalEnvelope(envelope{OK: true, Result: resultJSON})
}

func (p *Plugin) Shutdown() {
	p.shutdownOnce.Do(func() {
		close(p.stop)
		<-p.done
		p.logMu.Lock()
		defer p.logMu.Unlock()
		p.mu.Lock()
		p.accepting = false
		p.active = make(map[string]*requestState)
		p.mu.Unlock()
	})
}

func (p *Plugin) configure(raw []byte) (registration, error) {
	var req lifecycleRequest
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return registration{}, fmt.Errorf("decode lifecycle request: %w", err)
		}
	}
	if req.SchemaVersion < minimumHostSchema {
		return registration{}, fmt.Errorf("host schema version %d is unsupported; version %d or newer is required", req.SchemaVersion, minimumHostSchema)
	}

	cfg := defaultConfig()
	if len(req.ConfigYAML) > 0 {
		decoder := yaml.NewDecoder(bytes.NewReader(req.ConfigYAML))
		decoder.KnownFields(true)
		if err := decoder.Decode(&cfg); err != nil {
			return registration{}, errors.New("invalid plugin configuration")
		}
	}
	if err := cfg.normalize(); err != nil {
		return registration{}, err
	}

	p.logMu.Lock()
	p.mu.Lock()
	p.config = cfg
	p.revision++
	p.accepting = true
	p.expireLocked(p.now())
	p.scrubActiveLocked()
	p.mu.Unlock()
	p.logMu.Unlock()
	p.signalExpiryLoop()

	return pluginRegistration(), nil
}

func pluginRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             pluginName,
			Version:          Version,
			Author:           "ComputoRail",
			GitHubRepository: "https://github.com/Computo-Rail/cpa-request-diagnostics",
			ConfigFields: []pluginapi.ConfigField{
				{Name: "correlation_request_headers", Type: pluginapi.ConfigFieldTypeArray, Description: "Request headers copied into the bounded correlation map."},
				{Name: "upstream_response_headers", Type: pluginapi.ConfigFieldTypeArray, Description: "Response headers copied into the bounded upstream request-ID map."},
				{Name: "include_model", Type: pluginapi.ConfigFieldTypeBoolean, Description: "Include bounded requested and selected model names."},
				{Name: "max_active_requests", Type: pluginapi.ConfigFieldTypeInteger, Description: "Maximum request lifecycle states retained in memory."},
				{Name: "request_state_ttl", Type: pluginapi.ConfigFieldTypeString, Description: "Maximum lifetime of an incomplete request state, for example 10m."},
				{Name: "max_field_bytes", Type: pluginapi.ConfigFieldTypeInteger, Description: "Maximum byte length of any collected string field."},
				{Name: "sample_rate", Type: pluginapi.ConfigFieldTypeNumber, Description: "Deterministic request sample rate from 0 through 1."},
				{Name: "log_level", Type: pluginapi.ConfigFieldTypeEnum, EnumValues: []string{"debug", "info", "warn"}, Description: "Host log level for diagnostic records."},
			},
		},
		Capabilities: registrationCapabilities{
			RequestInterceptor:     true,
			RequestLifecyclePlugin: true,
			ResponseInterceptor:    true,
			StreamChunkInterceptor: true,
		},
	}
}

func defaultConfig() config {
	return config{
		CorrelationRequestHeaders: []string{"X-Request-Id", "Traceparent"},
		UpstreamResponseHeaders:   []string{"Request-Id", "X-Request-Id"},
		MaxActiveRequests:         10000,
		RequestStateTTL:           "10m",
		MaxFieldBytes:             256,
		SampleRate:                1,
		LogLevel:                  "info",
	}
}

func (c *config) normalize() error {
	if c.MaxActiveRequests < 1 || c.MaxActiveRequests > 1_000_000 {
		return fmt.Errorf("max_active_requests must be between 1 and 1000000")
	}
	if c.MaxFieldBytes < 16 || c.MaxFieldBytes > 4096 {
		return fmt.Errorf("max_field_bytes must be between 16 and 4096")
	}
	if math.IsNaN(c.SampleRate) || math.IsInf(c.SampleRate, 0) || c.SampleRate < 0 || c.SampleRate > 1 {
		return fmt.Errorf("sample_rate must be between 0 and 1")
	}
	ttl, err := time.ParseDuration(strings.TrimSpace(c.RequestStateTTL))
	if err != nil || ttl < time.Second || ttl > 24*time.Hour {
		return fmt.Errorf("request_state_ttl must be between 1s and 24h")
	}
	c.ttl = ttl
	c.RequestStateTTL = ttl.String()
	c.LogLevel = strings.ToLower(strings.TrimSpace(c.LogLevel))
	if c.LogLevel != "debug" && c.LogLevel != "info" && c.LogLevel != "warn" {
		return fmt.Errorf("log_level must be debug, info, or warn")
	}
	c.correlationSet, err = normalizeHeaders(c.CorrelationRequestHeaders)
	if err != nil {
		return fmt.Errorf("correlation_request_headers: %w", err)
	}
	c.upstreamHeaderSet, err = normalizeHeaders(c.UpstreamResponseHeaders)
	if err != nil {
		return fmt.Errorf("upstream_response_headers: %w", err)
	}
	c.CorrelationRequestHeaders = sortedHeaderNames(c.correlationSet)
	c.UpstreamResponseHeaders = sortedHeaderNames(c.upstreamHeaderSet)
	return nil
}

func normalizeHeaders(values []string) (map[string]string, error) {
	result := make(map[string]string, len(values))
	for _, value := range values {
		name := http.CanonicalHeaderKey(strings.TrimSpace(value))
		if name == "" {
			return nil, fmt.Errorf("header names must not be empty")
		}
		if !isIdentifierHeader(name) {
			return nil, errors.New("only request, trace, or correlation ID headers are allowed")
		}
		result[strings.ToLower(name)] = name
	}
	return result, nil
}

func sortedHeaderNames(values map[string]string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func isIdentifierHeader(name string) bool {
	_, ok := supportedIdentifierHeaders[strings.ToLower(http.CanonicalHeaderKey(strings.TrimSpace(name)))]
	return ok
}

var supportedIdentifierHeaders = map[string]struct{}{
	"request-id":          {},
	"traceparent":         {},
	"x-amzn-requestid":    {},
	"x-correlation-id":    {},
	"x-cpa-trace-id":      {},
	"x-goog-request-id":   {},
	"x-oneapi-request-id": {},
	"x-openai-request-id": {},
	"x-request-id":        {},
	"x-trace-id":          {},
}

func (p *Plugin) interceptBefore(raw []byte) struct{} {
	var req requestWire
	if json.Unmarshal(raw, &req) != nil || strings.TrimSpace(req.RequestID) == "" {
		return struct{}{}
	}
	now := p.now()

	p.mu.Lock()
	cfg := p.config
	revision := p.revision
	expired := p.expireLocked(now)
	if !p.accepting || !sampled(req.RequestID, cfg.SampleRate) {
		p.mu.Unlock()
		return struct{}{}
	}
	if _, exists := p.active[req.RequestID]; exists {
		p.mu.Unlock()
		return struct{}{}
	}
	if len(p.active) >= cfg.MaxActiveRequests {
		warn := p.lastCapacityWarning.IsZero() || now.Sub(p.lastCapacityWarning) >= time.Minute
		if warn {
			p.lastCapacityWarning = now
		}
		p.mu.Unlock()
		if warn {
			p.logPolicy(revision, req.HostCallbackID, cfg.LogLevel, map[string]any{
				"event":               "request_diagnostics_capacity_drop",
				"schema_version":      recordSchema,
				"max_active_requests": cfg.MaxActiveRequests,
			})
		}
		return struct{}{}
	}
	p.active[req.RequestID] = &requestState{
		startedAt:      now,
		traceID:        sanitize(req.TraceID, cfg.MaxFieldBytes),
		correlation:    extractHeaders(req.Headers, cfg.correlationSet, cfg.MaxFieldBytes),
		sourceFormat:   sanitize(req.SourceFormat, cfg.MaxFieldBytes),
		requestedModel: optionalSanitize(req.RequestedModel, cfg.IncludeModel, cfg.MaxFieldBytes),
		model:          optionalSanitize(req.Model, cfg.IncludeModel, cfg.MaxFieldBytes),
		stream:         req.Stream,
	}
	p.mu.Unlock()
	p.signalExpiryLoop()

	if expired > 0 {
		p.logPolicy(revision, req.HostCallbackID, cfg.LogLevel, map[string]any{
			"event":          "request_diagnostics_expired",
			"schema_version": recordSchema,
			"expired_count":  expired,
		})
	}
	return struct{}{}
}

func (p *Plugin) interceptAfter(raw []byte) struct{} {
	var req requestWire
	if json.Unmarshal(raw, &req) != nil || strings.TrimSpace(req.RequestID) == "" {
		return struct{}{}
	}
	now := p.now()

	p.mu.Lock()
	cfg := p.config
	revision := p.revision
	state := p.active[req.RequestID]
	if state == nil {
		p.mu.Unlock()
		return struct{}{}
	}
	selectionIndex := state.selectionCount
	state.selectionCount++
	state.toFormat = sanitize(req.ToFormat, cfg.MaxFieldBytes)
	if cfg.IncludeModel {
		state.model = sanitize(req.Model, cfg.MaxFieldBytes)
		state.requestedModel = sanitize(req.RequestedModel, cfg.MaxFieldBytes)
	}
	fields := p.baseFieldsLocked(req.RequestID, state)
	fields["event"] = "request_diagnostics_selection"
	fields["selection_index"] = selectionIndex
	fields["selection_elapsed_ms"] = elapsedMS(state.startedAt, now)
	if state.toFormat != "" {
		fields["to_format"] = state.toFormat
	}
	p.mu.Unlock()
	p.logPolicy(revision, req.HostCallbackID, cfg.LogLevel, fields)
	return struct{}{}
}

func (p *Plugin) interceptResponse(raw []byte) struct{} {
	var req responseWire
	if json.Unmarshal(raw, &req) != nil || strings.TrimSpace(req.RequestID) == "" {
		return struct{}{}
	}
	now := p.now()
	p.mu.Lock()
	cfg := p.config
	if state := p.active[req.RequestID]; state != nil {
		ms := elapsedMS(state.startedAt, now)
		state.responseHeadersMS = &ms
		state.responseStatus = req.StatusCode
		state.upstreamRequestIDs = extractHeaders(req.ResponseHeaders, cfg.upstreamHeaderSet, cfg.MaxFieldBytes)
	}
	p.mu.Unlock()
	return struct{}{}
}

func (p *Plugin) interceptStream(raw []byte) struct{} {
	var req streamWire
	if json.Unmarshal(raw, &req) != nil || strings.TrimSpace(req.RequestID) == "" {
		return struct{}{}
	}
	now := p.now()
	p.mu.Lock()
	cfg := p.config
	if state := p.active[req.RequestID]; state != nil {
		ms := elapsedMS(state.startedAt, now)
		if req.ChunkIndex == pluginapi.StreamChunkHeaderInitIndex {
			state.responseHeadersMS = &ms
			state.upstreamRequestIDs = extractHeaders(req.ResponseHeaders, cfg.upstreamHeaderSet, cfg.MaxFieldBytes)
		} else if req.ChunkIndex >= 0 && state.firstStreamEventMS == nil {
			state.firstStreamEventMS = &ms
		}
	}
	p.mu.Unlock()
	return struct{}{}
}

func (p *Plugin) complete(raw []byte) struct{} {
	var req completionWire
	if json.Unmarshal(raw, &req) != nil || strings.TrimSpace(req.RequestID) == "" {
		return struct{}{}
	}
	now := p.now()
	p.mu.Lock()
	cfg := p.config
	revision := p.revision
	state := p.active[req.RequestID]
	if state == nil {
		p.mu.Unlock()
		return struct{}{}
	}
	delete(p.active, req.RequestID)
	fields := p.baseFieldsLocked(req.RequestID, state)
	fields["event"] = "request_diagnostics_complete"
	fields["selection_count"] = state.selectionCount
	fields["total_ms"] = elapsedMS(state.startedAt, now)
	fields["outcome"] = sanitizeOutcome(req.Outcome)
	status := req.StatusCode
	if status == 0 {
		status = state.responseStatus
	}
	if status >= 100 && status <= 599 {
		fields["status"] = status
	}
	if state.responseHeadersMS != nil {
		fields["response_headers_ms"] = *state.responseHeadersMS
	}
	if state.firstStreamEventMS != nil {
		fields["first_stream_event_ms"] = *state.firstStreamEventMS
	}
	if len(state.upstreamRequestIDs) > 0 {
		fields["upstream_request_ids"] = cloneStrings(state.upstreamRequestIDs)
	}
	p.mu.Unlock()
	p.logPolicy(revision, req.HostCallbackID, cfg.LogLevel, fields)
	return struct{}{}
}

func (p *Plugin) baseFieldsLocked(requestID string, state *requestState) map[string]any {
	fields := map[string]any{
		"schema_version": recordSchema,
		"plugin_version": Version,
		"lifecycle_id":   requestID,
		"stream":         state.stream,
	}
	if state.traceID != "" {
		fields["trace_id"] = state.traceID
	}
	if state.sourceFormat != "" {
		fields["source_format"] = state.sourceFormat
	}
	if state.requestedModel != "" {
		fields["requested_model"] = state.requestedModel
	}
	if state.model != "" {
		fields["model"] = state.model
	}
	if len(state.correlation) > 0 {
		fields["correlation"] = cloneStrings(state.correlation)
	}
	return fields
}

func (p *Plugin) expireLocked(now time.Time) int {
	expired := 0
	for requestID, state := range p.active {
		if now.Sub(state.startedAt) >= p.config.ttl {
			delete(p.active, requestID)
			expired++
		}
	}
	return expired
}

func (p *Plugin) scrubActiveLocked() {
	if p.config.SampleRate == 0 {
		p.active = make(map[string]*requestState)
		return
	}
	for requestID, state := range p.active {
		if !sampled(requestID, p.config.SampleRate) {
			delete(p.active, requestID)
			continue
		}
		if !p.config.IncludeModel {
			state.requestedModel = ""
			state.model = ""
		}
		state.traceID = sanitize(state.traceID, p.config.MaxFieldBytes)
		state.sourceFormat = sanitize(state.sourceFormat, p.config.MaxFieldBytes)
		state.toFormat = sanitize(state.toFormat, p.config.MaxFieldBytes)
		state.requestedModel = sanitize(state.requestedModel, p.config.MaxFieldBytes)
		state.model = sanitize(state.model, p.config.MaxFieldBytes)
		state.correlation = filterCollectedHeaders(state.correlation, p.config.correlationSet, p.config.MaxFieldBytes)
		state.upstreamRequestIDs = filterCollectedHeaders(state.upstreamRequestIDs, p.config.upstreamHeaderSet, p.config.MaxFieldBytes)
	}
	for len(p.active) > p.config.MaxActiveRequests {
		var oldestID string
		var oldest time.Time
		for requestID, state := range p.active {
			if oldestID == "" || state.startedAt.Before(oldest) {
				oldestID = requestID
				oldest = state.startedAt
			}
		}
		delete(p.active, oldestID)
	}
}

func (p *Plugin) expiryLoop() {
	defer close(p.done)
	for {
		p.mu.Lock()
		wait := time.Hour
		now := p.now()
		for _, state := range p.active {
			remaining := p.config.ttl - now.Sub(state.startedAt)
			if remaining < wait {
				wait = remaining
			}
		}
		p.mu.Unlock()
		if wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			p.mu.Lock()
			expired := p.expireLocked(p.now())
			logLevel := p.config.LogLevel
			revision := p.revision
			p.mu.Unlock()
			if expired > 0 {
				p.logPolicy(revision, "", logLevel, map[string]any{
					"event":          "request_diagnostics_expired",
					"schema_version": recordSchema,
					"plugin_version": Version,
					"expired_count":  expired,
				})
			}
		case <-p.wake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-p.stop:
			if !timer.Stop() {
				<-timer.C
			}
			return
		}
	}
}

func (p *Plugin) signalExpiryLoop() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *Plugin) log(callbackID, level string, fields map[string]any) {
	if p.logger != nil {
		p.logger.Log(callbackID, level, "CPA request diagnostics", fields)
	}
}

func (p *Plugin) logPolicy(revision uint64, callbackID, level string, fields map[string]any) {
	p.logMu.Lock()
	defer p.logMu.Unlock()
	p.mu.Lock()
	current := p.revision == revision
	p.mu.Unlock()
	if current {
		p.log(callbackID, level, fields)
	}
}

func extractHeaders(raw json.RawMessage, allowed map[string]string, maxBytes int) map[string]string {
	if len(raw) == 0 || len(allowed) == 0 {
		return nil
	}
	var headers map[string]json.RawMessage
	if json.Unmarshal(raw, &headers) != nil {
		return nil
	}
	result := make(map[string]string)
	for rawName, rawValues := range headers {
		name, ok := allowed[strings.ToLower(http.CanonicalHeaderKey(rawName))]
		if !ok || !isIdentifierHeader(name) {
			continue
		}
		var values []string
		if json.Unmarshal(rawValues, &values) != nil || len(values) == 0 {
			continue
		}
		if value := sanitize(values[0], maxBytes); value != "" {
			result[name] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func filterCollectedHeaders(input map[string]string, allowed map[string]string, maxBytes int) map[string]string {
	if len(input) == 0 || len(allowed) == 0 {
		return nil
	}
	result := make(map[string]string)
	for rawName, rawValue := range input {
		name, ok := allowed[strings.ToLower(http.CanonicalHeaderKey(rawName))]
		if !ok || !isIdentifierHeader(name) {
			continue
		}
		if value := sanitize(rawValue, maxBytes); value != "" {
			result[name] = value
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func optionalSanitize(value string, include bool, maxBytes int) string {
	if !include {
		return ""
	}
	return sanitize(value, maxBytes)
}

func sanitize(value string, maxBytes int) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxBytes {
		return ""
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x21 || value[i] > 0x7e {
			return ""
		}
	}
	return value
}

func sanitizeOutcome(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "succeeded", "failed", "rejected", "canceled":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "unknown"
	}
}

func sampled(requestID string, rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1 {
		return true
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(requestID))
	return float64(h.Sum64())/float64(math.MaxUint64) < rate
}

func elapsedMS(start, end time.Time) int64 {
	if end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

func cloneStrings(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func marshalEnvelope(value envelope) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		return []byte(`{"ok":false,"error":{"code":"marshal_error","message":"failed to marshal response"}}`)
	}
	return raw
}
