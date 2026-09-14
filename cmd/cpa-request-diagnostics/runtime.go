package main

import (
	"sync"

	"github.com/Computo-Rail/cpa-request-diagnostics/internal/diagnostics"
)

type pluginRuntime struct {
	mu     sync.RWMutex
	logger diagnostics.HostLogger
	plugin *diagnostics.Plugin
}

func (r *pluginRuntime) initialize() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.plugin != nil {
		r.plugin.Shutdown()
	}
	r.plugin = diagnostics.New(r.logger)
}

func (r *pluginRuntime) call(method string, raw []byte) []byte {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.plugin == nil {
		return passThroughEnvelope
	}
	return r.plugin.Call(method, raw)
}

func (r *pluginRuntime) shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.plugin == nil {
		return
	}
	r.plugin.Shutdown()
	r.plugin = nil
}
