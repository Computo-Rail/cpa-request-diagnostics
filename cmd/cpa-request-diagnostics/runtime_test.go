package main

import (
	"encoding/json"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginabi"
)

func TestPluginRuntimeCanReinitializeAfterShutdown(t *testing.T) {
	runtime := &pluginRuntime{}
	register := func() {
		raw, err := json.Marshal(map[string]any{"schema_version": pluginabi.SchemaVersion})
		if err != nil {
			t.Fatal(err)
		}
		var response struct {
			OK bool `json:"ok"`
		}
		if err := json.Unmarshal(runtime.call(pluginabi.MethodPluginRegister, raw), &response); err != nil {
			t.Fatal(err)
		}
		if !response.OK {
			t.Fatal("plugin registration failed")
		}
	}

	runtime.initialize()
	register()
	runtime.shutdown()
	runtime.initialize()
	register()
	runtime.shutdown()
	runtime.shutdown()
}
