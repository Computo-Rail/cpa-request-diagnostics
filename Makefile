PLUGIN := cpa-request-diagnostics
VERSION ?= dev
GOARCH := $(shell go env GOARCH)
GOOS := $(shell go env GOOS)

ifeq ($(GOOS),darwin)
EXT := dylib
else ifeq ($(GOOS),windows)
EXT := dll
else
EXT := so
endif

.PHONY: build release test race vet check host-smoke clean

build:
	mkdir -p bin/$(GOOS)/$(GOARCH)
	go build -trimpath -buildmode=c-shared \
		-ldflags "-s -w -X github.com/Computo-Rail/cpa-request-diagnostics/internal/diagnostics.Version=$(VERSION)" \
		-o bin/$(GOOS)/$(GOARCH)/$(PLUGIN).$(EXT) ./cmd/$(PLUGIN)
	rm -f bin/$(GOOS)/$(GOARCH)/$(PLUGIN).h

release:
	@printf '%s\n' "$(VERSION)" | grep -Eq '^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$$' || (printf '%s\n' 'VERSION must be SemVer without a leading v, for example: make release VERSION=0.1.0' >&2; exit 1)
	mkdir -p bin/$(GOOS)/$(GOARCH)
	go build -trimpath -buildmode=c-shared \
		-ldflags "-s -w -X github.com/Computo-Rail/cpa-request-diagnostics/internal/diagnostics.Version=$(VERSION)" \
		-o bin/$(GOOS)/$(GOARCH)/$(PLUGIN)-v$(VERSION).$(EXT) ./cmd/$(PLUGIN)
	rm -f bin/$(GOOS)/$(GOARCH)/$(PLUGIN)-v$(VERSION).h

test:
	go test ./...

race:
	go test -race ./...

vet:
	go vet ./...

check: test race vet build

host-smoke:
	bash scripts/verify-host-v7.2.159.sh

clean:
	rm -rf bin
