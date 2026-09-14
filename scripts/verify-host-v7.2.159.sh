#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_checkout="${CLIPROXY_SOURCE:-}"
host_commit="ac02da6c05e18f465aa7e3ed5b0a65a2f060917d"
temporary_root="$(mktemp -d)"
host_source="${temporary_root}/CLIProxyAPI"

cleanup() {
	rm -rf "${temporary_root}"
}
trap cleanup EXIT

if [[ -n "${source_checkout}" ]]; then
	git -C "${source_checkout}" cat-file -e "${host_commit}^{commit}"
	git clone --quiet --local --no-hardlinks "${source_checkout}" "${host_source}"
else
	git clone --quiet https://github.com/router-for-me/CLIProxyAPI.git "${host_source}"
fi
git -C "${host_source}" checkout --quiet --detach "${host_commit}"
if [[ "$(git -C "${host_source}" rev-parse HEAD)" != "${host_commit}" ]]; then
	echo "CLIProxyAPI checkout does not match pinned v7.2.159 commit" >&2
	exit 1
fi

make -C "${root}" build
cp "${root}/testdata/host_smoke_test.go" "${host_source}/internal/pluginhost/external_diagnostics_smoke_test.go"
(
  cd "${host_source}"
  CPA_DIAGNOSTICS_PLUGIN_DIR="${root}/bin/$(go env GOOS)/$(go env GOARCH)" \
    go test ./internal/pluginhost -run '^TestExternalDiagnosticsPluginSmoke$' -count=1
)
