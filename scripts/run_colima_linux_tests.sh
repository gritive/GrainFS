#!/usr/bin/env bash
# Run Linux-only GrainFS tests directly inside the Colima VM.
#
# The host cross-compiles Linux binaries for Colima's architecture, copies only
# those binaries into the VM, then executes them there. This keeps kernel-facing
# paths such as O_DIRECT on Linux without building a VM image.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MODE="${1:-all}"
REMOTE_WORKDIR="${COLIMA_LINUX_TEST_WORKDIR:-/tmp/grainfs-colima-linux-tests}"
DIRECTIO_TEST_DIR="${GRAINFS_DIRECTIO_TEST_DIR:-/tmp/grainfs-directio-test}"
E2E_TEST_PATTERN="${E2E_TEST_PATTERN:-^Test}"
E2E_TEST_TIMEOUT="${E2E_TEST_TIMEOUT:-900s}"
E2E_TEST_PARALLEL="${E2E_TEST_PARALLEL:-1}"
VERSION="${VERSION:-$(git -C "$REPO_ROOT" describe --tags --always --dirty 2>/dev/null || echo dev)}"

require_command() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "[error] required command not found: $cmd" >&2
    exit 1
  fi
}

colima_ssh() {
  colima ssh -- "$@"
}

colima_sh() {
  colima ssh -- bash -lc "$1"
}

colima_arch() {
  local machine
  machine="$(colima_sh 'uname -m')"
  case "$machine" in
    aarch64|arm64) echo "arm64" ;;
    x86_64|amd64) echo "amd64" ;;
    *)
      echo "[error] unsupported Colima arch: $machine" >&2
      return 1
      ;;
  esac
}

copy_to_colima() {
  local src="$1"
  local dst="$2"
  colima_ssh mkdir -p "$(dirname "$dst")"
  cat "$src" | colima_sh "cat > '$dst' && chmod +x '$dst'"
}

run_directio() {
  local arch="$1"
  local local_test="$REPO_ROOT/bin/colima-linux-$arch/directio.test"
  local remote_test="$REMOTE_WORKDIR/bin/directio.test"

  echo "[colima] building Linux directio integration test ($arch)..."
  (cd "$REPO_ROOT" && GOOS=linux GOARCH="$arch" CGO_ENABLED=0 \
    go test -c -tags directio_integration -o "$local_test" ./internal/storage/directio)

  echo "[colima] copying directio test binary..."
  copy_to_colima "$local_test" "$remote_test"

  echo "[colima] running directio integration test..."
  colima_sh "rm -rf '$DIRECTIO_TEST_DIR' && mkdir -p '$DIRECTIO_TEST_DIR'"
  colima_ssh env GRAINFS_DIRECTIO_TEST_DIR="$DIRECTIO_TEST_DIR" \
    "$remote_test" -test.v -test.run '^TestLinuxDirectIOIntegration_' -test.count=1
}

run_e2e() {
  local arch="$1"
  local local_dir="$REPO_ROOT/bin/colima-linux-$arch"
  local local_grainfs="$local_dir/grainfs"
  local local_test="$local_dir/e2e.test"
  local remote_grainfs="$REMOTE_WORKDIR/bin/grainfs"
  local remote_test="$REMOTE_WORKDIR/bin/e2e.test"
  local short_arg=()

  echo "[colima] building Linux grainfs binary ($arch)..."
  (cd "$REPO_ROOT" && GOOS=linux GOARCH="$arch" CGO_ENABLED=0 \
    go build -ldflags "-s -w -X main.version=$VERSION" -o "$local_grainfs" ./cmd/grainfs/)

  echo "[colima] building Linux e2e test binary ($arch)..."
  (cd "$REPO_ROOT" && GOOS=linux GOARCH="$arch" CGO_ENABLED=0 \
    go test -c -o "$local_test" ./tests/e2e)

  echo "[colima] copying e2e binaries..."
  copy_to_colima "$local_grainfs" "$remote_grainfs"
  copy_to_colima "$local_test" "$remote_test"

  if [[ "${E2E_TEST_SHORT:-0}" == "1" ]]; then
    short_arg=(-test.short)
  fi

  echo "[colima] running e2e test binary..."
  colima_ssh env GRAINFS_BINARY="$remote_grainfs" \
    "$remote_test" \
    -test.v \
    -test.count=1 \
    -test.timeout "$E2E_TEST_TIMEOUT" \
    -test.parallel "$E2E_TEST_PARALLEL" \
    -test.run "$E2E_TEST_PATTERN" \
    "${short_arg[@]}"
}

case "$MODE" in
  all|directio|e2e) ;;
  *)
    echo "usage: $0 [all|directio|e2e]" >&2
    exit 2
    ;;
esac

require_command colima
if ! colima status >/dev/null 2>&1; then
  echo "[error] colima is not running; start with: colima start" >&2
  exit 1
fi

arch="$(colima_arch)"
mkdir -p "$REPO_ROOT/bin/colima-linux-$arch"
colima_ssh mkdir -p "$REMOTE_WORKDIR/bin"

case "$MODE" in
  all)
    run_directio "$arch"
    run_e2e "$arch"
    ;;
  directio)
    run_directio "$arch"
    ;;
  e2e)
    run_e2e "$arch"
    ;;
esac
