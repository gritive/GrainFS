#!/usr/bin/env bash
# 3-node S3 benchmark with pprof profiling.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ $# -gt 0 && "$1" != -* ]]; then
  export BINARY="$1"
  shift
fi

export PROFILE="${PROFILE:-1}"
exec "$SCRIPT_DIR/bench_cluster.sh" "$@"
