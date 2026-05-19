#!/usr/bin/env bash
# Wrapper around bench_iceberg_table_cluster.sh that injects per-subcommand
# env (PROFILE_ROOT, DURATION) so the 4 warp iceberg subcommands write
# isolated artifacts and use intended durations.
#
# Usage:
#   ./benchmarks/run_iceberg_warp.sh <subcommand>
# Where <subcommand> is one of: catalog-read, catalog-commits,
# catalog-mixed, sustained.

set -euo pipefail

SUB="${1:?usage: $0 <catalog-read|catalog-commits|catalog-mixed|sustained>}"
case "$SUB" in
  catalog-read|catalog-commits|catalog-mixed) DURATION_DEFAULT="30s" ;;
  sustained) DURATION_DEFAULT="2m" ;;
  *) echo "unknown subcommand: $SUB" >&2; exit 2 ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

NODE_COUNT="${NODE_COUNT:-4}"
TS="$(date +%Y%m%d-%H%M%S)"
PROFILE_ROOT_DEFAULT="$REPO_ROOT/benchmarks/profiles/iceberg-warp-${SUB}-${NODE_COUNT}n-${TS}"

export ICEBERG_WARP_COMMAND="$SUB"
export DURATION="${DURATION:-$DURATION_DEFAULT}"
export PROFILE_ROOT="${PROFILE_ROOT:-$PROFILE_ROOT_DEFAULT}"
export NODE_COUNT

mkdir -p "$PROFILE_ROOT"
echo "[run] subcommand=$SUB duration=$DURATION profile_root=$PROFILE_ROOT nodes=$NODE_COUNT"
exec "$SCRIPT_DIR/bench_iceberg_table_cluster.sh"
