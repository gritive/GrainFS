#!/usr/bin/env bash
# scripts/p0b-auto-measure.sh
#
# Drive P0b shared-badger perf matrix sweep when host conditions allow.
# Designed to be called periodically (e.g., from a 30-min cron). Idempotent —
# resumes from where it left off via a done-list file.
#
# What it does each invocation:
#   1. Check host load average + competing grainfs processes from other worktrees.
#      If the host is too loaded to give a clean measurement, log 'skipped' and exit 0.
#   2. Pick the next scenario in P0B_SCENARIOS that has NOT been recorded as done.
#      If all scenarios are done, exit 0 with a message.
#   3. Run that single scenario with GRAINFS_PERF_SHARED_BADGER=1.
#      Output goes to $RESULTS_DIR/<scenario>/.
#   4. On PASS, append <scenario> to $RESULTS_DIR/done.txt.
#      On FAIL, leave done.txt alone — the next run will retry.
#
# Set P0B_SCENARIOS env to override the default list. Set RESULTS_DIR to override
# the output dir. Set LOAD_THRESHOLD to override the 4.0 default.

set -e

readonly REPO_ROOT=${REPO_ROOT:-$(git rev-parse --show-toplevel)}
readonly RESULTS_DIR=${RESULTS_DIR:-/tmp/grainfs-perf-p0b-auto}
readonly LOAD_THRESHOLD=${LOAD_THRESHOLD:-4.0}
readonly LOG_FILE=${LOG_FILE:-${RESULTS_DIR}/auto.log}

# Ordered scenarios — run idle ones first (cheaper / faster), load last.
readonly DEFAULT_SCENARIOS="idle-N8 idle-N16 load-N8 load-N16 load-N32"
readonly SCENARIOS=${P0B_SCENARIOS:-$DEFAULT_SCENARIOS}

mkdir -p "$RESULTS_DIR"

log() {
    local ts
    ts=$(date '+%Y-%m-%d %H:%M:%S')
    printf '[%s] %s\n' "$ts" "$*" | tee -a "$LOG_FILE" >&2
}

# Returns the 1-min load average as a decimal string.
load_avg() {
    uptime | awk -F'load averages?: ' '{print $2}' | awk '{print $1}' | tr -d ','
}

# Counts grainfs procs from worktrees other than $REPO_ROOT.
foreign_grainfs_count() {
    pgrep -fl 'grainfs serve' 2>/dev/null \
        | grep -v "^$$ " \
        | grep -v "$REPO_ROOT/" \
        | wc -l \
        | tr -d ' '
}

# Returns 0 (success) when host is calm enough to measure cleanly.
host_is_idle_enough() {
    local load=$(load_avg)
    local foreign=$(foreign_grainfs_count)
    log "host check: load=$load (threshold $LOAD_THRESHOLD), foreign-grainfs=$foreign"

    # awk floating-point compare
    local under_threshold
    under_threshold=$(awk -v l="$load" -v t="$LOAD_THRESHOLD" 'BEGIN { print (l < t) ? 1 : 0 }')
    if [[ "$under_threshold" != "1" ]]; then
        log "skipped: load $load >= threshold $LOAD_THRESHOLD"
        return 1
    fi
    if [[ "$foreign" -gt 0 ]]; then
        log "skipped: $foreign foreign grainfs proc(s) competing"
        return 1
    fi
    return 0
}

next_undone_scenario() {
    local done_file="$RESULTS_DIR/done.txt"
    touch "$done_file"
    for sc in $SCENARIOS; do
        if ! grep -qx "$sc" "$done_file"; then
            echo "$sc"
            return 0
        fi
    done
    return 1
}

run_scenario() {
    local sc=$1
    log "starting scenario: $sc"
    rm -rf "$RESULTS_DIR/$sc"
    mkdir -p "$RESULTS_DIR/$sc"

    # Build first to make sure binary is fresh.
    (cd "$REPO_ROOT" && go build -o bin/grainfs ./cmd/grainfs/) || {
        log "build failed; aborting"
        return 1
    }

    local timeout_seconds=600
    case "$sc" in
        load-N32|load-N64) timeout_seconds=1200 ;;
    esac

    GRAINFS_PERF=1 \
    GRAINFS_PERF_SHARED_BADGER=1 \
    GRAINFS_PERF_SCENARIO="$sc" \
    GRAINFS_PERF_DIR="$RESULTS_DIR" \
        timeout "$((timeout_seconds + 60))" \
        go test -run '^TestE2E_ClusterPerf_All$' -count=1 \
            -timeout "${timeout_seconds}s" -v \
            "$REPO_ROOT/tests/e2e/" \
        > "$RESULTS_DIR/$sc/run.log" 2>&1
    local exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        log "scenario $sc PASSED"
        echo "$sc" >> "$RESULTS_DIR/done.txt"
        # Echo summary for cron output.
        if [[ -f "$RESULTS_DIR/$sc/summary.md" ]]; then
            log "--- $sc summary ---"
            sed -n '1,12p' "$RESULTS_DIR/$sc/summary.md" | tee -a "$LOG_FILE" >&2
        fi
        return 0
    else
        log "scenario $sc FAILED (exit=$exit_code); will retry next run"
        return 1
    fi
}

main() {
    log "==== p0b-auto-measure tick ===="
    cd "$REPO_ROOT"

    if ! host_is_idle_enough; then
        log "host not idle; nothing to do this tick"
        exit 0
    fi

    local sc
    if ! sc=$(next_undone_scenario); then
        log "all P0B scenarios done; results in $RESULTS_DIR"
        exit 0
    fi

    run_scenario "$sc" || exit 0
}

main "$@"
