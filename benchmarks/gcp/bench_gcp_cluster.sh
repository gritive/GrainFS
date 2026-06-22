#!/usr/bin/env bash
# Multi-node GCP cluster benchmark harness (git-tracked; prior copies lived in
# ephemeral worktrees and were lost — keep this one in the repo).
#
# Phase 5 use: cross-binary A/B (devel "신규 전체" vs master "옛 전체"). It
# provisions a 4-node GrainFS cluster + 1 warp/builder client in one GCP zone,
# builds both refs ON a linux VM (duckdb cgo can't cross-compile from darwin),
# boots the cluster per binary, runs warp PUT/GET/HEAD from the in-network
# client through the reused bench_s3_compat_compare.sh external-cluster path,
# pulls warp-results.tsv into the layout cross_binary_ab.sh's verdict python
# reads, and applies the merge-blocker rule.
#
# Subcommands (run incrementally; `full` chains the A/B):
#   up                 provision storage VMs + client (idempotent)
#   build              git-archive devel+master -> client -> go build both -> fan out
#   arm <new|old> <i>  reset+boot cluster with that binary, warp, pull run<i>/<arm>
#   verdict            aggregate pulled tsvs -> verdict.md (uses cross_binary_ab.sh python)
#   down               delete all VMs
#   full               up; build; for i in 1..RUNS { arm new i; arm old i }; verdict
#                      (down is NOT automatic — confirm results first, then `down`)
#
# Cost guard: always `down` when finished. `full` does not auto-teardown.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

PROJECT="${PROJECT:-grainfs}"
ZONE="${ZONE:-asia-northeast3-a}"
MACHINE="${MACHINE:-n2-standard-4}"
NODE_COUNT="${NODE_COUNT:-4}"
IMAGE_FAMILY="${IMAGE_FAMILY:-debian-12}"
IMAGE_PROJECT="${IMAGE_PROJECT:-debian-cloud}"
STORAGE_DISK_GB="${STORAGE_DISK_GB:-80}"
PREFIX="${PREFIX:-gr-p5}"
CLIENT="$PREFIX-cli"
GO_VERSION="${GO_VERSION:-1.26.4}"
WARP_VERSION="${WARP_VERSION:-1.1.4}"
# Ops Agent: stock Compute Engine metrics omit guest MEMORY and DISK-SPACE
# utilization; the Ops Agent adds agent.googleapis.com/memory/percent_used and
# .../disk/percent_used (visible in Cloud Monitoring / Metrics Explorer). Auto-
# installed via startup-script at boot (needs VM internet — VMs get an external
# IP). OPS_AGENT=0 to skip (e.g. to remove its small CPU/RSS overhead from a
# publishable bench run).
OPS_AGENT="${OPS_AGENT:-1}"

NEW_REF="${NEW_REF:-devel}"
OLD_REF="${OLD_REF:-master}"
RUNS="${RUNS:-1}"

# warp workload (passed through to bench_s3_compat_compare.sh on the client)
WARP_OPS="${WARP_OPS:-put,get,stat}"
WARP_OBJ_SIZE="${WARP_OBJ_SIZE:-10MiB}"
WARP_CONCURRENT="${WARP_CONCURRENT:-32}"
WARP_DURATION="${WARP_DURATION:-1m}"

# decision-rule thresholds (forwarded to the verdict python)
PUT_WIN_MIN="${PUT_WIN_MIN:-1.05}"
NOREG_MIN="${NOREG_MIN:-0.95}"

RESULT_DIR="${RESULT_DIR:-$REPO_ROOT/benchmarks/profiles/gcp-phase5-$(date +%Y%m%d-%H%M%S)}"
REMOTE_USER="${REMOTE_USER:-$(whoami)}"
DATA_DIR="/var/lib/gfs"
BIN_DIR="/opt/grainfs"
HTTP_PORT=9000
RAFT_PORT=7000
JOIN_PORT=7100
PSK="phase5-cross-binary-cluster-key"

ssh_node() { # node-name "cmd..."
  local n="$1"; shift
  gcloud compute ssh "$n" --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    --command="$*" 2>/dev/null
}
scp_to() { # localpath node:remotepath
  gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    "$1" "$2" >/dev/null 2>&1
}
node_name() { echo "$PREFIX-$1"; }   # storage node i (0-based)
internal_ip() {
  gcloud compute instances describe "$1" --zone="$ZONE" --project="$PROJECT" \
    --format='value(networkInterfaces[0].networkIP)' 2>/dev/null
}

log() { echo "### $(date +%H:%M:%S) $*" >&2; }

# ---------------------------------------------------------------- up -----------
cmd_up() {
  local names=("$CLIENT")
  for i in $(seq 0 $((NODE_COUNT - 1))); do names+=("$(node_name "$i")"); done
  log "provisioning ${names[*]} ($MACHINE, $ZONE)"
  # Ops Agent install at boot for guest memory + disk-space metrics.
  local meta=()
  if [[ "$OPS_AGENT" == "1" ]]; then
    local sf; sf="$(mktemp "${TMPDIR:-/tmp}/p5-opsagent.XXXXXX")" # no suffix: BSD/macOS mktemp needs trailing X's
    cat >"$sf" <<'STARTUP'
#! /bin/bash
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent.sh
bash add-google-cloud-ops-agent.sh --also-install
STARTUP
    meta=(--metadata-from-file "startup-script=$sf")
    log "ops-agent: auto-install via startup-script (guest memory + disk-space metrics)"
  fi
  # client: pd-balanced (no SSD quota); storage: pd-ssd
  gcloud compute instances create "$CLIENT" \
    --zone="$ZONE" --project="$PROJECT" --machine-type="$MACHINE" \
    --image-family="$IMAGE_FAMILY" --image-project="$IMAGE_PROJECT" \
    --boot-disk-size=40GB --boot-disk-type=pd-balanced \
    "${meta[@]}" \
    --min-cpu-platform="Intel Cascade Lake" --provisioning-model=SPOT --quiet >&2 || return 1
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    gcloud compute instances create "$(node_name "$i")" \
      --zone="$ZONE" --project="$PROJECT" --machine-type="$MACHINE" \
      --image-family="$IMAGE_FAMILY" --image-project="$IMAGE_PROJECT" \
      --boot-disk-size="${STORAGE_DISK_GB}GB" --boot-disk-type=pd-ssd \
      "${meta[@]}" \
      --min-cpu-platform="Intel Cascade Lake" --provisioning-model=SPOT --quiet >&2 || return 1
  done
  log "waiting for SSH on all nodes"
  for n in "${names[@]}"; do
    for _ in $(seq 1 40); do
      ssh_node "$n" "true" && break
      sleep 5
    done
  done
  log "installing deps (storage: nothing; client: go+gcc+warp+aws)"
  cmd_prep_client
  log "up done"
}

cmd_prep_client() {
  ssh_node "$CLIENT" "
    set -e
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential git curl python3 unzip >/dev/null
    if [ ! -x /usr/local/go/bin/go ]; then
      curl -sSL https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz -o /tmp/go.tgz
      sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tgz
    fi
    if [ ! -x /usr/local/bin/warp ]; then
      curl -sSL https://github.com/minio/warp/releases/download/v${WARP_VERSION}/warp_Linux_x86_64.tar.gz -o /tmp/warp.tgz
      tar -C /tmp -xzf /tmp/warp.tgz warp && sudo mv /tmp/warp /usr/local/bin/warp
    fi
    if ! command -v aws >/dev/null 2>&1; then
      curl -sSL 'https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip' -o /tmp/awscli.zip
      cd /tmp && unzip -q -o awscli.zip && sudo ./aws/install --update >/dev/null
    fi
    echo client-prep-done
  "
}

# ------------------------------------------------------------- build -----------
cmd_build() {
  log "git-archive $NEW_REF + $OLD_REF -> client -> build"
  ( cd "$REPO_ROOT" && git archive --format=tar.gz "$NEW_REF" -o /tmp/p5-src-new.tgz )
  ( cd "$REPO_ROOT" && git archive --format=tar.gz "$OLD_REF" -o /tmp/p5-src-old.tgz )
  scp_to /tmp/p5-src-new.tgz "$CLIENT:/tmp/p5-src-new.tgz"
  scp_to /tmp/p5-src-old.tgz "$CLIENT:/tmp/p5-src-old.tgz"
  ssh_node "$CLIENT" "
    set -e
    export PATH=/usr/local/go/bin:\$PATH
    for arm in new old; do
      rm -rf /tmp/p5-\$arm && mkdir -p /tmp/p5-\$arm
      tar -C /tmp/p5-\$arm -xzf /tmp/p5-src-\$arm.tgz
      cd /tmp/p5-\$arm
      echo \"[build \$arm] go build...\"
      CGO_ENABLED=1 go build -ldflags '-s -w' -o /tmp/grainfs-\$arm ./cmd/grainfs/
    done
    /tmp/grainfs-new --version; /tmp/grainfs-old --version
    echo build-done
  " || return 1
  # fan out both binaries to every storage node
  log "fanning out binaries to storage nodes"
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    local n; n="$(node_name "$i")"
    ssh_node "$n" "sudo mkdir -p $BIN_DIR && sudo chown $REMOTE_USER $BIN_DIR"
  done
  # pull binaries to local, push to each storage node (scp is VM<->local only).
  # The IAP-tunnelled scp intermittently corrupts the ~122MB binary (silent
  # "fatal error: invalid runtime symbol table" at boot), so every hop is md5-
  # verified and retried — a corrupt binary otherwise wastes a full boot cycle.
  local arm md5 nm
  for arm in new old; do
    md5="$(ssh_node "$CLIENT" "md5sum /tmp/grainfs-$arm | cut -d' ' -f1")"
    push_verified "$CLIENT:/tmp/grainfs-$arm" "/tmp/grainfs-$arm" "$md5" || { log "ERROR: client->local grainfs-$arm corrupt"; return 1; }
    for i in $(seq 0 $((NODE_COUNT - 1))); do
      nm="$(node_name "$i")"
      push_verified "/tmp/grainfs-$arm" "$nm:$BIN_DIR/grainfs-$arm" "$md5" "$nm" || { log "ERROR: ->$nm grainfs-$arm corrupt"; return 1; }
    done
  done
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    ssh_node "$(node_name "$i")" "chmod +x $BIN_DIR/grainfs-new $BIN_DIR/grainfs-old"
  done
  log "build done"
}

# push_verified <src> <dst> <expected-md5> [remote-node-for-dst]
# scp src->dst, verify dst md5 == expected, retry up to 4x. dst is "node:path"
# (verify over ssh on that node) or a local path (verify locally).
push_verified() {
  local src="$1" dst="$2" want="$3" rnode="${4:-}"
  local got
  for _ in 1 2 3 4; do
    if [[ "$dst" == *:* ]]; then scp_to "$src" "$dst"; else
      gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap "$src" "$dst" >/dev/null 2>&1
    fi
    if [[ -n "$rnode" ]]; then
      got="$(ssh_node "$rnode" "md5sum '${dst#*:}' 2>/dev/null | cut -d' ' -f1")"
    else
      got="$(md5 -q "$dst" 2>/dev/null || md5sum "$dst" 2>/dev/null | cut -d' ' -f1)"
    fi
    [[ "$got" == "$want" ]] && return 0
  done
  return 1
}

# --------------------------------------------------------- boot cluster --------
# Processes run under a transient systemd unit (grainfs-node) — `nohup ... &` over
# an IAP ssh channel drops the channel (exit 255) and the backgrounded serve never
# survives; systemd-run returns immediately and the service outlives the ssh.
# serve runs as root, so $DATA_DIR + admin.sock are root-owned -> admin CLI uses sudo.
UNIT="grainfs-node"
reset_cluster() {
  log "reset: stop unit + wipe data on all storage nodes"
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    ssh_node "$(node_name "$i")" "
      sudo systemctl stop $UNIT 2>/dev/null || true
      sudo systemctl reset-failed $UNIT 2>/dev/null || true
      sudo pkill -9 -f 'grainfs' 2>/dev/null || true
      # wait for the S3 port to actually free (a killed serve can hold it briefly);
      # a stale listener makes the next genesis exit 1 (bind failure).
      for _ in \$(seq 1 30); do
        (exec 3<>/dev/tcp/127.0.0.1/$HTTP_PORT) 2>/dev/null && { exec 3>&- 3<&-; sleep 0.5; } || break
      done
      sudo rm -rf $DATA_DIR && sudo mkdir -p $DATA_DIR/keys.d
    " &
  done
  wait
}

# serve_node <node_idx> <bin> [invite_bundle]
serve_node() {
  local idx="$1" bin="$2" bundle="${3:-}"
  local n; n="$(node_name "$idx")"
  local ipi; ipi="$(internal_ip "$n")"
  local setenv=()
  if [ -z "$bundle" ]; then
    # genesis: pre-stage cluster PSK (root-owned dir)
    ssh_node "$n" "printf '%s\n' '$PSK' | sudo tee $DATA_DIR/keys.d/current.key >/dev/null"
  else
    setenv=(--setenv="GRAINFS_INVITE_BUNDLE=$bundle")
  fi
  ssh_node "$n" "sudo systemd-run --unit=$UNIT --collect ${setenv[*]} \
    $bin serve --data $DATA_DIR --port $HTTP_PORT --node-id p5-node-$idx \
    --raft-addr $ipi:$RAFT_PORT --join-listen-addr $ipi:$JOIN_PORT \
    --raft-heartbeat-interval ${RAFT_HEARTBEAT:-1s} --raft-election-timeout ${RAFT_ELECTION:-3s} \
    --nbd-port 0 --scrub-interval 0 --lifecycle-interval 0 --log-level ${LOG_LEVEL:-warn} \
    && echo node-$idx-launched"
}

# wait_http <node_idx> — poll the cluster status endpoint (root path is not 2xx).
# Prints "http-up" and returns 0 on success; returns 1 (loud) if the node never
# binds — a silent timeout here used to print genesis-up anyway and cascade into
# bogus empty-SA/empty-bundle failures that masqueraded as forward bugs.
wait_http() {
  local n; n="$(node_name "$1")"
  local out
  out="$(ssh_node "$n" "for _ in \$(seq 1 90); do curl -sf http://127.0.0.1:$HTTP_PORT/api/cluster/status >/dev/null 2>&1 && { echo http-up; exit 0; }; sleep 1; done; echo http-down; sudo journalctl -u $UNIT --no-pager 2>/dev/null | tail -8")"
  echo "$out"
  [[ "$out" == *http-up* ]]
}

# boot_cluster <arm: new|old>  -> exports BOOT_ACCESS_KEY/BOOT_SECRET_KEY/BOOT_HOSTS
boot_cluster() {
  local arm="$1"
  local bin="$BIN_DIR/grainfs-$arm"
  local ip0; ip0="$(internal_ip "$(node_name 0)")"
  log "boot arm=$arm genesis=$(node_name 0)@$ip0"
  serve_node 0 "$bin"
  if ! wait_http 0; then log "ERROR: arm=$arm genesis never came up — aborting"; return 1; fi
  ssh_node "$(node_name 0)" "for _ in \$(seq 1 60); do sudo test -f $DATA_DIR/cluster.id && break; sleep 0.5; done; echo genesis-up"
  # bootstrap IAM SA on node-0 (admin.sock is root-owned)
  local saj
  saj="$(ssh_node "$(node_name 0)" "
    sudo curl -sf --unix-socket $DATA_DIR/admin.sock -X PUT -H 'Content-Type: application/json' \
      -d '{\"value\":\"10.0.0.0/8\"}' http://unix/v1/config/trusted-proxy.cidr >/dev/null
    sudo $bin iam --json sa create phase5 --endpoint $DATA_DIR/admin.sock
  ")"
  if [ -z "$saj" ] || ! echo "$saj" | python3 -c 'import json,sys;json.load(sys.stdin)["access_key"]' 2>/dev/null; then
    log "ERROR: arm=$arm IAM SA create returned no JSON: ${saj:0:120}"; return 1
  fi
  BOOT_ACCESS_KEY="$(echo "$saj" | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_key"])')"
  BOOT_SECRET_KEY="$(echo "$saj" | python3 -c 'import json,sys;print(json.load(sys.stdin)["secret_key"])')"
  local sa_id; sa_id="$(echo "$saj" | python3 -c 'import json,sys;print(json.load(sys.stdin)["sa_id"])')"
  log "arm=$arm SA created access=${BOOT_ACCESS_KEY:0:8}..."
  # joiners 1..N-1 (serial: each mint needs the live leader)
  for i in $(seq 1 $((NODE_COUNT - 1))); do
    local bundle
    bundle="$(ssh_node "$(node_name 0)" "sudo $bin cluster invite create --endpoint $DATA_DIR/admin.sock" \
      | awk 'f{print;exit} /GRAINFS_INVITE_BUNDLE:/{f=1}')"
    if [ -z "$bundle" ]; then log "ERROR: empty invite bundle for node $i"; return 1; fi
    serve_node "$i" "$bin" "$bundle"
    wait_http "$i" >/dev/null
  done
  # wait leader + shard groups (poll on node-0 over ssh; mac can't reach internal IPs)
  local expected=$((NODE_COUNT * 4)); [ "$expected" -lt 8 ] && expected=8
  log "arm=$arm waiting leader + $expected shard groups"
  ssh_node "$(node_name 0)" "
    for _ in \$(seq 1 120); do
      s=\$(curl -sf http://127.0.0.1:$HTTP_PORT/api/cluster/status 2>/dev/null||true)
      st=\$(echo \"\$s\" | python3 -c 'import sys,json;print(json.load(sys.stdin).get(\"state\",\"\"))' 2>/dev/null||true)
      [ \"\$st\" = Leader ] && break; sleep 0.5
    done
    for _ in \$(seq 1 240); do
      s=\$(curl -sf http://127.0.0.1:$HTTP_PORT/api/cluster/status 2>/dev/null||true)
      c=\$(echo \"\$s\" | python3 -c 'import sys,json;print(len((json.load(sys.stdin) or {}).get(\"shard_groups\") or []))' 2>/dev/null||true)
      [ -n \"\$c\" ] && [ \"\$c\" -ge $expected ] 2>/dev/null && { echo \"shard-groups \$c/$expected\"; break; }
      sleep 0.5
    done
  "
  # pre-create warp buckets with bucket-admin policy for the SA
  for op in ${WARP_OPS//,/ }; do
    ssh_node "$(node_name 0)" "sudo $bin bucket create warp-grainfs-cluster-$op --endpoint $DATA_DIR/admin.sock --attach-sa $sa_id --attach-policy bucket-admin >/dev/null 2>&1 || true"
  done
  # build comma-separated host:port list of all node internal IPs
  local hosts=""
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    local ipi; ipi="$(internal_ip "$(node_name "$i")")"
    hosts="${hosts:+$hosts,}$ipi:$HTTP_PORT"
  done
  BOOT_HOSTS="$hosts"
  log "arm=$arm cluster ready hosts=$BOOT_HOSTS"
}

# ----------------------------------------------------------- arm (warp) --------
# run_warp_on_client emits warp-results.tsv to a remote dir, pulled into run<i>/<arm>/.
cmd_arm() { # <new|old> <runidx>
  local arm="$1" idx="$2"
  reset_cluster
  boot_cluster "$arm" || return 1
  local rprof="/tmp/p5-prof-$arm-$idx"
  log "arm=$arm run=$idx warp via bench_s3_compat_compare.sh external mode"
  # push the repo's benchmark scripts to the client (external cluster mode reuses
  # all warp+parse+tsv logic). The client already has the source from build.
  ssh_node "$CLIENT" "
    export PATH=/usr/local/go/bin:\$PATH
    cd /tmp/p5-new
    rm -rf $rprof
    GRAINFS_CLUSTER_URL='$BOOT_HOSTS' \
    GRAINFS_ACCESS_KEY='$BOOT_ACCESS_KEY' \
    GRAINFS_SECRET_KEY='$BOOT_SECRET_KEY' \
    BINARY=/tmp/grainfs-$arm \
    TARGETS=grainfs-cluster \
    WARP_OPS='$WARP_OPS' WARP_OBJ_SIZE='$WARP_OBJ_SIZE' \
    WARP_CONCURRENT='$WARP_CONCURRENT' WARP_DURATION='$WARP_DURATION' \
    WARP_NOCLEAR=1 \
    PROFILE_ROOT='$rprof' \
    bash benchmarks/bench_s3_compat_compare.sh || echo 'BENCH_NONZERO'
    echo '--- warp-results.tsv ---'; cat $rprof/warp-results.tsv 2>/dev/null || echo MISSING
  "
  # pull the tsv into the layout the verdict python reads
  local localdir="$RESULT_DIR/run$idx/$arm"
  mkdir -p "$localdir"
  gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    "$CLIENT:$rprof/warp-results.tsv" "$localdir/warp-results.tsv" >/dev/null 2>&1 \
    || log "WARN: no warp-results.tsv pulled for $arm run$idx"
  log "arm=$arm run=$idx pulled -> $localdir"
}

# ----------------------------------------------------------- verdict -----------
cmd_verdict() {
  log "aggregating $RESULT_DIR -> verdict.md"
  # reuse the validated aggregation python embedded in cross_binary_ab.sh
  sed -n '/^python3 - "\$CROSS_ROOT"/,/^PY$/p' "$REPO_ROOT/benchmarks/cross_binary_ab.sh" \
    | sed '1d;$d' > /tmp/p5-agg.py
  python3 /tmp/p5-agg.py "$RESULT_DIR" "$RUNS" "$PUT_WIN_MIN" "$NOREG_MIN" "$NEW_REF" "$OLD_REF" 1
}

# ----------------------------------------------------------- down --------------
cmd_down() {
  log "TEARDOWN: deleting all VMs"
  local names=("$CLIENT")
  for i in $(seq 0 $((NODE_COUNT - 1))); do names+=("$(node_name "$i")"); done
  gcloud compute instances delete "${names[@]}" --zone="$ZONE" --project="$PROJECT" --quiet >&2 || true
  log "down done"
}

cmd_full() {
  cmd_up || return 1
  cmd_build || return 1
  for i in $(seq 1 "$RUNS"); do
    cmd_arm new "$i" || return 1
    cmd_arm old "$i" || return 1
  done
  cmd_verdict
  log "full done — results in $RESULT_DIR. Run '$0 down' to delete VMs."
}

case "${1:-}" in
  up) cmd_up ;;
  prep-client) cmd_prep_client ;;
  build) cmd_build ;;
  arm) cmd_arm "${2:?arm new|old}" "${3:?runidx}" ;;
  boot) reset_cluster; boot_cluster "${2:?new|old}" ;;
  verdict) cmd_verdict ;;
  down) cmd_down ;;
  full) cmd_full ;;
  ip) for i in $(seq 0 $((NODE_COUNT-1))); do echo "$(node_name "$i") $(internal_ip "$(node_name "$i")")"; done ;;
  *) echo "usage: $0 {up|prep-client|build|arm <new|old> <i>|boot <new|old>|verdict|down|full|ip}" >&2; exit 2 ;;
esac
