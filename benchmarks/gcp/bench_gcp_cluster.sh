#!/usr/bin/env bash
# Multi-node GCP GrainFS/MinIO benchmark harness.
#
# Provisions storage VMs plus one warp/builder client in one GCP zone, builds
# NEW_REF on a Linux VM (duckdb cgo can't cross-compile from darwin), boots
# GrainFS and MinIO targets, runs signed MinIO warp workloads from the in-network
# client, and pulls results into a stable run layout.
#
# Subcommands (run incrementally):
#   up                 provision storage VMs + client (idempotent)
#   build              git-archive NEW_REF -> client -> go build -> fan out
#   grainfs-cluster [i]
#                      reset+boot GrainFS cluster, warp, pull run<i>
#   minio [i]          install+run MinIO on node-0 (SSE-S3 encrypted), warp from client
#   minio-verdict      print MinIO vs GrainFS-devel throughput comparison
#   minio-cluster [i]  install+run distributed MinIO on storage nodes, warp from client
#   cluster-minio-verdict
#                      print distributed MinIO vs GrainFS cluster comparison
#   single [i]         single-node GrainFS (devel) on node-0 + pprof, warp from client
#   single-verdict     print single-node GrainFS vs MinIO comparison + pprof inspect cmds
#   down               delete all VMs
#
# Cost guard: always `down` when finished.
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
PREFIX="${PREFIX:-gr-bench}"
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
RUNS="${RUNS:-1}"

# warp workload (passed through to bench_s3_compat_compare.sh on the client)
WARP_OPS="${WARP_OPS:-put,get,stat}"
WARP_OBJ_SIZE="${WARP_OBJ_SIZE:-10MiB}"
WARP_CONCURRENT="${WARP_CONCURRENT:-32}"
WARP_DURATION="${WARP_DURATION:-1m}"
WARP_OBJECTS="${WARP_OBJECTS:-4096}"

RESULT_DIR="${RESULT_DIR:-$REPO_ROOT/benchmarks/profiles/gcp-$(date +%Y%m%d-%H%M%S)}"
REMOTE_USER="${REMOTE_USER:-$(whoami)}"
DATA_DIR="/var/lib/gfs"
BIN_DIR="/opt/grainfs"
HTTP_PORT=9000
RAFT_PORT=7000
JOIN_PORT=7100
PSK="gcp-grainfs-minio-cluster-key"

MINIO_PORT="${MINIO_PORT:-9001}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-10001}"
MINIO_UNIT="minio-bench"
PPROF_PORT="${PPROF_PORT:-6060}"
PPROF_CPU_SECONDS="${PPROF_CPU_SECONDS:-30}"
PPROF_TRACE_SECONDS="${PPROF_TRACE_SECONDS:-5}"   # execution trace (on+off CPU); 0 to skip
PPROF_WARMUP_SECONDS="${PPROF_WARMUP_SECONDS:-5}"
CLUSTER_PPROF="${CLUSTER_PPROF:-0}"
CLUSTER_PPROF_BASE_PORT="${CLUSTER_PPROF_BASE_PORT:-6060}"
CLUSTER_PUT_TRACE="${CLUSTER_PUT_TRACE:-0}"

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
  log "git-archive $NEW_REF -> client -> build"
  ( cd "$REPO_ROOT" && git archive --format=tar.gz "$NEW_REF" -o /tmp/p5-src-new.tgz )
  scp_to /tmp/p5-src-new.tgz "$CLIENT:/tmp/p5-src-new.tgz"
  ssh_node "$CLIENT" "
    set -e
    export PATH=/usr/local/go/bin:\$PATH
    rm -rf /tmp/p5-new && mkdir -p /tmp/p5-new
    tar -C /tmp/p5-new -xzf /tmp/p5-src-new.tgz
    cd /tmp/p5-new
    echo '[build grainfs] go build...'
    CGO_ENABLED=1 go build -ldflags '-s -w' -o /tmp/grainfs-new ./cmd/grainfs/
    /tmp/grainfs-new --version
    echo build-done
  " || return 1
  # fan out the GrainFS binary to every storage node
  log "fanning out binaries to storage nodes"
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    local n; n="$(node_name "$i")"
    ssh_node "$n" "sudo mkdir -p $BIN_DIR && sudo chown $REMOTE_USER $BIN_DIR"
  done
  # pull binaries to local, push to each storage node (scp is VM<->local only).
  # The IAP-tunnelled scp intermittently corrupts the ~122MB binary (silent
  # "fatal error: invalid runtime symbol table" at boot), so every hop is md5-
  # verified and retried — a corrupt binary otherwise wastes a full boot cycle.
  local md5 nm
  md5="$(ssh_node "$CLIENT" "md5sum /tmp/grainfs-new | cut -d' ' -f1")"
  push_verified "$CLIENT:/tmp/grainfs-new" "/tmp/grainfs-new" "$md5" || { log "ERROR: client->local grainfs-new corrupt"; return 1; }
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    nm="$(node_name "$i")"
    push_verified "/tmp/grainfs-new" "$nm:$BIN_DIR/grainfs-new" "$md5" "$nm" || { log "ERROR: ->$nm grainfs-new corrupt"; return 1; }
  done
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    ssh_node "$(node_name "$i")" "chmod +x $BIN_DIR/grainfs-new"
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

install_minio_binary() { # node-name
  local n="$1"
  ssh_node "$n" "
    curl -fsSL https://dl.min.io/server/minio/release/linux-amd64/minio -o /tmp/minio-dl
    if ! file /tmp/minio-dl 2>/dev/null | grep -q ELF; then
      echo 'ERROR: minio download is not an ELF binary (got HTML/redirect?)'
      file /tmp/minio-dl
      exit 1
    fi
    sudo install -m 755 /tmp/minio-dl /usr/local/bin/minio
    /usr/local/bin/minio --version
  "
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
      sudo pkill -9 -x grainfs-new 2>/dev/null || true
      sudo pkill -9 -x grainfs 2>/dev/null || true
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

# serve_node <node_idx> <bin> [invite_bundle] [pprof_port]
serve_node() {
  local idx="$1" bin="$2" bundle="${3:-}" pprof_port="${4:-0}"
  local n; n="$(node_name "$idx")"
  local ipi; ipi="$(internal_ip "$n")"
  local invite_env=""
  if [ -z "$bundle" ]; then
    # genesis: pre-stage cluster PSK (root-owned dir)
    ssh_node "$n" "printf '%s\n' '$PSK' | sudo tee $DATA_DIR/keys.d/current.key >/dev/null"
  else
    invite_env="--setenv=GRAINFS_INVITE_BUNDLE=$bundle"
  fi
  local pprof_flag=""
  if (( pprof_port > 0 )); then pprof_flag="--pprof-port $pprof_port"; fi
  local directio_env=""
  if [[ -n "${GRAINFS_SHARD_DIRECT_IO:-}" ]]; then
    directio_env="--setenv=GRAINFS_SHARD_DIRECT_IO=$GRAINFS_SHARD_DIRECT_IO"
  fi
  local s3_etag_env=""
  if [[ "${GRAINFS_S3_ETAG_MD5:-1}" == "0" ]]; then
    s3_etag_env="--setenv=GRAINFS_S3_ETAG_MD5=0"
  fi
  local trace_env=""
  if [[ "$CLUSTER_PUT_TRACE" == "1" ]]; then
    ssh_node "$n" "sudo rm -f /tmp/grainfs-put-trace-node$idx.jsonl"
    trace_env="--setenv=GRAINFS_PUT_TRACE_FILE=/tmp/grainfs-put-trace-node$idx.jsonl --setenv=GRAINFS_NODE_ID=p5-node-$idx"
  fi
  ssh_node "$n" "sudo systemd-run --unit=$UNIT --collect $invite_env $directio_env $s3_etag_env $trace_env \
    $bin serve --data $DATA_DIR --port $HTTP_PORT --node-id p5-node-$idx \
    --raft-addr $ipi:$RAFT_PORT --join-listen-addr $ipi:$JOIN_PORT \
    --raft-heartbeat-interval ${RAFT_HEARTBEAT:-1s} --raft-election-timeout ${RAFT_ELECTION:-3s} \
    --scrub-interval 0 --lifecycle-interval 0 --log-level ${LOG_LEVEL:-warn} \
    $pprof_flag \
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

# boot_cluster -> exports BOOT_ACCESS_KEY/BOOT_SECRET_KEY/BOOT_HOSTS
boot_cluster() {
  local bin="$BIN_DIR/grainfs-new"
  local ip0; ip0="$(internal_ip "$(node_name 0)")"
  BOOT_PPROF_PORTS=""
  local pprof0=0
  if [[ "$CLUSTER_PPROF" == "1" ]]; then
    pprof0="$CLUSTER_PPROF_BASE_PORT"
    BOOT_PPROF_PORTS="$pprof0"
    log "grainfs-cluster: pprof enabled base_port=$CLUSTER_PPROF_BASE_PORT"
  fi
  log "grainfs-cluster: boot genesis=$(node_name 0)@$ip0"
  serve_node 0 "$bin" "" "$pprof0"
  if ! wait_http 0; then log "ERROR: grainfs-cluster genesis never came up — aborting"; return 1; fi
  ssh_node "$(node_name 0)" "for _ in \$(seq 1 60); do sudo test -f $DATA_DIR/cluster.id && break; sleep 0.5; done; echo genesis-up"
  # bootstrap IAM SA on node-0 (admin.sock is root-owned)
  local saj
  saj="$(ssh_node "$(node_name 0)" "
    sudo $bin iam --json sa create phase5 --endpoint $DATA_DIR/admin.sock
  ")"
  if [ -z "$saj" ] || ! echo "$saj" | python3 -c 'import json,sys;json.load(sys.stdin)["access_key"]' 2>/dev/null; then
    log "ERROR: grainfs-cluster IAM SA create returned no JSON: ${saj:0:120}"; return 1
  fi
  BOOT_ACCESS_KEY="$(echo "$saj" | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_key"])')"
  BOOT_SECRET_KEY="$(echo "$saj" | python3 -c 'import json,sys;print(json.load(sys.stdin)["secret_key"])')"
  local sa_id; sa_id="$(echo "$saj" | python3 -c 'import json,sys;print(json.load(sys.stdin)["sa_id"])')"
  log "grainfs-cluster: SA created access=${BOOT_ACCESS_KEY:0:8}..."
  # joiners 1..N-1 (serial: each mint needs the live leader)
  for i in $(seq 1 $((NODE_COUNT - 1))); do
    local bundle
    bundle="$(ssh_node "$(node_name 0)" "sudo $bin cluster invite create --endpoint $DATA_DIR/admin.sock" \
      | awk 'f{print;exit} /GRAINFS_INVITE_BUNDLE:/{f=1}')"
    if [ -z "$bundle" ]; then log "ERROR: empty invite bundle for node $i"; return 1; fi
    local pprof_port=0
    if [[ "$CLUSTER_PPROF" == "1" ]]; then
      pprof_port=$((CLUSTER_PPROF_BASE_PORT + i))
      BOOT_PPROF_PORTS="${BOOT_PPROF_PORTS:+$BOOT_PPROF_PORTS,}$pprof_port"
    fi
    serve_node "$i" "$bin" "$bundle" "$pprof_port"
    wait_http "$i" >/dev/null
  done
  # wait leader + shard groups (poll on node-0 over ssh; mac can't reach internal IPs)
  local expected=$((NODE_COUNT * 4)); [ "$expected" -lt 8 ] && expected=8
  log "grainfs-cluster: waiting leader + $expected shard groups"
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
  log "grainfs-cluster: ready hosts=$BOOT_HOSTS"
}

cluster_pprof_enabled() {
  [[ "$CLUSTER_PPROF" == "1" && -n "${BOOT_PPROF_PORTS:-}" ]]
}

# Captures, per node, one load window aligned with the CPU profile:
#   cpu/node<i>.pb.gz            — on-CPU profile (PPROF_CPU_SECONDS)
#   load/node<i>/block.before    — block contention counter at window start
#   load/node<i>/block.after     — block contention counter at window end
#   load/node<i>/mutex.before    — mutex contention counter at window start
#   load/node<i>/mutex.after     — mutex contention counter at window end
#   load/node<i>/goroutine.pb.gz — goroutine snapshot under load (window end)
# block/mutex are cumulative counters (grainfs sets rate=1 when --pprof-port>0),
# so the in-window delta — go tool pprof -base block.before block.after —
# isolates contention during the load window. This surfaces OFF-CPU waits (disk
# fsync, inter-node shard transfer, lock serialization) that the CPU profile
# cannot see: the live limiter for IO-bound PUT. The goroutine snapshot taken at
# the window end shows where goroutines are parked (e.g. bounded shard-write
# fan-out) while the box runs <1 core busy.
capture_cluster_load_profiles() { # <runidx> <pprof_dir>
  local idx="$1" pprof_dir="$2"
  cluster_pprof_enabled || return 0
  mkdir -p "$pprof_dir/cpu"
  local ports=()
  IFS=',' read -ra ports <<<"$BOOT_PPROF_PORTS"
  log "grainfs-cluster: run=$idx pprof load capture ${PPROF_WARMUP_SECONDS}s warmup + ${PPROF_TRACE_SECONDS}s trace + ${PPROF_CPU_SECONDS}s (cpu+block+mutex+goroutine)"
  local pids=()
  local i
  for i in "${!ports[@]}"; do
    (
      local n port remote_cpu out_cpu node_dir rb
      n="$(node_name "$i")"
      port="${ports[$i]}"
      node_dir="$pprof_dir/load/node${i}"
      mkdir -p "$node_dir"
      remote_cpu="/tmp/grainfs-cluster-${idx}-cpu-node${i}.pb.gz"
      out_cpu="$pprof_dir/cpu/node${i}.pb.gz"
      rb="/tmp/gr-${idx}-n${i}"   # remote basename for load profiles
      sleep "$PPROF_WARMUP_SECONDS"
      # execution trace FIRST (under load, before the CPU window) so its heavy
      # scheduler-event recording does not inflate the CPU profile. go tool trace
      # shows wall-clock on+off CPU: syscall blocking (disk fsync), network wait,
      # GC, and scheduler latency — the ground-truth view of what stalls PUT.
      if (( PPROF_TRACE_SECONDS > 0 )); then
        ssh_node "$n" "curl -sf 'http://127.0.0.1:${port}/debug/pprof/trace?seconds=$PPROF_TRACE_SECONDS' -o '${rb}-trace.out' 2>/dev/null" || true
      fi
      # window-start contention baselines (cumulative counters)
      ssh_node "$n" "curl -sf 'http://127.0.0.1:${port}/debug/pprof/block' -o '${rb}-block-before.pb.gz' 2>/dev/null; \
                     curl -sf 'http://127.0.0.1:${port}/debug/pprof/mutex' -o '${rb}-mutex-before.pb.gz' 2>/dev/null" || true
      # CPU profile defines the load window (blocking, PPROF_CPU_SECONDS long)
      ssh_node "$n" "curl -sf 'http://127.0.0.1:${port}/debug/pprof/profile?seconds=$PPROF_CPU_SECONDS' -o '$remote_cpu' 2>/dev/null && echo pprof-cpu-node${i}-done" || true
      # window-end contention + goroutine snapshots (still under load).
      # goroutine.pb.gz = aggregated stacks for `go tool pprof`.
      # goroutine.txt (debug=2) = full per-goroutine stacks WITH wait state/reason
      # (e.g. "IO wait", "syscall", "chan receive 3 minutes") — this is the only
      # view that surfaces OFF-CPU syscall waits (disk fsync, network), which the
      # block/mutex profilers do not record. Count goroutines parked in fsync vs
      # net vs chan to locate the live PUT limiter.
      ssh_node "$n" "curl -sf 'http://127.0.0.1:${port}/debug/pprof/block' -o '${rb}-block-after.pb.gz' 2>/dev/null; \
                     curl -sf 'http://127.0.0.1:${port}/debug/pprof/mutex' -o '${rb}-mutex-after.pb.gz' 2>/dev/null; \
                     curl -sf 'http://127.0.0.1:${port}/debug/pprof/goroutine' -o '${rb}-goroutine.pb.gz' 2>/dev/null; \
                     curl -sf 'http://127.0.0.1:${port}/debug/pprof/goroutine?debug=2' -o '${rb}-goroutine.txt' 2>/dev/null" || true
      gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
        "$n:$remote_cpu" "$out_cpu" >/dev/null 2>&1 || log "WARN: cluster CPU profile pull failed for $n"
      local pr
      for pr in block-before block-after mutex-before mutex-after goroutine; do
        gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
          "$n:${rb}-${pr}.pb.gz" "$node_dir/${pr/-/.}.pb.gz" >/dev/null 2>&1 || true
      done
      gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
        "$n:${rb}-goroutine.txt" "$node_dir/goroutine.txt" >/dev/null 2>&1 || true
      if (( PPROF_TRACE_SECONDS > 0 )); then
        gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
          "$n:${rb}-trace.out" "$node_dir/trace.out" >/dev/null 2>&1 || true
      fi
    ) &
    pids+=($!)
  done
  wait "${pids[@]}" 2>/dev/null || true
  log "grainfs-cluster: run=$idx pprof load profiles -> $pprof_dir/cpu/ + $pprof_dir/load/"
}

collect_cluster_pprof_snapshots() { # <runidx> <pprof_dir>
  local idx="$1" pprof_dir="$2"
  cluster_pprof_enabled || return 0
  local ports=()
  IFS=',' read -ra ports <<<"$BOOT_PPROF_PORTS"
  # heap/allocs only: in-use/cumulative allocation counters stay meaningful when
  # read post-warp. goroutine/mutex/block are captured under load by
  # capture_cluster_load_profiles (a post-warp idle snapshot would miss the live
  # limiter).
  log "grainfs-cluster: run=$idx pprof collecting post-warp heap/allocs snapshots"
  local i prof
  for i in "${!ports[@]}"; do
    local n node_dir
    n="$(node_name "$i")"
    node_dir="$pprof_dir/snap/node${i}"
    mkdir -p "$node_dir"
    for prof in heap allocs; do
      local remote="/tmp/grainfs-cluster-${idx}-${prof}-node${i}.pb.gz"
      ssh_node "$n" "curl -sf 'http://127.0.0.1:${ports[$i]}/debug/pprof/$prof' -o '$remote' 2>/dev/null" || true
      gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
        "$n:$remote" "$node_dir/$prof.pb.gz" >/dev/null 2>&1 || true
    done
  done
  log "grainfs-cluster: run=$idx pprof snapshots -> $pprof_dir/snap/"
}

# --------------------------------------------------- grainfs-cluster ----------
# Emits warp-results.tsv to a remote dir, pulled into grainfs-cluster/run<i>/.
cmd_grainfs_cluster() { # <runidx>
  local idx="${1:-1}"
  reset_cluster
  boot_cluster || return 1
  local rprof="/tmp/p5-prof-grainfs-cluster-$idx"
  local localdir="$RESULT_DIR/grainfs-cluster/run$idx"
  local pprof_dir="$localdir/pprof"
  mkdir -p "$localdir"
  log "grainfs-cluster: run=$idx warp via bench_s3_compat_compare.sh external mode"
  local pprof_pid=""
  if cluster_pprof_enabled; then
    capture_cluster_load_profiles "$idx" "$pprof_dir" &
    pprof_pid=$!
  fi
  # push the repo's benchmark scripts to the client (external cluster mode reuses
  # all warp+parse+tsv logic). The client already has the source from build.
  ssh_node "$CLIENT" "
    export PATH=/usr/local/go/bin:\$PATH
    cd /tmp/p5-new
    rm -rf $rprof
    GRAINFS_CLUSTER_URL='$BOOT_HOSTS' \
    GRAINFS_ACCESS_KEY='$BOOT_ACCESS_KEY' \
    GRAINFS_SECRET_KEY='$BOOT_SECRET_KEY' \
    BINARY=/tmp/grainfs-new \
    TARGETS=grainfs-cluster \
    WARP_OPS='$WARP_OPS' WARP_OBJ_SIZE='$WARP_OBJ_SIZE' \
    WARP_CONCURRENT='$WARP_CONCURRENT' WARP_DURATION='$WARP_DURATION' \
    WARP_OBJECTS='$WARP_OBJECTS' \
    WARP_WRITE_AUTOTERM='${WARP_WRITE_AUTOTERM:-1}' \
    WARP_NOCLEAR=1 \
    PROFILE_ROOT='$rprof' \
    bash benchmarks/bench_s3_compat_compare.sh || echo 'BENCH_NONZERO'
    echo '--- warp-results.tsv ---'; cat $rprof/warp-results.tsv 2>/dev/null || echo MISSING
  "
  if [[ -n "$pprof_pid" ]]; then
    wait "$pprof_pid" 2>/dev/null || true
    collect_cluster_pprof_snapshots "$idx" "$pprof_dir"
  fi
  gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    "$CLIENT:$rprof/warp-results.tsv" "$localdir/warp-results.tsv" >/dev/null 2>&1 \
    || log "WARN: no warp-results.tsv pulled for grainfs-cluster run$idx"
  if [[ ! -s "$localdir/warp-results.tsv" ]]; then
    log "ERROR: empty warp-results.tsv for grainfs-cluster run$idx"
    return 1
  fi
  mkdir -p "$localdir/raw"
  gcloud compute scp --recurse --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    "$CLIENT:$rprof" "$localdir/raw/" >/dev/null 2>&1 \
    || log "WARN: raw profile pull failed for grainfs-cluster run$idx"
  if [[ "$CLUSTER_PUT_TRACE" == "1" ]]; then
    mkdir -p "$localdir/put-trace"
    for i in $(seq 0 $((NODE_COUNT - 1))); do
      local n
      n="$(node_name "$i")"
      ssh_node "$n" "sudo cp /tmp/grainfs-put-trace-node$i.jsonl /tmp/grainfs-put-trace-node$i.pull.jsonl 2>/dev/null && sudo chmod 0644 /tmp/grainfs-put-trace-node$i.pull.jsonl" || true
      gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
        "$n:/tmp/grainfs-put-trace-node$i.pull.jsonl" "$localdir/put-trace/node$i.jsonl" >/dev/null 2>&1 \
        || log "WARN: put trace pull failed for $n"
    done
  fi
  log "grainfs-cluster: run=$idx pulled -> $localdir"
}

# --------------------------------------------------------- minio arm -----------
# Runs MinIO (single-node, single-drive) on node-0 and benchmarks from the
# client with the same warp parameters used by GrainFS arms.
# Encryption: MINIO_KMS_AUTO_ENCRYPTION=on (server-side SSE-S3 via inline KMS
# master key) — mirrors GrainFS XAES-256-GCM at-rest encryption for a fair
# comparison.  Note: single node vs 4-node cluster; latency/throughput scale
# differently, but it gives an absolute MinIO baseline on identical hardware.
cmd_minio() { # <runidx>
  local idx="${1:-1}"
  local n; n="$(node_name 0)"
  local ip0; ip0="$(internal_ip "$n")"
  local minio_data="/var/lib/minio-bench"

  log "minio: installing binary on $n"
  # Always re-download to avoid stale corrupt installs. Verify ELF before
  # installing — dl.min.io returns 200 HTML on quota/redirect errors.
  install_minio_binary "$n"

  log "minio: reset node-0 (stop grainfs + minio, wipe $minio_data)"
  ssh_node "$n" "
    sudo systemctl stop $UNIT 2>/dev/null || true
    sudo systemctl stop $MINIO_UNIT 2>/dev/null || true
    sudo pkill -9 -x minio 2>/dev/null || true
    for _ in \$(seq 1 30); do
      (exec 3<>/dev/tcp/127.0.0.1/$MINIO_PORT) 2>/dev/null && { exec 3>&- 3<&-; sleep 0.5; } || break
    done
    sudo rm -rf $minio_data && sudo mkdir -p $minio_data
  "

  # AES-256 key for SSE-S3 auto-encryption. MinIO parses MINIO_KMS_SECRET_KEY
  # value as base64 and rejects non-{16,24,32}-byte decoded lengths; openssl
  # rand 32 | base64 produces exactly 32 bytes when decoded (AES-256).
  local kms_key; kms_key="$(openssl rand 32 | base64 | tr -d '\n')"

  log "minio: starting on $n:$MINIO_PORT (SSE-S3 MINIO_KMS_AUTO_ENCRYPTION=on)"
  ssh_node "$n" "sudo systemd-run --unit=$MINIO_UNIT --collect \
    --setenv=MINIO_ROOT_USER=minioadmin \
    --setenv=MINIO_ROOT_PASSWORD=minioadmin \
    '--setenv=MINIO_KMS_SECRET_KEY=warp-bench:$kms_key' \
    --setenv=MINIO_KMS_AUTO_ENCRYPTION=on \
    /usr/local/bin/minio server $minio_data --address :$MINIO_PORT \
    && echo minio-launched"

  log "minio: waiting for health endpoint on $n:$MINIO_PORT"
  local minio_up
  minio_up="$(ssh_node "$n" "
    for _ in \$(seq 1 90); do
      curl -sf http://127.0.0.1:$MINIO_PORT/minio/health/live >/dev/null 2>&1 && { echo minio-http-up; exit 0; }; sleep 1
    done; echo minio-http-down; sudo journalctl -u $MINIO_UNIT --no-pager 2>/dev/null | tail -8
  ")"
  echo "$minio_up"
  if ! [[ "$minio_up" == *minio-http-up* ]]; then
    log "ERROR: minio never came up on $n:$MINIO_PORT — aborting"
    return 1
  fi

  local rprof="/tmp/p5-prof-minio-$idx"
  log "minio: run=$idx warp from client -> $ip0:$MINIO_PORT"
  ssh_node "$CLIENT" "
    export PATH=/usr/local/go/bin:\$PATH
    cd /tmp/p5-new
    rm -rf $rprof
    MINIO_URL='http://$ip0:$MINIO_PORT' \
    MINIO_ACCESS_KEY='minioadmin' \
    MINIO_SECRET_KEY='minioadmin' \
    TARGETS=minio \
    WARP_OPS='$WARP_OPS' WARP_OBJ_SIZE='$WARP_OBJ_SIZE' \
    WARP_CONCURRENT='$WARP_CONCURRENT' WARP_DURATION='$WARP_DURATION' \
    WARP_OBJECTS='$WARP_OBJECTS' \
    WARP_WRITE_AUTOTERM='${WARP_WRITE_AUTOTERM:-1}' \
    WARP_NOCLEAR=1 \
    PROFILE_ROOT='$rprof' \
    bash benchmarks/bench_s3_compat_compare.sh || echo 'BENCH_NONZERO'
    echo '--- warp-results.tsv ---'; cat $rprof/warp-results.tsv 2>/dev/null || echo MISSING
  "

  local localdir="$RESULT_DIR/minio/run$idx"
  mkdir -p "$localdir"
  gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    "$CLIENT:$rprof/warp-results.tsv" "$localdir/warp-results.tsv" >/dev/null 2>&1 \
    || log "WARN: no warp-results.tsv pulled for minio run$idx"
  mkdir -p "$localdir/raw"
  gcloud compute scp --recurse --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    "$CLIENT:$rprof" "$localdir/raw/" >/dev/null 2>&1 \
    || log "WARN: raw profile pull failed for minio run$idx"
  log "minio: run=$idx pulled -> $localdir"
}

# ------------------------------------------------- distributed minio arm -------
# Runs distributed MinIO across the same storage VMs used by GrainFS cluster and
# benchmarks from the client with the same warp parameters.
cmd_minio_cluster() { # <runidx>
  local idx="${1:-1}"
  local minio_data="/var/lib/minio-cluster-bench"
  local urls=""
  local endpoint_args=""
  local i n ipi

  if (( NODE_COUNT < 4 )); then
    log "ERROR: minio-cluster requires NODE_COUNT>=4; got $NODE_COUNT"
    return 1
  fi

  log "minio-cluster: installing binary on all storage nodes"
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    install_minio_binary "$(node_name "$i")" || return 1
  done

  log "minio-cluster: reset storage nodes (stop grainfs + minio, wipe $minio_data)"
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    n="$(node_name "$i")"
    ssh_node "$n" "
      sudo systemctl stop $UNIT 2>/dev/null || true
      sudo systemctl stop $MINIO_UNIT 2>/dev/null || true
      sudo systemctl reset-failed $UNIT 2>/dev/null || true
      sudo systemctl reset-failed $MINIO_UNIT 2>/dev/null || true
      sudo pkill -9 -x grainfs-new 2>/dev/null || true
      sudo pkill -9 -x grainfs 2>/dev/null || true
      sudo pkill -9 -x minio 2>/dev/null || true
      for port in $HTTP_PORT $MINIO_PORT; do
        for _ in \$(seq 1 30); do
          (exec 3<>/dev/tcp/127.0.0.1/\$port) 2>/dev/null && { exec 3>&- 3<&-; sleep 0.5; } || break
        done
      done
      sudo rm -rf $minio_data && sudo mkdir -p $minio_data
    " || return 1
  done

  for i in $(seq 0 $((NODE_COUNT - 1))); do
    ipi="$(internal_ip "$(node_name "$i")")"
    urls="${urls:+$urls,}http://$ipi:$MINIO_PORT"
    endpoint_args="${endpoint_args:+$endpoint_args }http://$ipi:$MINIO_PORT$minio_data"
  done

  local kms_key; kms_key="$(openssl rand -base64 32 | tr -d '\n')"
  log "minio-cluster: starting distributed MinIO nodes (SSE-S3 MINIO_KMS_AUTO_ENCRYPTION=on)"
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    n="$(node_name "$i")"
    ssh_node "$n" "sudo systemd-run --unit=$MINIO_UNIT --collect \
      --setenv=MINIO_CI_CD=1 \
      --setenv=MINIO_ROOT_USER=minioadmin \
      --setenv=MINIO_ROOT_PASSWORD=minioadmin \
      '--setenv=MINIO_KMS_SECRET_KEY=warp-bench:$kms_key' \
      --setenv=MINIO_KMS_AUTO_ENCRYPTION=on \
      /usr/local/bin/minio server $endpoint_args \
      --address :$MINIO_PORT --console-address :$MINIO_CONSOLE_PORT \
      && echo minio-cluster-node-$i-launched" || return 1
  done

  log "minio-cluster: waiting for live endpoints"
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    n="$(node_name "$i")"
    local minio_up
    minio_up="$(ssh_node "$n" "
      for _ in \$(seq 1 120); do
        curl -sf http://127.0.0.1:$MINIO_PORT/minio/health/live >/dev/null 2>&1 && { echo minio-http-up; exit 0; }; sleep 1
      done; echo minio-http-down; sudo journalctl -u $MINIO_UNIT --no-pager 2>/dev/null | tail -8
    ")"
    echo "$minio_up"
    if ! [[ "$minio_up" == *minio-http-up* ]]; then
      log "ERROR: minio-cluster node $n never came up — aborting"
      return 1
    fi
  done

  n="$(node_name 0)"
  log "minio-cluster: waiting for cluster health on $n:$MINIO_PORT"
  local cluster_up
  cluster_up="$(ssh_node "$n" "
    for _ in \$(seq 1 240); do
      curl -sf http://127.0.0.1:$MINIO_PORT/minio/health/cluster >/dev/null 2>&1 && { echo minio-cluster-up; exit 0; }; sleep 0.5
    done; echo minio-cluster-down; sudo journalctl -u $MINIO_UNIT --no-pager 2>/dev/null | tail -20
  ")"
  echo "$cluster_up"
  if ! [[ "$cluster_up" == *minio-cluster-up* ]]; then
    log "ERROR: minio-cluster never reported healthy — aborting"
    return 1
  fi

  local rprof="/tmp/p5-prof-minio-cluster-$idx"
  log "minio-cluster: run=$idx warp from client -> $urls"
  ssh_node "$CLIENT" "
    export PATH=/usr/local/go/bin:\$PATH
    cd /tmp/p5-new
    rm -rf $rprof
    MINIO_CLUSTER_URL='$urls' \
    MINIO_ACCESS_KEY='minioadmin' \
    MINIO_SECRET_KEY='minioadmin' \
    TARGETS=minio-cluster \
    WARP_OPS='$WARP_OPS' WARP_OBJ_SIZE='$WARP_OBJ_SIZE' \
    WARP_CONCURRENT='$WARP_CONCURRENT' WARP_DURATION='$WARP_DURATION' \
    WARP_OBJECTS='$WARP_OBJECTS' \
    WARP_WRITE_AUTOTERM='${WARP_WRITE_AUTOTERM:-1}' \
    WARP_NOCLEAR=1 \
    PROFILE_ROOT='$rprof' \
    bash benchmarks/bench_s3_compat_compare.sh || echo 'BENCH_NONZERO'
    echo '--- warp-results.tsv ---'; cat $rprof/warp-results.tsv 2>/dev/null || echo MISSING
  "

  local localdir="$RESULT_DIR/minio-cluster/run$idx"
  mkdir -p "$localdir"
  gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    "$CLIENT:$rprof/warp-results.tsv" "$localdir/warp-results.tsv" >/dev/null 2>&1 \
    || log "WARN: no warp-results.tsv pulled for minio-cluster run$idx"
  if [[ ! -s "$localdir/warp-results.tsv" ]]; then
    log "ERROR: empty warp-results.tsv for minio-cluster run$idx"
    return 1
  fi
  mkdir -p "$localdir/raw" "$localdir/logs"
  gcloud compute scp --recurse --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    "$CLIENT:$rprof" "$localdir/raw/" >/dev/null 2>&1 \
    || log "WARN: raw profile pull failed for minio-cluster run$idx"
  for i in $(seq 0 $((NODE_COUNT - 1))); do
    n="$(node_name "$i")"
    ssh_node "$n" "sudo journalctl -u $MINIO_UNIT --no-pager -n 300" >"$localdir/logs/$n.log" 2>/dev/null || true
  done
  log "minio-cluster: run=$idx pulled -> $localdir"
}

# ----------------------------------------------------------- minio-verdict ------
# Print a side-by-side summary of MinIO vs GrainFS-devel throughput.
# TSV columns: target, op, throughput_MBps, ops_per_sec, p50_ms, p99_ms
cmd_minio_verdict() {
  log "minio comparison: $RESULT_DIR"
  echo ""
  echo "=== MinIO vs GrainFS-devel comparison (warp-results.tsv) ==="
  echo ""
  echo "--- GrainFS single node (devel, EC 4+2, XAES-256-GCM) ---"
  cat "$RESULT_DIR"/single/run*/warp-results.tsv 2>/dev/null || echo "  (no results)"
  echo ""
  echo "--- MinIO (single node, SSE-S3 auto-encryption) ---"
  cat "$RESULT_DIR"/minio/run*/warp-results.tsv 2>/dev/null || echo "  (no results)"
  echo ""
}

# ----------------------------------------------------------- cluster verdict ---
cmd_cluster_minio_verdict() {
  log "cluster comparison: $RESULT_DIR"
  echo ""
  echo "=== Cluster GrainFS (devel) vs distributed MinIO comparison ==="
  echo ""
  echo "--- GrainFS cluster (devel, EC 4+2, XAES-256-GCM) ---"
  cat "$RESULT_DIR"/grainfs-cluster/run*/warp-results.tsv 2>/dev/null || echo "  (no results)"
  echo ""
  echo "--- MinIO distributed cluster (SSE-S3 auto-encryption) ---"
  cat "$RESULT_DIR"/minio-cluster/run*/warp-results.tsv 2>/dev/null || echo "  (no results)"
  echo ""
}

# --------------------------------------------------------- single-node ----------
# Single-node GrainFS (devel) with pprof — goal: amd64 re-confirm whether
# MD5 ETag still dominates write CPU (was 68% arm64; unclear on amd64 with
# hardware MD5 asm). CPU profile captured during warp PUT; heap/allocs/goroutine
# collected post-warp. Compare against cmd_minio for throughput baseline.
cmd_single() { # <runidx>
  local idx="${1:-1}"
  local n; n="$(node_name 0)"
  local ip0; ip0="$(internal_ip "$n")"
  local bin="$BIN_DIR/grainfs-new"

  log "single: resetting node-0"
  ssh_node "$n" "
    sudo systemctl stop $UNIT 2>/dev/null || true
    sudo systemctl stop $MINIO_UNIT 2>/dev/null || true
    sudo systemctl reset-failed $UNIT 2>/dev/null || true
    sudo pkill -9 -x grainfs-new 2>/dev/null || true
    sudo pkill -9 -x grainfs 2>/dev/null || true
    sudo pkill -9 -x minio 2>/dev/null || true
    for _ in \$(seq 1 30); do
      (exec 3<>/dev/tcp/127.0.0.1/$HTTP_PORT) 2>/dev/null && { exec 3>&- 3<&-; sleep 0.5; } || break
    done
    sudo rm -rf $DATA_DIR && sudo mkdir -p $DATA_DIR/keys.d
  "

  log "single: starting GrainFS (devel) on $n:$HTTP_PORT pprof=:$PPROF_PORT"
  serve_node 0 "$bin" "" "$PPROF_PORT"
  if ! wait_http 0; then
    log "ERROR: single GrainFS never came up on $n — aborting"
    ssh_node "$n" "sudo journalctl -u $UNIT -n 120 --no-pager 2>/dev/null | grep -iE 'error|panic|failed|listen|bind|flag' | head -20" || true
    return 1
  fi
  # Wait for leader — single node promotes quickly; no shard-group wait needed.
  ssh_node "$n" "
    for _ in \$(seq 1 60); do
      s=\$(curl -sf http://127.0.0.1:$HTTP_PORT/api/cluster/status 2>/dev/null||true)
      st=\$(echo \"\$s\" | python3 -c 'import sys,json;print(json.load(sys.stdin).get(\"state\",\"\"))' 2>/dev/null||true)
      [ \"\$st\" = Leader ] && { echo leader-ready; exit 0; }; sleep 0.5
    done; echo leader-timeout
  "

  # Bootstrap IAM SA
  local saj
  saj="$(ssh_node "$n" "sudo $bin iam --json sa create bench-single --endpoint $DATA_DIR/admin.sock")"
  if ! echo "$saj" | python3 -c 'import json,sys;json.load(sys.stdin)["access_key"]' 2>/dev/null; then
    log "ERROR: single IAM SA create failed: ${saj:0:120}"; return 1
  fi
  local access_key secret_key sa_id
  access_key="$(echo "$saj" | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_key"])')"
  secret_key="$(echo "$saj" | python3 -c 'import json,sys;print(json.load(sys.stdin)["secret_key"])')"
  sa_id="$(echo "$saj" | python3 -c 'import json,sys;print(json.load(sys.stdin)["sa_id"])')"
  log "single: SA access=${access_key:0:8}..."

  # Pre-create warp buckets (grainfs-cluster target names; single node reuses that path)
  for op in ${WARP_OPS//,/ }; do
    ssh_node "$n" "sudo $bin bucket create warp-grainfs-cluster-$op --endpoint $DATA_DIR/admin.sock --attach-sa $sa_id --attach-policy bucket-admin >/dev/null"
  done

  local rprof="/tmp/p5-prof-single-$idx"
  local localdir="$RESULT_DIR/single/run$idx"
  local pprof_dir="$localdir/pprof"
  mkdir -p "$pprof_dir"

  # Background: PPROF_SINGLE_DELAY (default 5s = PUT warm-up) then capture
  # PPROF_CPU_SECONDS of CPU. Raise the delay to land the window on a later op
  # (e.g. the GET measure phase when WARP_OPS=get).
  log "single: spawning background CPU profile capture (${PPROF_SINGLE_DELAY:-5}s delay + ${PPROF_CPU_SECONDS}s)"
  (
    sleep "${PPROF_SINGLE_DELAY:-5}"
    ssh_node "$n" "curl -sf 'http://127.0.0.1:$PPROF_PORT/debug/pprof/profile?seconds=$PPROF_CPU_SECONDS' -o /tmp/single-cpu.pb.gz 2>/dev/null && echo pprof-cpu-done" || true
    if gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
      "$n:/tmp/single-cpu.pb.gz" "$pprof_dir/cpu.pb.gz" >/dev/null 2>&1; then
      log "single: CPU profile -> $pprof_dir/cpu.pb.gz"
    else
      log "WARN: single CPU profile pull failed"
    fi
  ) &
  local pprof_pid=$!

  # Run warp from client (single node = 1-node cluster via grainfs-cluster target).
  log "single: run=$idx warp from client -> $ip0:$HTTP_PORT"
  ssh_node "$CLIENT" "
    export PATH=/usr/local/go/bin:\$PATH
    cd /tmp/p5-new
    rm -rf $rprof
    GRAINFS_CLUSTER_URL='$ip0:$HTTP_PORT' \
    GRAINFS_ACCESS_KEY='$access_key' \
    GRAINFS_SECRET_KEY='$secret_key' \
    BINARY=/tmp/grainfs-new \
    TARGETS=grainfs-cluster \
    WARP_OPS='$WARP_OPS' WARP_OBJ_SIZE='$WARP_OBJ_SIZE' \
    WARP_CONCURRENT='$WARP_CONCURRENT' WARP_DURATION='$WARP_DURATION' \
    WARP_OBJECTS='$WARP_OBJECTS' \
    WARP_WRITE_AUTOTERM='${WARP_WRITE_AUTOTERM:-1}' \
    WARP_NOCLEAR=1 \
    PROFILE_ROOT='$rprof' \
    bash benchmarks/bench_s3_compat_compare.sh || echo 'BENCH_NONZERO'
    echo '--- warp-results.tsv ---'; cat $rprof/warp-results.tsv 2>/dev/null || echo MISSING
  "

  wait "$pprof_pid" 2>/dev/null || true

  # Post-warp snapshots: heap, allocs, goroutine, mutex, block.
  log "single: collecting post-warp pprof snapshots on $n"
  for prof in heap allocs goroutine mutex block; do
    ssh_node "$n" "curl -sf 'http://127.0.0.1:$PPROF_PORT/debug/pprof/$prof' -o /tmp/single-${prof}.pb.gz 2>/dev/null" || true
    gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
      "$n:/tmp/single-${prof}.pb.gz" "$pprof_dir/${prof}.pb.gz" >/dev/null 2>&1 || true
  done
  log "single: pprof snapshots -> $pprof_dir/"

  # Pull warp TSV.
  gcloud compute scp --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    "$CLIENT:$rprof/warp-results.tsv" "$localdir/warp-results.tsv" >/dev/null 2>&1 \
    || log "WARN: no warp-results.tsv for single run$idx"
  if [[ ! -s "$localdir/warp-results.tsv" ]]; then
    log "ERROR: empty warp-results.tsv for single run$idx"
    return 1
  fi
  mkdir -p "$localdir/raw"
  gcloud compute scp --recurse --zone="$ZONE" --project="$PROJECT" --tunnel-through-iap \
    "$CLIENT:$rprof" "$localdir/raw/" >/dev/null 2>&1 \
    || log "WARN: raw profile pull failed for single run$idx"
  log "single: run=$idx done -> $localdir"
}

# ----------------------------------------------------------- single-verdict -----
# Print single-node GrainFS vs MinIO throughput + pprof inspect commands.
cmd_single_verdict() {
  log "single-node comparison: $RESULT_DIR"
  echo ""
  echo "=== Single-node GrainFS (devel, pprof) vs MinIO comparison ==="
  echo ""
  echo "--- GrainFS single node (devel, EC 4+2, XAES-256-GCM) ---"
  cat "$RESULT_DIR"/single/run*/warp-results.tsv 2>/dev/null || echo "  (no results)"
  echo ""
  echo "--- MinIO (single node, SSE-S3 auto-encryption) ---"
  cat "$RESULT_DIR"/minio/run*/warp-results.tsv 2>/dev/null || echo "  (no results)"
  echo ""
  local cpu_prof
  cpu_prof="$(find "$RESULT_DIR/single" -name 'cpu.pb.gz' -path '*/pprof/*' 2>/dev/null | sort -r | head -1)"
  if [ -n "$cpu_prof" ]; then
    local pdir; pdir="$(dirname "$cpu_prof")"
    echo "--- Profiling (GrainFS single node) ---"
    echo "CPU:    go tool pprof -http=:8080 $cpu_prof"
    echo "Heap:   go tool pprof -http=:8080 $pdir/heap.pb.gz"
    echo "Allocs: go tool pprof -http=:8080 $pdir/allocs.pb.gz"
    echo "Top:    go tool pprof -top $cpu_prof"
  fi
}

# ----------------------------------------------------------- down --------------
cmd_down() {
  log "TEARDOWN: deleting all VMs"
  local names=("$CLIENT")
  for i in $(seq 0 $((NODE_COUNT - 1))); do names+=("$(node_name "$i")"); done
  gcloud compute instances delete "${names[@]}" --zone="$ZONE" --project="$PROJECT" --quiet >&2 || true
  log "down done"
}

case "${1:-}" in
  up) cmd_up ;;
  prep-client) cmd_prep_client ;;
  build) cmd_build ;;
  grainfs-cluster) cmd_grainfs_cluster "${2:-1}" ;;
  minio) cmd_minio "${2:-1}" ;;
  minio-cluster) cmd_minio_cluster "${2:-1}" ;;
  minio-verdict) cmd_minio_verdict ;;
  cluster-minio-verdict) cmd_cluster_minio_verdict ;;
  single) cmd_single "${2:-1}" ;;
  single-verdict) cmd_single_verdict ;;
  down) cmd_down ;;
  ip) for i in $(seq 0 $((NODE_COUNT-1))); do echo "$(node_name "$i") $(internal_ip "$(node_name "$i")")"; done ;;
  *) echo "usage: $0 {up|prep-client|build|grainfs-cluster [i]|minio [i]|minio-cluster [i]|minio-verdict|cluster-minio-verdict|single [i]|single-verdict|down|ip}" >&2; exit 2 ;;
esac
