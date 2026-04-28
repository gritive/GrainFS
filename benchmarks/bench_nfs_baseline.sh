#!/usr/bin/env bash
# bench_nfs_baseline.sh — Linux kernel nfsd + nfs-ganesha + (옵션) grainfs를
# 같은 VM의 ext4에서 띄워 fio sweep으로 비교.
#
# Colima VM 안에서 실행. macOS host에서:
#   colima ssh -- bash /Users/whitekid/work/gritive/grains/benchmarks/bench_nfs_baseline.sh
# 또는 직접 colima ssh 후 실행.
#
# 환경 변수:
#   SERVERS  — "kernel ganesha grainfs-single grainfs-cluster" (default 첫 둘)
#   PROTOS   — "v3 v4"  (default "v3")
#   JOBS_DIR — fio job 파일 위치 (default benchmarks/fio_jobs)
#   OUT_DIR  — 결과 디렉토리
#
# 실행 전제: nfs-kernel-server, nfs-ganesha-vfs, fio, rpcbind 설치.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVERS="${SERVERS:-kernel ganesha}"
PROTOS="${PROTOS:-v3}"
JOBS_DIR="${JOBS_DIR:-$SCRIPT_DIR/fio_jobs}"
TS=$(date +%Y%m%d-%H%M%S)
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/results-nfs-$TS}"

DATA_ROOT="/srv/nfsbench"
MNT_ROOT="/mnt/nfsbench"

mkdir -p "$OUT_DIR"
echo "[bench] results: $OUT_DIR"

# ── helper: cleanup ──────────────────────────────────────────────────────────
cleanup() {
  echo "[bench] cleanup..."
  for p in v3 v4; do
    sudo umount -f "$MNT_ROOT/$p" 2>/dev/null || true
  done
  sudo systemctl stop nfs-server 2>/dev/null || true
  sudo systemctl stop nfs-ganesha 2>/dev/null || true
  sudo exportfs -ua 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ── helper: start kernel nfsd ────────────────────────────────────────────────
start_kernel_nfsd() {
  local data_dir="$1"
  sudo mkdir -p "$data_dir"
  sudo chmod 777 "$data_dir"
  echo "$data_dir *(rw,no_subtree_check,no_root_squash,sync)" | sudo tee /etc/exports >/dev/null
  sudo systemctl restart nfs-server
  sudo exportfs -ra
  sleep 1
}

# ── helper: start ganesha ────────────────────────────────────────────────────
start_ganesha() {
  local data_dir="$1"
  sudo mkdir -p "$data_dir"
  sudo chmod 777 "$data_dir"
  sudo tee /etc/ganesha/ganesha.conf >/dev/null <<EOF
NFS_CORE_PARAM {
    Protocols = 3, 4;
    NFS_Port = 2049;
}
EXPORT {
    Export_Id = 1;
    Path = $data_dir;
    Pseudo = $data_dir;
    Access_Type = RW;
    Squash = No_Root_Squash;
    FSAL { Name = VFS; }
    Filesystem_Id = 100.1;
    SecType = sys;
    Disable_ACL = TRUE;
}
EOF
  sudo systemctl restart nfs-ganesha
  sleep 2
}

# ── helper: mount + fio sweep + unmount ──────────────────────────────────────
sweep_one() {
  local server="$1" proto="$2" export_path="$3" port="${4:-2049}"
  local mnt="$MNT_ROOT/$proto"
  sudo mkdir -p "$mnt"

  # noacl: go-nfs is the grainfs NFS server library and does not implement
  # NFSACL (RPC program 100227). nolock: no NLM. intr is deprecated post 2.6.25
  # so we drop it.
  local mount_opts="vers=${proto#v},rsize=1048576,wsize=1048576,hard,nolock,noacl"
  if [[ "$port" != "2049" ]]; then
    mount_opts="$mount_opts,port=$port,mountport=$port"
  fi

  echo ""
  echo "════════════════════════════════════════════════════════════════════"
  echo "  $server / NFS$proto / $export_path"
  echo "════════════════════════════════════════════════════════════════════"

  if ! sudo mount -t nfs -o "$mount_opts" "127.0.0.1:$export_path" "$mnt"; then
    echo "[error] mount failed"
    return 1
  fi
  sudo chmod 777 "$mnt"

  for job in "$JOBS_DIR"/*.fio; do
    [[ -f "$job" ]] || continue
    local name; name=$(basename "$job" .fio)
    local report="$OUT_DIR/${server}-${proto}-${name}.json"
    echo "  -- $name"
    sudo fio "$job" \
      --directory="$mnt" \
      --output-format=json \
      --output="$report" \
      >/dev/null 2>&1 || echo "  (job failed: $name)"
    # 작은 정리 — 다음 job용
    sudo rm -rf "$mnt"/{seq-*,rand-*,metadata-*}.* 2>/dev/null || true
  done

  sudo umount -f "$mnt"
}

# ── 실행 ─────────────────────────────────────────────────────────────────────
sudo rm -rf "$DATA_ROOT" "$MNT_ROOT"
sudo mkdir -p "$DATA_ROOT" "$MNT_ROOT"

for sys in $SERVERS; do
  case "$sys" in
    kernel)
      cleanup
      start_kernel_nfsd "$DATA_ROOT/kernel"
      for p in $PROTOS; do
        sweep_one kernel "$p" "$DATA_ROOT/kernel"
      done
      sudo systemctl stop nfs-server
      ;;
    ganesha)
      cleanup
      start_ganesha "$DATA_ROOT/ganesha"
      for p in $PROTOS; do
        sweep_one ganesha "$p" "$DATA_ROOT/ganesha"
      done
      sudo systemctl stop nfs-ganesha
      ;;
    grainfs-single)
      cleanup
      data_dir="$DATA_ROOT/grainfs-single"
      sudo rm -rf "$data_dir"; sudo mkdir -p "$data_dir"
      gbin="${GRAINFS_BIN:-/Users/whitekid/work/gritive/grains/bin/grainfs-linux-arm64}"
      sudo "$gbin" serve --data "$data_dir" \
        --port 9100 --nfs-port 9102 --nfs4-port 0 --nbd-port 0 \
        --no-encryption \
        --rate-limit-ip-rps 1000000 --rate-limit-ip-burst 1000000 \
        --rate-limit-user-rps 1000000 --rate-limit-user-burst 1000000 \
        >/tmp/grainfs-nfs-single.log 2>&1 &
      gpid=$!
      sleep 3
      for p in $PROTOS; do
        sweep_one grainfs-single "$p" "/default" 9102
      done
      sudo kill "$gpid" 2>/dev/null || true
      sudo pkill -9 -f grainfs-linux-arm64 2>/dev/null || true
      sleep 1
      ;;
    grainfs-cluster)
      echo "[bench] grainfs-cluster — TODO (binary lifecycle 4-node 셋업)"
      ;;
    *)
      echo "[warn] unknown server: $sys"
      ;;
  esac
done

# ── summary ──────────────────────────────────────────────────────────────────
echo ""
echo "[bench] generating SUMMARY.md..."
SUMMARY="$OUT_DIR/SUMMARY.md"
{
  echo "# NFS baseline 측정 결과"
  echo ""
  echo "- **시각:** $(date)"
  echo "- **환경:** $(uname -srm), Ubuntu $(lsb_release -rs 2>/dev/null || echo unknown)"
  echo "- **fio:** $(fio --version)"
  echo "- **데이터:** ext4 native ($DATA_ROOT, $(df -T $DATA_ROOT | awk 'NR==2{print $2,$NF}'))"
  echo ""
  echo "| server | proto | workload | iops | bw (MB/s) | lat p50 (us) | lat p99 (us) |"
  echo "|--------|-------|----------|------|-----------|--------------|--------------|"
  for f in "$OUT_DIR"/*.json; do
    [[ -f "$f" ]] || continue
    local_name=$(basename "$f" .json)
    server=$(echo "$local_name" | cut -d- -f1)
    proto=$(echo "$local_name" | cut -d- -f2)
    workload=$(echo "$local_name" | cut -d- -f3-)
    job=$(jq -r '.jobs[0]' "$f" 2>/dev/null)
    if [[ -z "$job" || "$job" == "null" ]]; then continue; fi
    rw=$(jq -r '.jobs[0]["job options"].rw // .jobs[0].job_options.rw // "unknown"' "$f")
    if [[ "$rw" == read* || "$rw" == randread ]]; then op="read"; else op="write"; fi
    iops=$(jq -r ".jobs[0].$op.iops // 0 | floor" "$f")
    bw_kbs=$(jq -r ".jobs[0].$op.bw // 0" "$f")
    bw_mbs=$(awk -v k="$bw_kbs" 'BEGIN{ printf "%.1f", k/1024 }')
    p50=$(jq -r ".jobs[0].$op.clat_ns.percentile[\"50.000000\"] // 0" "$f")
    p99=$(jq -r ".jobs[0].$op.clat_ns.percentile[\"99.000000\"] // 0" "$f")
    p50_us=$(awk -v n="$p50" 'BEGIN{ printf "%.1f", n/1000 }')
    p99_us=$(awk -v n="$p99" 'BEGIN{ printf "%.1f", n/1000 }')
    echo "| $server | $proto | $workload | $iops | $bw_mbs | $p50_us | $p99_us |"
  done
} > "$SUMMARY"

echo "Done. Summary: $SUMMARY"
