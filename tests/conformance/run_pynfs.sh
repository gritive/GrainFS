#!/usr/bin/env bash
# Run pynfs against a local GrainFS NFSv4.1 export.

set -euo pipefail

COLIMA=0
SUITE="basic"

usage() {
  echo "Usage: $0 [--colima] [--suite basic|all]" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --colima)
      COLIMA=1
      shift
      ;;
    --suite)
      if [[ $# -lt 2 ]]; then
        usage
        exit 2
      fi
      SUITE="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown arg: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ "${SUITE}" != "basic" && "${SUITE}" != "all" ]]; then
  echo "ERROR: --suite must be basic or all" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_REL="tests/conformance/run_pynfs.sh"
SCRIPT_DIR="${ROOT}/tests/conformance"

if [[ "${COLIMA}" -eq 1 ]]; then
  if ! command -v colima >/dev/null 2>&1; then
    echo "ERROR: colima not found" >&2
    exit 1
  fi
  ROOT_Q="$(printf '%q' "${ROOT}")"
  SUITE_Q="$(printf '%q' "${SUITE}")"
  exec colima ssh -- bash -lc "cd ${ROOT_Q} && make build && ${SCRIPT_REL} --suite ${SUITE_Q}"
fi

if ! python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 9) else 1)' >/dev/null 2>&1; then
  echo "ERROR: python3 >= 3.9 required" >&2
  exit 1
fi

for tool in git nc; do
  if ! command -v "${tool}" >/dev/null 2>&1; then
    echo "ERROR: ${tool} required" >&2
    exit 1
  fi
done

if [[ ! -x "${ROOT}/bin/grainfs" ]]; then
  echo "ERROR: bin/grainfs missing; run make build first" >&2
  exit 1
fi

PYNFS_SHA="$(tr -d '[:space:]' < "${SCRIPT_DIR}/PYNFS_SHA")"
if [[ -z "${PYNFS_SHA}" ]]; then
  echo "ERROR: tests/conformance/PYNFS_SHA is empty" >&2
  exit 1
fi

PYNFS_DIR="${SCRIPT_DIR}/.pynfs"
RESULTS_DIR="${SCRIPT_DIR}/results"
SUMMARY="${RESULTS_DIR}/summary.json"
mkdir -p "${RESULTS_DIR}"

PYNFS_HTTPS="https://git.linux-nfs.org/cgit/bfields/pynfs.git"
PYNFS_GIT="git://linux-nfs.org/~bfields/pynfs.git"

if [[ ! -d "${PYNFS_DIR}/.git" ]]; then
  git clone "${PYNFS_HTTPS}" "${PYNFS_DIR}" 2>/dev/null || git clone "${PYNFS_GIT}" "${PYNFS_DIR}"
fi
(cd "${PYNFS_DIR}" && git fetch --quiet origin && git checkout --quiet "${PYNFS_SHA}")

DATA_DIR="$(mktemp -d -t grainfs-pynfs-data.XXXXXX)"
ADMIN_SOCK="${DATA_DIR}/admin.sock"
BUCKET="conformance-$(date +%s)"
S3_PORT="${GRAINFS_S3_PORT:-19000}"
NFS_PORT="${GRAINFS_NFS4_PORT:-12049}"
GRAINFS_PID=""

cleanup() {
  if [[ -n "${GRAINFS_PID}" ]]; then
    kill "${GRAINFS_PID}" 2>/dev/null || true
    wait "${GRAINFS_PID}" 2>/dev/null || true
  fi
  rm -rf "${DATA_DIR}"
}
trap cleanup EXIT

"${ROOT}/bin/grainfs" serve \
  --data "${DATA_DIR}" \
  --port "${S3_PORT}" \
  --admin-socket "${ADMIN_SOCK}" \
  --nfs4-port "${NFS_PORT}" \
  --nbd-port 0 >/tmp/grainfs-pynfs-server.log 2>&1 &
GRAINFS_PID=$!

for _ in $(seq 1 30); do
  if [[ -S "${ADMIN_SOCK}" ]] && nc -z 127.0.0.1 "${NFS_PORT}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if [[ ! -S "${ADMIN_SOCK}" ]] || ! nc -z 127.0.0.1 "${NFS_PORT}" >/dev/null 2>&1; then
  echo "ERROR: GrainFS did not become ready; see /tmp/grainfs-pynfs-server.log" >&2
  exit 1
fi

GRAINFS_ADMIN_SOCKET="${ADMIN_SOCK}" "${ROOT}/bin/grainfs" bucket create "${BUCKET}" >/dev/null
GRAINFS_ADMIN_SOCKET="${ADMIN_SOCK}" "${ROOT}/bin/grainfs" nfs export add "${BUCKET}" --quiet >/dev/null

LOG="${RESULTS_DIR}/pynfs-$(date +%Y%m%d-%H%M%S).log"
cd "${PYNFS_DIR}/nfs4.1"

set +e
./testserver.py --port "${NFS_PORT}" --maketree "127.0.0.1:/${BUCKET}" "${SUITE}" 2>&1 | tee "${LOG}"
PYNFS_EXIT=${PIPESTATUS[0]}
set -e

PASS=$(grep -cE '(^|[[:space:]])PASS([[:space:]]|$)|: PASS$' "${LOG}" || true)
FAIL=$(grep -cE '(^|[[:space:]])FAIL([[:space:]]|$)|: FAIL$' "${LOG}" || true)
SKIP=$(grep -cE '(^|[[:space:]])SKIP([[:space:]]|$)|: SKIP$' "${LOG}" || true)

cat > "${SUMMARY}" <<EOF
{"ts":"$(date -u +%Y-%m-%dT%H:%M:%SZ)","suite":"${SUITE}","sha":"${PYNFS_SHA}","exit":${PYNFS_EXIT},"pass":${PASS},"fail":${FAIL},"skip":${SKIP},"log":"${LOG}"}
EOF

echo "Summary: pass=${PASS} fail=${FAIL} skip=${SKIP}; log=${LOG}; summary=${SUMMARY}"
exit "${PYNFS_EXIT}"
