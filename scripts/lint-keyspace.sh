#!/bin/bash
# lint-keyspace.sh — detect raw FSM-state key literals bypassing stateKeyspace
#
# OK (safe — keyspace prepends the group prefix internally):
#   ks.Prefix([]byte("obj:...")), ks.scanGroupPrefix(txn, []byte("obj:"), fn)
#   ks.Key([]byte("bucket:...")), rawFoo := []byte("lat:...")  # variable, used later via ks.Prefix(rawFoo)
#
# NOT OK (bypasses keyspace → cross-group data leak in shared mode):
#   txn.Get([]byte("obj:..."))
#   txn.Set([]byte("bucket:..."), v)
#   txn.Delete([]byte("lat:..."))
#   it.Seek([]byte("mpu:..."))
#   it.ValidForPrefix([]byte("placement:..."))
#   opts.Prefix = []byte("bucketver:...")
#
# FSM-state key prefixes: bucket: obj: lat: mpu: placement: policy: bucketver:
#                         pending-migration: quarantine:
# Excluded: lifecycle: (process-global, lives outside internal/cluster)
#           raft:      (raft log store, separate DB)

set -euo pipefail

CLUSTER_DIR="internal/cluster"
KEY_PREFIXES="(bucket:|obj:|lat:|mpu:|placement:|policy:|bucketver:|pending-migration:|quarantine:)"

# Pattern A: []byte("prefix...") directly inside a Badger txn/iterator call on the SAME line.
#   Matches: txn.Get([]byte("obj:...")), txn.Set([]byte("bucket:...")), txn.Delete(...)
#            it.Seek([]byte("...")), it.ValidForPrefix([]byte("..."))
# The key: the []byte literal appears after an opening paren that belongs to the op,
# NOT after a keyspace method (.Prefix, .Key, .scanGroupPrefix etc.).
CALL_REGEX="(txn\.(Get|Set|Delete)|\.Seek|ValidForPrefix)\([^)]*\[\]byte\(\"${KEY_PREFIXES}"

# Pattern B: assignment form: opts.Prefix = []byte("prefix:...")
#   Matches: opts.Prefix = []byte("lat:..."), iterOpts.Prefix = []byte("obj:...")
ASSIGN_REGEX="\.Prefix\s*=\s*\[\]byte\(\"${KEY_PREFIXES}"

# Run both checks; capture output without failing immediately on grep's exit-1-if-no-match
matches_call=$(grep -rEn "${CALL_REGEX}" \
    "${CLUSTER_DIR}" \
    --include='*.go' \
    --exclude='*_test.go' \
    2>/dev/null | grep -v "/${CLUSTER_DIR##*/}/keyspace\.go:" || true)

matches_assign=$(grep -rEn "${ASSIGN_REGEX}" \
    "${CLUSTER_DIR}" \
    --include='*.go' \
    --exclude='*_test.go' \
    2>/dev/null | grep -v "/${CLUSTER_DIR##*/}/keyspace\.go:" || true)

all_matches=$(printf '%s\n%s\n' "${matches_call}" "${matches_assign}" | sed '/^$/d')

if [ -z "${all_matches}" ]; then
    echo "lint-keyspace: OK (no raw FSM-state key literals in Badger ops)"
    exit 0
fi

echo "lint-keyspace: FAIL — raw FSM-state key prefix(es) found bypassing stateKeyspace:" >&2
echo "" >&2
echo "${all_matches}" >&2
echo "" >&2
cat >&2 <<'EOF'
Each flagged line passes a []byte("bucket:|obj:|lat:|mpu:|placement:|policy:|bucketver:|pending-migration:|quarantine:...")
literal directly to a Badger txn/iterator op. This bypasses the group prefix and causes
cross-group data leaks in shared-DB mode.

Fix: route every FSM-state key through stateKeyspace:
  Point ops:   txn.Get(ks.BucketKey(b))  /  txn.Set(ks.ObjectMetaKey(b, k), v)
  Prefix ops:  ks.Prefix([]byte("obj:" + bucket + "/"))  then pass the result to Seek/ValidForPrefix
  Scans:       ks.scanGroupPrefix(txn, []byte("bucket:"), fn)

See docs/architecture/badger-consolidation.md for the full pattern guide.
EOF
exit 1
