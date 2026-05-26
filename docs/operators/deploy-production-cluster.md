# Deploy a Production Cluster

GrainFS uses one binary for local development and production clusters. This
guide assumes operator-owned data directories on each node and shows the
choices that turn a fresh deployment into a production cluster: peer
membership, S3 authentication, TLS/proxy posture, key rotation, audit controls,
and maintenance mode.

The usual path is:

1. Start the first production node and verify anonymous `default` bucket access.
2. Add peers if this deployment needs cluster durability.
3. Create the first service account when S3 clients need credentials.
4. Add TLS, proxy trust, key rotation, audit filters, and maintenance controls
   as production posture requires.

## Bootstrap the first node

```bash
DATA_DIR=/var/lib/grainfs
CLUSTER_KEY=$(openssl rand -hex 32)
install -d -m 0700 "$DATA_DIR"
./grainfs serve --data "$DATA_DIR" --port 9000 --cluster-key "$CLUSTER_KEY"
```

- `<data>/keys/0.key` (mode 0600) is auto-generated on first start — this is the active KEK file in the versioned keystore
- `<data>/cluster.id` (16-byte UUID v7) is generated at first-cluster boot and binds the cluster identity into the join handshake
- `--cluster-key` is required even for the first single-node bootstrap
- `default` bucket is auto-created with anonymous read/write access
- `_grainfs` reserved bucket + `_grainfs/audit/evaluations` Iceberg table seeded

Joining nodes need the same `<data>/keys/0.key` bytes and the same
`<data>/cluster.id` bytes as the first node.

Verify (TTHW ~30s):

```bash
S3_ENDPOINT=http://node-a:9000
aws --no-sign-request --endpoint-url "$S3_ENDPOINT" s3 cp file.txt s3://default/
```

`--no-sign-request` is intentional here: the first-node bootstrap keeps the
`default` bucket anonymous until you create the first service account. The flag
prevents local AWS credentials from hiding whether anonymous access works.

## Add cluster peers

There are two join paths. Use `grainfs cluster join` for a not-yet-running
node during first bootstrap. Use `grainfs join` only when a node is already
running and you want it to restart into the cluster through its admin socket.

### Offline bootstrap for a new node

## Phase A: Keystore + Cluster Identity Staging

Each node has TWO files that must be staged before joining an existing cluster:

1. `<dataDir>/keys/0.key` — the cluster's active Key Encryption Key (KEK). 32 bytes, 0o600.
2. `<dataDir>/cluster.id` — the 16-byte cluster identity (UUID v7) bound into the join handshake.

Both files are generated at first-cluster boot. To add a node to an existing cluster, copy both from a healthy peer:

```bash
DATA_DIR=/var/lib/grainfs
install -d -m 0700 "$DATA_DIR"
install -d -m 0700 "$DATA_DIR/keys"
scp node-a:/var/lib/grainfs/keys/0.key  "$DATA_DIR/keys/0.key"
scp node-a:/var/lib/grainfs/cluster.id  "$DATA_DIR/cluster.id"
chmod 0600 "$DATA_DIR/keys/0.key" "$DATA_DIR/cluster.id"
./grainfs cluster join node-a:7001 \
  --data "$DATA_DIR" \
  --node-id node-b \
  --bind-addr node-b:7001 \
  --cluster-key "$CLUSTER_KEY"
```

After the join succeeds, start the new node with the same data directory,
node ID, Raft address, and cluster key:

```bash
./grainfs serve \
  --data "$DATA_DIR" \
  --node-id node-b \
  --raft-addr node-b:7001 \
  --port 9000 \
  --cluster-key "$CLUSTER_KEY"
```

The join handshake (HMAC-SHA256 challenge-response on a 32B nonce) verifies
KEK possession. Nodes with a different KEK are refused with 403 and would not
be able to decrypt the cluster's wrapped DEKs anyway.

### Runtime join for an already-running node

If the node is already running as a solo node, send the join request through
that node's admin socket instead. Phase A requires that both
`<dataDir>/keys/0.key` and `<dataDir>/cluster.id` be pre-staged before the
runtime join, and the operator must explicitly acknowledge that the staged
keys belong to the destination cluster via `--confirm-staged-keys`:

```bash
grainfs join node-a:7001 \
  --endpoint /var/lib/grainfs/admin.sock \
  --confirm-staged-keys
```

If the node has local user data, the admin API refuses the join unless you
repeat the command with `--force`, which discards the solo data before the
node restarts into the cluster.

## Require identity and S3 auth

```bash
export GRAINFS_ADMIN_SOCKET=/var/lib/grainfs/admin.sock
grainfs iam sa create admin
grainfs iam policy attach readwrite --sa <id> --i-know
grainfs iam bucket create analytics --attach-sa <id> --attach-policy readwrite
```

Side effects of the first `iam sa create`:
- `iam.anon-enabled` → false atomically
- Anonymous requests to `s3://default` still succeed (implicit anon policy)
- Anonymous requests to other buckets → 401

Override default's public access:

```bash
grainfs iam bucket policy put default --file my-policy.json
```

Re-public default (after override or after delete-recreate):

```bash
grainfs iam bucket policy delete default
# OR
grainfs iam bucket delete default && grainfs iam bucket create default
```

## Harden the deployment

Each block independent.

### TLS

Convention path: `<data>/tls/cert.pem` + `<data>/tls/key.pem` (or `GRAINFS_TLS_CERT/KEY` env). After placing files: `kill -SIGHUP $(pidof grainfs)` for hot-swap.

If `iam.anon-enabled=false` and no TLS cert and no `trusted-proxy.cidr`, startup refuses with the three-option message.

### Reverse-proxy mode

```bash
grainfs config set trusted-proxy.cidr 10.0.0.0/8,172.16.0.0/12
```

Accepts plaintext token endpoint behind a validated proxy (`Forwarded` or `X-Forwarded-Proto: https`).

### Operator-managed KEK source

> **Phase A:** The legacy `GRAINFS_KEK_SOURCE` env var is no longer supported and
> boot will refuse if it is set. The keystore is always at `<data>/keys/0.key`.
> For test environments only, `GRAINFS_KEK_DIR` overrides the directory.

By default, the first node auto-generates `<data>/keys/0.key`. The contents
must be identical on every node — stage the file out-of-band (e.g. `scp` from
a healthy peer) before starting a joining node. Do not replace the KEK after
data exists unless you are restoring the same 32-byte cluster KEK; GrainFS
does not currently expose a separate KEK rotation flow.

### DEK rotation

Operators do not provide DEK bytes. `GrainFS` generates each DEK generation
internally, wraps it with the cluster KEK, and persists the wrapped DEK in the
meta-FSM state. The operator control surface is rotation and pruning only:

```bash
grainfs config set encryption.rotate-dek now
```

Rotation generates a new active DEK generation. Background scrubber re-encrypts
existing objects with the new DEK. Foreground reads use the old DEK when an
object record's `dek_gen` still points at the previous generation. When
rewrap completes:

```bash
grainfs config set encryption.prune-dek-version <N>
```

(Refused 409 if any record still references gen N.)

### JWT signing key rotation

```bash
grainfs config set jwt.signing-key-rotate now
# wait for in-flight tokens to expire (≤ 1h)
grainfs config set jwt.signing-key-prune now
```

### Audit (always on)

Query via DuckDB:

```bash
grainfs audit query "SELECT count(*) FROM _grainfs.audit.evaluations WHERE outcome='deny' AND ts > now() - INTERVAL 1 DAY"
```

Convenience presets:

```bash
grainfs audit recent-denies --limit 50
grainfs audit by-sa <sa_id>
grainfs audit by-request-id <rid>
```

Filter to deny-only (cluster-wide):

```bash
grainfs config set audit.deny-only true
```

### Read-only mode (maintenance)

```bash
grainfs config set cluster.read-only true
```

Data-plane writes → 503 with `Retry-After: 60`. Admin UDS, audit writes, JWT rotation, DEK scrubber continue.
