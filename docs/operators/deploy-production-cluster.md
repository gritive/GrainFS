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
install -d -m 0700 "$DATA_DIR"
./grainfs serve --data "$DATA_DIR" --port 9000
```

- `<data>/keys/0.key` (mode 0600) is auto-generated on first start — this is the active KEK file in the versioned keystore
- `<data>/cluster.id` (16-byte UUID v7) is generated at first-cluster boot and binds the cluster identity into the join handshake
- `<data>/keys.d/current.key` (the cluster transport PSK) is self-generated and persisted at genesis — no flag, no operator-supplied key
- `<data>/keys.d/raft-store.key.enc` is generated on each node and seals that node's local raft v2 Badger encryption key under the cluster KEK
- A fixed/externally-managed transport key is supplied by writing `<data>/keys.d/current.key` before first boot (a file, never an argv literal)
- `default` bucket is auto-created with anonymous read/write access
- `_grainfs` reserved bucket seeded

New joining nodes do not hand-copy `<data>/keys/0.key`, `<data>/cluster.id`, or
the cluster transport key. They receive the required sealed bootstrap material
through the Zero-CA invite flow below.

Verify (TTHW ~30s):

```bash
S3_ENDPOINT=http://node-a:9000
aws --no-sign-request --endpoint-url "$S3_ENDPOINT" s3 cp file.txt s3://default/
```

`--no-sign-request` is intentional here: the first-node bootstrap keeps the
`default` bucket anonymous until you install an explicit bucket policy for
`default`. The flag prevents local AWS credentials from hiding whether
anonymous access works.

## Add cluster peers

Joining is Zero-CA invite join: the leader mints a single-use invite bundle and
the new node boots `grainfs serve` with it, pulling the cluster KEK,
`cluster.id`, and transport key over the dedicated join listener — nothing is
pre-copied. There is no separate join command; the offline `grainfs cluster
join` and runtime `grainfs join` verbs have both been retired (the invite
bundle is a secret and must stay out of argv).

### Zero-CA invite join for a new node

Pin a stable join listener on the leader:

```bash
grainfs serve \
  --data /var/lib/grainfs-a \
  --node-id node-a \
  --raft-addr node-a:7001 \
  --join-listen-addr node-a:7443
```

The genesis leader self-seeds its cluster key (no operator-supplied key); see
[`zero-ca-cluster-join.md`](zero-ca-cluster-join.md) for the self-seed semantics
and the `grainfs_cluster_self_seeded` alerting caution.

Mint a one-time invite:

```bash
grainfs cluster invite create \
  --endpoint /var/lib/grainfs-a/admin.sock \
  --ttl 1h
```

Start the joining node with the printed bundle and no pre-staged cluster
secrets:

```bash
GRAINFS_INVITE_BUNDLE='<bundle-token>' grainfs serve \
  --data /var/lib/grainfs-b \
  --node-id node-b \
  --raft-addr node-b:7001 \
  --port 9001
```

Verify membership from the leader:

```bash
grainfs cluster --endpoint /var/lib/grainfs-a/admin.sock peers
```

The joiner should have `keys.d/node.key.enc` and should not have
`encryption.key`. The full operator procedure, including cutover, post-cutover
join behavior, and revocation, is in
[`zero-ca-cluster-join.md`](zero-ca-cluster-join.md).

### Replacing a failed node

To replace a node that died, treat the replacement as a brand-new peer: mint a
fresh invite on the leader (above) and boot the replacement with the bundle.
The dead node is then revoked/evicted from membership (see
[`zero-ca-cluster-join.md`](zero-ca-cluster-join.md)). You do not hand-copy
cluster secrets and there is no runtime join command.

### Backup and restore of an existing node

A node's on-disk identity is its own keystore, not something copied from a peer.
To restore an existing node from backup, restore *that node's own* files
together: `keys/`, `cluster.id`, `keys.d/current.key`,
`keys.d/raft-store.key.enc`, `raft/`, and `meta_raft/`. In particular
`keys.d/raft-store.key.enc` is node-local (generated when the node first opens
its raft v2 stores) — never copy it from another node.

## Require identity and S3 auth

```bash
export GRAINFS_ADMIN_SOCKET=/var/lib/grainfs/admin.sock
grainfs iam sa create admin
grainfs iam policy attach readwrite --sa <id> --i-know
grainfs iam bucket create analytics --attach-sa <id> --attach-policy readwrite
```

Side effects of the first `iam sa create`:
- The first service account and key are created.
- Anonymous requests to `s3://default` still succeed until an explicit bucket
  policy overrides the implicit default policy.
- Anonymous requests to other buckets require an explicit anonymous bucket
  policy.

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

TLS is strongly recommended for any network-exposed authenticated deployment.

### Operator-managed KEK source

> **Phase A:** The legacy `GRAINFS_KEK_SOURCE` env var is no longer supported and
> boot will refuse if it is set. The keystore is always at `<data>/keys/0.key`.
> For test environments only, `GRAINFS_KEK_DIR` overrides the directory.

By default, the first node auto-generates `<data>/keys/0.key`. New peers receive
the cluster KEK through Zero-CA invite join; do not stage the KEK with `scp` for
a fresh joining node. Do not replace the KEK after data exists unless you are
restoring the same 32-byte cluster KEK for that node. GrainFS also keeps each
node's raft-store sidecar sealed under the active KEK and rewraps it after KEK
rotation.

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
