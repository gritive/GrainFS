# Cluster Lifecycle — Phase 0 → 3

GrainFS uses **progressive application**: the binary is the same across phases; FSM state determines what's enforced.

## Phase 0 — single node, anonymous

```bash
./grainfs serve --data ./tmp --port 9000
```

- `<data>/kek.key` (mode 0600) is auto-generated on first start
- `default` bucket is auto-created; anon read/write
- `_grainfs` reserved bucket + `_grainfs/audit/evaluations` Iceberg table seeded

Verify (TTHW ~30s):

```bash
aws --no-sign-request --endpoint-url http://localhost:9000 s3 cp file.txt s3://default/
```

## Phase 1 — cluster

On each new node:

```bash
scp nodeA:/path/to/dataA/kek.key /path/to/dataB/kek.key
./grainfs serve --data ./dataB --port 9000 &
./grainfs cluster join nodeA:9000 --endpoint ./dataB/admin.sock
```

The KEK is the cluster's shared identity secret. The join handshake (HMAC-SHA256 challenge-response on a 32B nonce) verifies KEK possession. Nodes with a different KEK are refused with 403.

## Phase 2 — identity + auth

```bash
grainfs iam sa create admin
grainfs iam policy attach --sa <id> --policy readwrite --i-know
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

## Phase 3 — production hardening

Each block independent.

### TLS

Convention path: `<data>/tls/cert.pem` + `<data>/tls/key.pem` (or `GRAINFS_TLS_CERT/KEY` env). After placing files: `kill -SIGHUP $(pidof grainfs)` for hot-swap.

If `iam.anon-enabled=false` and no TLS cert and no `trusted-proxy.cidr`, startup refuses with the three-option message.

### Reverse-proxy mode

```bash
grainfs config set trusted-proxy.cidr 10.0.0.0/8,172.16.0.0/12
```

Accepts plaintext token endpoint behind a validated proxy (`Forwarded` or `X-Forwarded-Proto: https`).

### Operator-owned KEK

```bash
GRAINFS_KEK_SOURCE=file:///etc/grainfs/kek.key ./grainfs serve --data ./tmp ...
```

KEK replacement = file-replace on every node. DEK unwrap re-runs on next read/restart.

### DEK rotation

```bash
grainfs config set encryption.rotate-dek now
```

Background scrubber re-encrypts existing objects with the new DEK. Foreground reads use the old DEK (record's `dek_gen` field). When complete:

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
