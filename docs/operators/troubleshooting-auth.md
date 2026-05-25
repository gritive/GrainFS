# Troubleshooting Auth

## "401 InvalidAccessKeyId" on signed S3 request

Likely causes:

1. SA was deleted (or never created). Check: `grainfs iam sa list`.
2. Access key revoked. Check: `grainfs iam sa key list --sa <id>`.
3. Clock skew > 5 minutes. Check: `date -u` on client and server; sync with NTP.
4. Wrong `signing region` in client; GrainFS accepts any region but the SDK signs based on the region you configured. SDKs use this only to compute the canonical signature.

Audit lookup:

```bash
grainfs audit by-request-id <X-GrainFS-Request-Id from response>
```

## "403 AccessDenied" on signed request

The SA was authenticated but the policy denied. Use `grainfs iam policy simulate` to debug:

```bash
grainfs iam policy simulate --sa <sa_id> --action s3:PutObject --resource arn:aws:s3:::wh/key
```

Output names the matched/non-matched Statement and the final decision.

Common gotchas:
- `bucket-admin` does NOT grant `s3:CreateBucket` / `s3:DeleteBucket` / `Put|DeleteBucketPolicy`. Those are admin-UDS-only by design (D#8).
- A `Resource:*` policy with an explicit Deny on one bucket: Deny wins.
- Bucket policy `Principal:"*"` is ignored unless `iam.allow-anonymous-bucket-policy=true`.

## "401" on Iceberg client request

1. `iam.anon-enabled=false` and no bearer present → 401 by middleware. Mint a token:
   ```bash
   curl -X POST http://host:9000/iceberg/v1/oauth/tokens \
     -d grant_type=client_credentials \
     -d client_id=<AK> -d client_secret=<SK> \
     -d scope=PRINCIPAL_ROLE:<warehouse>
   ```
2. Bearer signature mismatch → likely SA's `kid` was pruned. Re-mint after `jwt.signing-key-prune`.
3. Expired token → re-mint (3600s TTL).

JWT key state:

```bash
grainfs status --json | jq .jwt_keys
```

## "warehouse claim mismatch" 403

The bearer's `warehouse` claim ≠ the warehouse you targeted via `?warehouse=` or path. One token = one warehouse. Mint a fresh token with the right `scope`.

## "STARTUP REFUSED — auth required + no TLS cert + no trusted proxy"

`iam.anon-enabled=false` was applied without satisfying the TLS posture. Choose one:

```bash
# Option A: place cert on every node, then SIGHUP
cp /etc/grainfs/cert.pem <data>/tls/cert.pem
cp /etc/grainfs/key.pem  <data>/tls/key.pem
kill -SIGHUP $(pidof grainfs)

# Option B: env override
GRAINFS_TLS_CERT=/etc/grainfs/cert.pem GRAINFS_TLS_KEY=/etc/grainfs/key.pem grainfs serve …

# Option C: reverse proxy
grainfs config set trusted-proxy.cidr 10.0.0.0/8
```

## "KEK not found at <data>/kek.key"

The node restarted without its KEK file. The KEK is needed to unwrap the FSM-stored DEK. Options:

```bash
# Option A (recommended): scp KEK from any healthy peer
scp nodeA:<data>/kek.key <data>/kek.key
chmod 0600 <data>/kek.key

# Option B: decommission and rejoin before starting the replacement node
rm -rf <data>
mkdir -p <data>
scp nodeA:<data>/kek.key <data>/kek.key
chmod 0600 <data>/kek.key
grainfs cluster join <healthy-peer>:7001 \
  --data <data> \
  --node-id <replacement-node-id> \
  --bind-addr <replacement-node>:7001 \
  --cluster-key "$CLUSTER_KEY"
grainfs serve \
  --data <data> \
  --node-id <replacement-node-id> \
  --raft-addr <replacement-node>:7001 \
  --cluster-key "$CLUSTER_KEY"
```

`grainfs cluster join` is the offline bootstrap path for a not-yet-running
node. If the node is already running and has an admin socket, use
`grainfs join <healthy-peer>:7001 --endpoint <data>/admin.sock` instead.

## "KEK does not decrypt FSM DEK"

A KEK was placed at the right path but doesn't match the one that wrapped the FSM DEK — usually a stale backup or a KEK from a different cluster. Replace with the right KEK (see "KEK not found" above for `scp` path).

## Cluster join refused with 403

```
WARN: KEK handshake HMAC mismatch from <addr>
```

The joiner has the wrong KEK. Verify the file matches a healthy node's `kek.key` byte-for-byte (`sha256sum kek.key`). On match, retry `cluster join`. Nonces are single-use and 60s-TTL — a retry forces a fresh challenge automatically.

## Audit table queries return zero rows

1. `audit.deny-only=true` filters allow events at the writer. Toggle off if you need allow rows: `grainfs config set audit.deny-only false`.
2. Audit writer may be stuck. Check `grainfs status --json | jq .audit`.
3. Schema migration hasn't run if you upgraded from a pre-redesign snapshot — the snapshot loader fails-loud (see CHANGELOG re: BREAKING).
