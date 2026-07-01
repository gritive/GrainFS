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

## TLS Hardening For Authenticated Traffic

For network-exposed authenticated traffic, choose one:

```bash
# Option A: place cert on every node, then SIGHUP
cp /etc/grainfs/cert.pem <data>/tls/cert.pem
cp /etc/grainfs/key.pem  <data>/tls/key.pem
kill -SIGHUP $(pidof grainfs)

# Option B: env override
GRAINFS_TLS_CERT=/etc/grainfs/cert.pem GRAINFS_TLS_KEY=/etc/grainfs/key.pem grainfs serve …
```

## "KEK not found at <data>/keys/0.key"

The node restarted without its keystore. Phase A stores the active KEK in the
versioned keystore directory at `<data>/keys/0.key` and the cluster identity at
`<data>/cluster.id`. Both are needed: the KEK unwraps the FSM-stored DEK, and
the cluster identity binds this node to its cluster in the join handshake.
Options:

```bash
# Option A (recommended): scp keystore + cluster identity from any healthy peer
mkdir -p <data>/keys
scp nodeA:<data>/keys/0.key  <data>/keys/0.key
scp nodeA:<data>/cluster.id  <data>/cluster.id
chmod 0600 <data>/keys/0.key <data>/cluster.id

# Option B: replace the node entirely via invite-join (no hand-copied secrets)
rm -rf <data>
#   On the leader, mint a fresh single-use invite:
grainfs cluster invite create --endpoint <leader-data>/admin.sock --ttl 1h
#   Boot the replacement with the printed bundle; it pulls the KEK,
#   cluster.id, and transport key over the join listener:
GRAINFS_INVITE_BUNDLE='<bundle-token>' grainfs serve \
  --data <data> \
  --node-id <replacement-node-id> \
  --raft-addr <replacement-node>:7001
```

Option A restores an existing node's own keystore in place. Option B provisions
a fresh replacement via invite-join, which carries the KEK, `cluster.id`, and
transport key in the sealed bundle — nothing is hand-copied. Both the offline
`grainfs cluster join` and the runtime `grainfs join` commands have been
retired; joining is always `grainfs serve` with an invite bundle.

## "KEK does not decrypt FSM DEK"

A KEK was placed at the right path but doesn't match the one that wrapped the FSM DEK — usually a stale backup or a KEK from a different cluster. Replace with the right KEK (see "KEK not found" above for `scp` path) and confirm `<data>/cluster.id` matches the destination cluster.

## Invite-join refused

A `grainfs serve` started with `GRAINFS_INVITE_BUNDLE` fails Phase-1 and the
node exits. Common causes:

- **Bundle is from a different cluster** — the invite transcript binds the
  cluster identity, so a bundle minted on cluster A cannot redeem against
  cluster B.
- **Channel-binding mismatch** — the invite is bound to the leader's
  join-listener TLS session (RFC 5705 exporter); a relayed or proxied dial
  fails verification.
- **Expired or already-redeemed bundle** — invites are single-use and TTL-bound
  (`--ttl` on `cluster invite create`, default 1h).

The fix is always to mint a fresh bundle on the current leader
(`grainfs cluster invite create --endpoint <leader-data>/admin.sock`) and start
the joiner with it. There is no KEK to hand-copy — the bundle carries the sealed
KEK, transport key, and `cluster.id`.

## Audit table queries return zero rows

1. `audit.deny-only=true` filters allow events at the writer. Toggle off if you need allow rows: `grainfs config set audit.deny-only false`.
2. Audit writer may be stuck. Check `grainfs status --json | jq .audit`.
3. Schema migration hasn't run if you upgraded from a pre-redesign snapshot — the snapshot loader fails-loud (see CHANGELOG re: BREAKING).
