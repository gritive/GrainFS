# How to add cluster peers with Zero-CA invite join

Zero-CA invite join adds a brand-new node without copying the cluster KEK,
`cluster.id`, or the cluster transport key to the node first. The leader mints a
single-use invite bundle. The joining node presents a fresh per-node identity,
pulls the sealed bootstrap secrets over the dedicated join listener, and then
finishes membership through meta-Raft.

Use this path for new nodes. When you cannot use invite join, the alternative is
the runtime `grainfs join` with pre-staged `keys/0.key`, `cluster.id`, and
`keys.d/current.key` — see
[`deploy-production-cluster.md`](deploy-production-cluster.md). The offline
`grainfs cluster join` command has been retired.

## Prerequisites

- A running leader with an admin socket, usually `<leader-data>/admin.sock`.
- A stable `--raft-addr` for the new node. Invite join rejects ephemeral `:0`
  Raft addresses because the cluster must be able to dial the node later.
- Network reachability from the new node to the leader's Zero-CA join listener.
  The listener starts automatically in cluster mode. In production, pin it with
  `--join-listen-addr` so invite bundles do not depend on an ephemeral port.
- An out-of-band path to copy the printed invite bundle to the joining node.

## Start or restart the leader with a stable join listener

```bash
LEADER_DATA=/var/lib/grainfs-a

grainfs serve \
  --data "$LEADER_DATA" \
  --node-id node-a \
  --raft-addr node-a:7001 \
  --join-listen-addr node-a:7443
```

The genesis leader needs **no operator-supplied key**. On a fresh data dir with
no invite bundle and no peers it self-generates and seals its own cluster
transport key into `keys.d/current.key`, so you never generate or hand-carry raw
key material. For a deterministic / externally-managed key, write
`<data>/keys.d/current.key` before first boot — a file, never an argv literal;
on a restart the on-disk key always wins.

> **Caution — a keyless boot on an EMPTY data dir bootstraps a NEW single-node
> cluster.** If you intended to JOIN an existing cluster, set
> `GRAINFS_INVITE_BUNDLE`. A keyless, bundle-less, empty-dir start forks its own
> cluster with a distinct `cluster.id` (it cannot silently merge into the intended
> one). The node logs `self_seeded=true` at startup AND exports the
> `grainfs_cluster_self_seeded` gauge (=1) on `/metrics` — **alert on the gauge**
> for unattended fleets, since the startup log is invisible to automation.

`--join-listen-addr` is the address embedded in newly minted invite bundles. If
it is omitted, GrainFS derives a listener address from `--raft-addr`. That is
acceptable for local testing, but production bundles should contain a stable
host and port.

## Mint a one-time invite

Run the command against the leader's admin socket:

```bash
grainfs cluster invite create \
  --endpoint "$LEADER_DATA/admin.sock" \
  --ttl 1h
```

The output has this shape:

```text
Invite minted: id=<invite-id>
Set this on the joining node as GRAINFS_INVITE_BUNDLE:
<bundle-token>
```

The bundle contains the invite ID, cluster identity, seed join-listener address,
and pinned join-listener SPKI. It is single-use and expires after the TTL. TTLs
longer than 24 hours are rejected.

## Start the joining node

Do not copy `keys/0.key`, `cluster.id`, `keys.d/raft-store.key.enc`, or a
cluster transport key to a fresh Zero-CA joiner.

```bash
JOINER_DATA=/var/lib/grainfs-b

GRAINFS_INVITE_BUNDLE='<bundle-token>' grainfs serve \
  --data "$JOINER_DATA" \
  --node-id node-b \
  --raft-addr node-b:7001 \
  --port 9001
```

During startup the joining node:

1. Generates a per-node transport identity.
2. Dials the leader's Zero-CA join listener and pulls sealed bootstrap secrets.
3. Stages `cluster.id`, KEK generations, and transport material locally.
4. Seals `keys.d/node.key.enc` under the cluster KEK generation.
5. Sends the Phase-2 membership ACK and becomes a meta-Raft voter.

The joiner should not create legacy `<data>/encryption.key`.

## Verify the join

On the leader, confirm the node appears in the voter set:

```bash
grainfs cluster --endpoint "$LEADER_DATA/admin.sock" peers
grainfs cluster --endpoint "$LEADER_DATA/admin.sock" status
```

Check the new node's local key material:

```bash
test -f "$JOINER_DATA/keys.d/node.key.enc"
test ! -f "$JOINER_DATA/encryption.key"
```

For a data-path check, use the joined node's S3 endpoint for a PUT and GET. This
proves the node can serve client traffic and forward cluster operations:

```bash
aws --endpoint-url http://node-b:9001 s3 cp file.txt s3://default/
aws --endpoint-url http://node-b:9001 s3 cp s3://default/file.txt -
```

Use signed AWS requests once the `default` bucket has a non-anonymous policy.

## Complete the Zero-CA cutover

After the cluster is running on per-node identities, drop the shared cluster-key
accept path:

```bash
grainfs cluster --endpoint "$LEADER_DATA/admin.sock" complete-cutover
```

Successful output:

```text
Zero-CA cutover complete: cluster key dropped, connections recycled.
```

New nodes can still join after cutover with the same invite flow. A post-drop
joiner does not stage the revoked shared transport key; it pins its per-node
certificate before opening the normal cluster listener.

## Revoke a node identity

Use `revoke-node` when a node identity should be blocked from future membership,
not only removed from the current voter set:

```bash
grainfs cluster --endpoint "$LEADER_DATA/admin.sock" revoke-node node-b
```

Successful output:

```text
Zero-CA node revoked: node-b
```

The command removes the peer from meta-Raft membership, burns pending invites
for the same node ID, deny-lists the node transport SPKI, and closes cached QUIC
connections to that peer.

### Data-group voter eviction

Revoking a node also evicts it from the per-data-group Raft voter sets it
belonged to. Each surviving data-group leader runs an eviction controller that,
on the next tick (within ~30s) or on the revoke wake, removes the revoked node
from the groups it leads (adding a healthy replacement when one is available,
otherwise shrinking the group). This is eventually-consistent: the node leaves
every group whose quorum allows the change.

Confirm the eviction completed by reading the real Raft voter set per group:

```bash
grainfs cluster --endpoint "$LEADER_DATA/admin.sock" --format json health \
  | jq '.data_groups.groups[] | {group_id, raft_voters, peer_ids}'
```

`raft_voters` is the live committed voter set (resolved to node IDs); a revoked
node should disappear from `raft_voters` across all groups, and from the
`peer_ids` placement mirror shortly after. Aggregate across every node's admin
socket — a group led by a node other than the one you query reports its real
voters only on that leader.

**Limitation — a 2-voter group whose other voter is revoked cannot self-heal.**
If a data group has exactly `[revoked-node, one-survivor]`, the survivor cannot
commit the removal: Raft joint consensus needs a quorum of the old 2-voter
config, which includes the now-unreachable revoked node. The controller logs
`evacuator: eviction failed; retry next tick` for that group and the revoked node
stays in `raft_voters`. Detection: the revoked node persists in `raft_voters` for
a group plus that repeating log line. Remedy: an operator must intervene, or run
`complete-cutover` to drop the cluster key (the hard-security path that excludes
the revoked SPKI from the accept-set regardless of voter membership). Groups with
replication factor 3 or higher evict automatically.

## Troubleshooting

`invite create` fails:
Check that the command targets the leader's admin socket and that the leader has
finished booting. The invite endpoint commits metadata through meta-Raft.

The joiner rejects `--raft-addr :0`:
Use a stable address such as `node-b:7001`. The leader stores this address during
Phase 1 and uses it when finalizing membership.

The joiner cannot dial the seed:
Confirm that the leader's `--join-listen-addr` host and port are reachable from
the joining node. Mint a new invite after correcting the address.

Phase 1 is rejected with `signature invalid`:
The invite handshake is bound to the joiner's TLS session (RFC 5705 channel
binding), so the signed request is only valid on the exact connection it was
created on. This rejection is expected if the join stream is relayed or proxied
through a third party — the joiner must dial the leader's join listener
directly. If you front the leader behind an L7 proxy that terminates TLS, point
`--join-listen-addr`/the seed address at the leader itself instead.

The joiner resumes after a crash:
Restart it with the same data directory before the invite TTL expires. If
Phase-1 artifacts are complete but Phase-2 did not ACK, GrainFS resumes from
`.invite-join-pending` and completes membership without re-running Phase 1.
An incomplete Phase 1 is re-run from scratch (it re-dials and re-signs over the
new session's channel binding); the signed request is never persisted.

`revoke-node` succeeds but the old process is still running:
Stop or isolate the old process. The identity is denied for future membership
and cached connections are closed, but an operator should still remove the
failed or compromised process from service.

## Related

- [Deploy a Production Cluster](deploy-production-cluster.md)
- [Operator runbook](runbook.md)
- [README quick start](../../README.md)
