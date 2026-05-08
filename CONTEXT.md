# Context

## Domain Vocabulary

### Storage Operations Facade

The storage operations facade is the module that upper layers use for meaningful
storage actions instead of probing optional backend capabilities directly. It
owns storage side-effect ordering such as cache invalidation, WAL recording,
recovery write gating, and fallback behavior across decorated backends.

### Storage Decorator Capability Plan

The storage decorator capability plan is the storage operations facade's
internal plan for discovering optional storage capabilities across a decorated
backend stack, then executing them while preserving wrapper side effects and
fallback ordering.

The plan is not a public extension interface and does not widen the primitive
storage backend interface. Its purpose is to keep capability probing, wrapper
ordering, and fallback rules inside the facade so callers do not reach through
decorated backends directly.

For ACL object writes, the plan preserves the ordering contract: cache
invalidation happens before the mutation can be observed, WAL recording happens
only after a successful mutation, atomic ACL put adapters are preferred when
available without bypassing outer mutation side-effect wrappers, put-then-set
fallback rolls back the newly-created version on ACL failure, and recovery
write gates are not used as rollback deleters.

### Mutation Result

A mutation result is the storage-facing outcome of a write operation, including
the new object state and any previous-object facts needed for metrics,
invalidation, or protocol responses. Mutation result methods belong on the
storage operations facade so callers do not perform their own pre-mutation
bookkeeping around decorated backends.

The new object state is exposed as object facts, not as a storage object handle:
size, ETag, version identity, and last-modified time. Result normalization
rejects nil objects, negative sizes, and empty ETags for newly written objects
so handlers do not emit responses or metrics from invalid mutation metadata.

Previous-object facts are a summary, not a leaked historical object. They state
whether a previous object existed and the stable fields needed for accounting,
such as size, ETag, and version identity.

The facade reads previous-object facts immediately before the mutation inside
the same operation boundary. This keeps bookkeeping reads with the storage
side-effect ordering they explain, without claiming stronger atomicity than the
backend provides.

For copy mutations, previous-object facts describe the destination object being
overwritten. Source object state belongs to copy-source validation and metadata
selection, not mutation accounting.

Delete mutations have their own result shape because their protocol meaning is
delete-marker state and version identity, not a newly written object. They still
carry previous-object facts for accounting.

Multipart complete reads previous-object facts before invoking the backend
complete operation. The facade contract does not require backend-specific
hooks between object assembly and publication, though a backend may use such a
hook internally as an optimization.

If reading previous-object facts returns not-found, the mutation proceeds with
`Previous.Exists=false`. Any other previous-object read error fails the
operation before mutation.

### CopyObject Semantics

CopyObject semantics belong to the storage operations facade, not to HTTP
handlers or backend copy primitives. Handlers translate raw HTTP fields such as
`x-amz-copy-source`, metadata directive headers, and copy-source conditional
headers into typed storage requests. The facade owns source validation,
precondition evaluation, delete-marker rejection, destination previous-object
facts, fast-path eligibility, and result normalization.

The facade validates the copy source with `HeadObject` or `HeadObjectVersion`
before opening the source body. Missing source, explicit delete-marker source,
and failed source preconditions are distinct outcomes so protocol adapters can
map them without string matching.

Optimized copy adapters are acceleration paths behind the facade. They receive
already validated source and destination facts and must not decide S3
CopyObject semantics. The storage backend interface remains a primitive storage
interface; copy semantics are exposed through `Operations.CopyObject`.

Metadata directive handling is intentionally narrow until the object metadata
model grows. `COPY` preserves source `ContentType`, and `REPLACE` uses the
request `ContentType`; arbitrary user metadata remains unsupported.

### Admin API Wire Schema

`internal/adminapi` is the single source of truth for admin HTTP JSON body
types. Server admin handlers and the `volumeadmin` client package use aliases
to those wire types so response fields such as scrub peer failure details and
volume snapshot metadata cannot drift between producer and consumer packages.

Runtime concerns stay outside `adminapi`: handler dependencies, CLI options,
HTTP transport behavior, typed client errors, and server domain models remain
in their owning packages.

### Shard Group Peer Identity

Shard group peer identity is the node identifier stored in
`ShardGroupEntry.PeerIDs` for data Raft group membership. New shard group
metadata resolves known peer addresses to stable node IDs before writing group
membership; raft addresses are tolerated only as legacy/static aliases that
must be resolved at the cluster address book seam.

The MetaFSM address book is the canonical source for resolving node identity to
raft address. Runtime join, remove, observe, and data-group wiring paths should
cross an explicit nodeID-to-address resolution seam instead of storing raft
addresses as peer identity.

Legacy raft addresses already stored in shard group peer metadata are accepted
only as read-time compatibility input. Runtime membership views normalize peers
to node IDs when the address book can reverse-resolve the address. New shard
group entries and membership changes are nodeID-only. Legacy peers that cannot
be resolved remain observable as unresolved legacy peers instead of being
silently rewritten or treated as healthy node identities.

Unresolved legacy peers degrade read and observe paths but block membership
mutation. Existing raft-group attach may continue through the stored raft
address when possible, and admin status should surface the unresolved peer
state. Operations that add, remove, rebalance, or dynamically join data-group
voters must fail until the node mapping is restored or an explicit migration
resolves the legacy identity.

The address book lifecycle is add/update-only for the node identity unification
step. Static bootstrap and future dynamic join upsert nodeID-to-address
mappings, and raft address changes update the same node ID. Removing a peer
does not delete its address book entry; deletion and garbage collection are
separate operator lifecycle concerns because historical group metadata may
still need the mapping for interpretation.

Node identity and node liveness are separate signals. The address book is an
identity registry: it says which raft address belongs, or belonged, to a node
ID. Peer health is the liveness signal: it says whether that node is currently
reachable and safe for operational decisions. Identity normalization must not
claim that address book presence means a peer is live; true metaRaft liveness
monitoring remains a separate follow-up.

Cluster peer observation should present node ID as the primary peer identity
and raft address as resolved supporting detail. Role calculations compare node
IDs to node IDs. Until a real peer-health source exists, status language should
describe identity resolution state, such as configured or unresolved legacy,
rather than implying live/down liveness.

Existing shard group metadata is not automatically backfilled or rewritten as
part of identity unification. Historical entries remain as written; the
MetaFSM read/apply boundary normalizes them into node-ID runtime views when
possible, and all new membership writes use node IDs. Explicit migration
reports or rewrite commands are separate operational tooling.

The identity resolution boundary has two explicit directions: nodeID-to-address
for normal operation and address-to-nodeID for legacy compatibility. Code that
normalizes shard group peers should preserve whether a peer came from legacy
address resolution or remained unresolved so observe paths and mutation guards
can make different decisions.

### Cluster Peer Liveness Snapshot

The cluster peer liveness snapshot is the operator-facing view of cluster peer
identity and liveness. It composes existing signals rather than probing the
network on read paths.

Rows use `PeerID` as their primary identity. For resolved peers, `PeerID` is
the node ID. For unresolved legacy peers, `PeerID` is the legacy raft address so
operators can still see and act on the row that blocks membership mutation.

The snapshot separates identity state from liveness state. Identity state says
whether the row is self, resolved, or unresolved legacy. Liveness state says
whether the row is configured, recently observed live, in health cooldown, or
failed a probe. The snapshot should include a short reason string so callers do
not infer meaning from display labels.

The snapshot module is a pure composer. It consumes metaRaft voter membership,
the address book, peer health state, and optional recent probe results. Active
probing belongs to a separate monitor because status and admin read paths must
not perform network I/O just to render cluster state.

For metaRaft voters, positive liveness evidence means recent successful Raft
replication from the local node to that peer. The evidence source belongs inside
the Raft node implementation, because heartbeat and AppendEntries outcomes are
Raft timing facts. Runtime adapters translate that evidence into snapshot probe
results; the snapshot module does not subscribe to Raft events or maintain its
own liveness map.

Recent metaRaft replication evidence is fresh for three metaRaft election
timeouts. A resolved remote voter with fresh successful AppendEntries evidence
is `live`; a resolved remote voter with no fresh success evidence remains
`configured`. Failed heartbeats alone do not make a peer `probe_failed` for
membership-mutation policy because false-positive liveness is more dangerous
than conservative unknown state.

Follower nodes do not infer remote voter liveness from leader hints or inbound
Raft traffic. Followers report themselves as `live` and leave remote voters
`configured` unless they have their own explicit evidence source. Leader-side
replication evidence is the authoritative source for remove-peer preflight.

When signals disagree, identity resolution comes first. Unresolved legacy rows
remain `unresolved_legacy` with reason `identity_unresolved` rather than
claiming live or down state. For resolved rows, recent probe success wins over
recent probe failure, recent probe failure wins over peer-health cooldown, and
peer-health cooldown wins over the configured fallback. Rows with no liveness
signal remain `configured`.

The deep module lives in `internal/cluster` as a pure function that builds a
snapshot from explicit inputs. The snapshot includes the local node as a row
with identity state `self`. Compatibility adapters may omit self when filling
legacy wire fields such as `peers`, but the module's own interface represents
the whole membership view.

The cluster status wire response keeps legacy fields while adding a full
`peer_snapshot` row list. Legacy fields such as `peers`, `peer_addrs`,
`peer_states`, and `down_nodes` are derived from the snapshot rather than
recomputed by handlers. `down_nodes` means peers with an explicit negative
liveness signal, such as health cooldown or probe failure. Merely configured
peers with no liveness evidence are not reported as down.

Display policy and membership-mutation policy intentionally differ. Display
policy reports explicit negative signals as down. Membership-mutation policy is
stricter: only self and rows with positive `live` evidence count as alive for
quorum safety, configured-without-evidence rows are unknown, and unresolved
legacy rows block membership mutation. The generic predicates for these
policies live next to the snapshot module in `internal/cluster`; admin command
packages use those predicates rather than re-deriving policy from strings.

### Data Group Bucket Forwarding

Data group bucket forwarding is the runtime path that routes bucket-scoped
object and multipart operations from a non-owning or non-leading node to the
data Raft group that owns the bucket.

The forwarding module is scoped to data-group bucket operations. Node-scoped
queries such as scrub session status are intentionally outside this concept
because they do not route through bucket ownership, data-group lookup, or
leader gating.

Forward operation metadata owns transport-shape policy for bucket operations:
whether an operation is frame-only, body-streamed, or read-streamed, and
whether it mutates data. FlatBuffers encoding, reply parsing, retry policy,
leader-hint dialing, and storage semantics remain in their existing modules.

### IAM (ServiceAccount + AccessKey + Grant)

`internal/iam/` is the cluster IAM domain: it owns the ServiceAccount /
AccessKey / Grant model. As of v0.0.107.0 the legacy single-credential
`--access-key/--secret-key` flag is gone; bootstrap routes exclusively
through the admin UDS (`grainfs iam sa create admin --endpoint
<data>/admin.sock`). State lives in `iam.Store` as a COW-projected
`iamState` snapshot (`atomic.Pointer[iamState]` for lock-free reads,
mu-serialized writers); the canonical write path is the cluster
meta-Raft FSM. `MetaCmdType` 21..28 carry per-record IAM payloads
(SACreate, SADelete, KeyCreate, KeyRevoke, GrantPut, GrantDelete,
GrantWildcardPut), and `MetaCmdType` 31 (`IAMInitFirstSA`) carries the
composite `InitFirstSAPayload` for atomic first-SA bootstrap. Each
payload is a FlatBuffers blob in `internal/iam/iampb/`.

Secret_key persistence uses AES-256-GCM via `internal/encrypt.Encryptor`
with the SA id as additional-authenticated-data, so a stolen ciphertext
without the matching sa_id binding cannot be unwrapped against another
SA. Plaintext only ever lives in memory after `UnwrapSecret` and is
rebuilt at FSM apply / snapshot restore time.

Auth uses two layers. SigV4 verification (`s3auth.Verifier` +
`CachingVerifier`) resolves access_key → secret_key via the
`SecretLookup` closure that walks the IAM store. After the signature
matches, the auth middleware calls `iam.ResolveSA` to attach the
principal sa_id to the request context. The authz middleware then
serially evaluates IAM grants and bucket policies — both must allow.

Bootstrap path: a fresh cluster starts with an empty IAM store and
authzMiddleware always-on, so all S3 traffic returns 401 until an
operator calls `POST /v1/iam/sa` over the admin UDS. The first SA on an
empty store dispatches to `IAMInitFirstSA`, which atomically commits
the SA + AccessKey + wildcard Admin grant via a single FSM Apply. The
race guard is a fixed `DefaultSAID = "sa-default"`: concurrent
proposes collapse via FSM idempotent skip, and the losing operator
receives `409 Conflict` from the admin API. Subsequent SA creates take
the regular per-record path with no auto-grant. The sticky
`auth_enabled` bit was removed in v0.0.107.0; there is no anonymous
mode. Admin endpoints live on the admin UDS at `/v1/iam/*`; the CLI
(`grainfs iam ...`) talks to that socket via the same `--endpoint`
contract as `grainfs cluster`.

Reference: `docs/adr/0007-iam-foundation.md`,
`docs/superpowers/specs/2026-05-08-iam-foundation-design.md`.

### Volume Block I/O

Volume block I/O is the volume-layer path that turns logical byte-range reads,
writes, deferred writes, and discards into physical block object reads, writes,
cache invalidations, and allocation-accounting changes.

The first deepening scope owns block I/O planning, block merge rules, dedup and
direct-block write selection, block cache read/write behavior, pool quota
checks, and `AllocatedBlocks` accounting. Snapshot, clone, and rollback remain
separate callers for the first slice, though they may reuse the same result
shape later.
