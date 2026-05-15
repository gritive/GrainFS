# ADR 0011: Bucket Lifecycle Policy via FSM Command

## Status

Accepted (2026-05-11).
Related: ADR 0007 (IAM Foundation, meta-Raft FSM command pattern) and ADR 0001
(storage operations facade, handlers to domain boundary discipline).

## Context

`internal/lifecycle.Store` currently writes directly to
`cluster.DistributedBackend.FSMDB()` using raw `db.Update(...)` calls. Because it
does not pass through the meta-Raft FSM, lifecycle config is not replicated
between nodes. That can silently drop policy in two cases:

1. If a client sends **PUT to a follower node**, the policy is stored only in
   that follower's local BadgerDB. The worker running on the leader never sees
   it.
2. If a **leader change** occurs, the new leader starts its worker without
   policies that existed only in the previous leader's local BadgerDB. Operator
   GET responses can also differ by node.

`internal/lifecycle.Service` introduced a deeper module boundary. To preserve
the invariant that a policy applied by an operator is executed by the worker,
policy changes must synchronize to every voter. The IAM domain already solves
the same class of problem with meta-Raft FSM commands (`MetaCmdType` 21..33).

## Decision

- Bucket lifecycle config changes are replicated through **meta-Raft FSM
  commands**.
- New `MetaCmdType` values:
  - `MetaCmdTypeBucketLifecyclePut = 34`
  - `MetaCmdTypeBucketLifecycleDelete = 35`
- **Payload encoding is the original S3 wire XML bytes**, exactly the body of
  `PUT /{bucket}?lifecycle`. Validation runs through `lifecycle.Validate` before
  proposal. FSM apply treats the payload as opaque bytes and writes it to the
  `"lifecycle:{bucket}"` key in `lifecycle.Store`.
  The canonical lifecycle config is the S3 wire XML, and operator GET
  round-trips should be byte-for-byte. This differs from IAM payloads, which use
  FlatBuffers because IAM wire formats and internal domain models are not the
  same.
- Server handlers (`PUT/GET/DELETE /{bucket}?lifecycle`) accept only the XML
  body and call `lifecycle.Service.Apply`, `Get`, or `Delete`. They do not access
  `lifecycle.Store` directly.
- The worker still runs only on the leader node. The invariant closes because
  the policy read by that worker is now replicated to all nodes.

## Consequences

- Follower PUTs apply to every voter's store, so non-leader-aware clients can
  safely configure lifecycle policy.
- After a leader change, the new leader's worker sees existing policies.
- Raft snapshots and InstallSnapshot naturally include lifecycle keys because
  they are part of the FSM-managed BadgerDB.
- PUT and DELETE pay one extra Raft round trip. Lifecycle changes are operator
  events, so this is acceptable.
- `MetaCmdType` 34 and 35 are permanently reserved. If payload encoding changes
  to FlatBuffers later, that change must allocate new IDs rather than changing
  the meaning of existing IDs.
- Upgrade behavior: lifecycle keys written only to a follower's local BadgerDB
  before this change remain orphaned. New workers read the leader's store, so
  those orphan keys do not affect execution. Operators can reapply a policy once
  if they want a clean replicated state.

## Out of scope

- Exposing worker execution statistics (`Stats`) through admin or dashboard
  surfaces is a separate follow-up.
- Multiple workers, such as read-only audit workers on non-leader nodes, would
  change the domain model and are outside this ADR.
