# ADR 0010: Relocate Bucket-Upstream Admin/CLI Surface

## Status

Accepted (2026-05-09). Supersedes the surface-placement portion of ADR 0009.
ADR 0009's FSM, snapshot, and AAD decisions remain unchanged.

## Context

ADR 0009 decided to store bucket-scoped upstream credentials in the IAM store.
It also placed the admin path and CLI under the `iam` category:
`/v1/iam/bucket-upstream` and `grainfs iam bucket-upstream ...`. After the
v0.0.123.0 release, user review found that placement to be a category mismatch:
`iam` is the principal axis, covering SAs, access keys, and grants, while
`bucket-upstream` configures external credentials on the resource axis, scoped
to a bucket.

## Decision

- CLI:
  `grainfs iam bucket-upstream {set,...}` becomes
  `grainfs bucket upstream {put,get,list,delete}`.
- Admin path:
  - `POST /v1/iam/bucket-upstream` becomes `PUT /v1/buckets/upstream`.
  - `GET /v1/iam/bucket-upstream` becomes `GET /v1/buckets/upstream`.
  - `GET /v1/iam/bucket-upstream/:bucket` becomes
    `GET /v1/buckets/:bucket/upstream`.
  - `DELETE /v1/iam/bucket-upstream/:bucket` becomes
    `DELETE /v1/buckets/:bucket/upstream`.
- HTTP method changes from POST to PUT for idempotent upsert, matching grant
  behavior.
- The old path and CLI are removed immediately because admin UDS is an internal
  interface.
- FSM implementation (`internal/iam`), snapshot trailer, AAD
  `"bucket-upstream:"+bucket`, `MetaCmdType` IDs 32/33, and FlatBuffers payloads
  are preserved.

## Consequences

- Raft compatibility between v0.0.122 and v0.0.133 is preserved because payload
  and snapshot formats do not change.
- Breaking change: scripts using the old CLI commands must update. CHANGELOG
  provides the command mapping.
- Future bucket subresources such as policy, lifecycle, and events can use the
  same `/v1/buckets/:bucket/*` prefix and `grainfs bucket *` CLI tree.

## Out of scope

Splitting the FSM into a separate package remains future work. The current FSM
has deep dependencies on Raft snapshot, AAD, and command type definitions, so it
needs a separate ADR and PR.
