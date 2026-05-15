# ADR 0009: Bucket-Scoped Upstream Credentials

**Status:** Accepted
**Date:** 2026-05-08
**Predecessor:** ADR 0008 (drop-access-key-flag)

## Context

PR #258 (v0.0.113.0) completed the IAM-only auth migration for cluster S3
authentication, removing `--access-key` / `--secret-key` flags. The
pull-through subsystem retained its three flags — `--upstream`,
`--upstream-access-key`, `--upstream-secret-key` — because the work was
scoped narrowly to bootstrap and authorization.

Two problems remained after PR #258:

1. **Cmdline secret leak.** Plaintext upstream credentials were exposed in
   `ps`, `journald`, and audit logs. Inconsistent with the IAM model
   PR #258 established for cluster S3 credentials.
2. **Cluster-wide single endpoint.** Pull-through accepted one
   `(endpoint, access_key, secret_key)` tuple per cluster. Per-bucket
   migration sources couldn't be expressed; multi-source migrations
   required running multiple `GrainFS` clusters or staging the migration
   sequentially.

The user describes pull-through as a *migration tool*, not a permanent
caching layer. The current implementation only supports the caching
shape and only in cluster-wide form.

## Position within the broader migration design

This PR is **Phase 1** of the bucket-level migration system tracked by
`TODOS.md:63` ("Migration: bucket-level server-side injection"). The
broader design covers three modes (`import` / `mirror` / `pull-through`)
with BadgerDB cursor state, raft worker leader election, admin API, and
dashboard. Phase 1 (this PR) ships only the credentials substrate that
all three modes will share. Subsequent phases — cutover signal, progress
tracking, list/head/copy upstream operations, mirror/import semantics —
are deferred and tracked in TODOS.md.

## Decision

Move per-bucket upstream credentials into the IAM Store as a new record
type `BucketUpstream{bucket, endpoint, access_key, secret_key_enc, …}`.
Drop the three `--upstream*` cmdline flags. Add admin UDS API verbs at
`/v1/iam/bucket-upstream` and a CLI surface `grainfs iam bucket-upstream
set/get/list/delete`. The pull-through `Backend` takes a `Resolver` that
performs per-bucket lookup against the IAM Store and caches resolved
S3 clients with lazy invalidation on rotation.

## Alternatives considered

1. **Environment variables** (`GRAINFS_UPSTREAM_ACCESS_KEY` /
   `GRAINFS_UPSTREAM_SECRET_KEY`).
   Closes the `ps` leak but inherits the cluster-wide single-endpoint
   shape and can't be rotated without restart. Does not solve the user's
   primary objection (per-bucket configurability).
2. **`--upstream-creds-file=<path>`.**
   Same shape as env-var. Adds operational footprint (file replication
   across nodes, file permissions) without solving the per-bucket
   problem. Not aligned with the IAM-managed model.
3. **Separate `cluster` namespace / new `BucketSource` FSM type.**
   Cleaner domain separation (storage routing vs. IAM identity) but
   forces secret encryption layer to be lifted out of `iam` or
   duplicated. Doubles the propose/admin/CLI surface for one record
   type. Hybrid (routing in cluster, secrets in IAM with cross-FSM
   reference) added cross-FSM consistency burden.
4. **Hybrid — `BucketSource` in cluster meta + `BucketUpstreamCreds` in
   IAM, joined by an opaque ID.**
   Most architecturally pure. Two-FSM partial-failure modes increase
   operator surface area for negligible benefit. Rejected.

## Consequences

**Positive:**

- Cmdline plaintext leak eliminated for upstream credentials.
- Per-bucket configuration enables multi-source and gradual migration.
- Reuses existing `iam.WrapSecret` / `iam.UnwrapSecret` paths and the
  v3 snapshot trailer-append layout — minimal new infra.
- CLI surface symmetrical with `grainfs iam sa` group.
- AAD namespace prefix `"bucket-upstream:" + bucket` is provably disjoint
  from the `sa_id` AAD space — defense-in-depth at trivial cost.

**Negative:**

- Breaking removal of three flags. Operators must migrate to the new CLI.
  Migration is a single `grainfs iam bucket-upstream set` per bucket.
- IAM Store gains responsibility outside identity/auth. Justified by
  shared encryption + replication mechanics; the alternative (separate
  FSM) costs more.

**Operational notes:**

- **Snapshot compatibility (per /plan-eng-review override A1):** the IAM
  snapshot stays at version 3. The bucket-upstreams section is appended
  as TRAILER bytes after the existing revoked-AKs section. Pre-v0.0.123
  readers stop at their existing `return nil` after the revoked loop and
  ignore the trailing bytes; v0.0.123+ readers consume the trailer.
  Bidirectional rolling upgrade is preserved — downgrading a node back
  to v0.0.114 just makes the per-bucket records invisible until upgraded
  again. No "forward-only" trap.

- **Rotation semantics (per /plan-eng-review override A12):** the
  Resolver invalidates lazily — within one Resolve call after the FSM
  apply commits, callers see the new client. In-flight requests still
  using the old `*S3Upstream` complete with the old credentials; this
  is acceptable because rotation is admin-driven and AWS S3 typically
  tolerates parallel old/new credentials briefly.

## Out-of-scope (Phase 2+)

Tracked in `TODOS.md:63` ("Migration: bucket-level server-side
injection"):

- Migration cutover signal (`status` field, `POST /:bucket/cutover`).
- Migration progress / dashboard.
- `import` mode (one-shot bulk copy).
- `mirror` mode (continuous replication).
- `List` / `Head` / `CopyObject` upstream operations.
- Per-bucket cache TTL / write-back policy.
- Multi-source per-bucket (sharding upstream sources).
- Worker leader election + cursor state in BadgerDB.
