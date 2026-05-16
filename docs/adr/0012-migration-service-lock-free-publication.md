# ADR 0012: Migration Service Lock-Free Publication (Actor Reject)

## Status

Accepted (2026-05-17).
Related: ADR 0011 (Bucket Lifecycle Policy via FSM Command),
`docs/architecture/scrubber-director-actor.md`,
`docs/architecture/lock-free-audit.md`.

## Context

The scrubber Director and the alerts webhook Dispatcher were both deepened
into single-owner actors (controller goroutine + inbox + ephemeral workers).
A natural follow-up is to ask whether `internal/migration.Service` should
receive the same treatment, so that scrubber, lifecycle, and migration share
a common JobActor abstraction.

The same proposal will resurface in future architecture reviews simply because
two siblings already went that direction. This ADR records why the migration
service is **not** a good fit and what the correct deepening is, so that future
reviewers do not re-investigate the same ground.

### What migration actually looks like

`internal/migration.Worker` is **already actor-shaped**:

- a single `Run` goroutine selects on `trigger / tickC / ctx.Done`
  (`internal/migration/worker.go:50-68`).
- the worker holds **no in-memory registry state**. Job records, cursors,
  and statuses live durably in `JobStore` (BadgerDB). The worker reads job
  state; it does not maintain a parallel in-memory map.
- `Trigger` is a cap-1 non-blocking signal whose silent-drop semantics is
  safe because the ticker fallback acts as a multi-node safety net for any
  trigger lost in transit between submit and pickup.

`internal/migration.Service` holds **only worker-lifecycle bookkeeping**:
`running bool`, `worker *Worker`, `cancelFn`, plus a `sync.WaitGroup`. The
only cross-goroutine state is the `worker` pointer, which is published by the
`Run` goroutine (in `reconcile -> start/stop`) and read by external
`SubmitJob` callers.

### Why the scrubber-Director pattern does not transfer

The scrubber Director's actor consolidation eliminated four in-memory maps
(`sources`, `verifiers`, `sessions`, `dedup`) whose mutations were split
between mutex critical sections and the worker loop. That genuinely concentrated
lifecycle responsibility, which is the locality argument from
`docs/architecture/scrubber-director-actor.md`.

`migration.Service` has no analogous in-memory registry. The mutex it holds
protects pointer publication, not lifecycle responsibility. Replacing that
publication with an actor message queue would not concentrate any state that
is currently scattered; it would only change the **shape** of pointer
publication while adding an inbox, a dispatcher goroutine, and a message
catalogue.

Apply the deletion test from the architecture review skill: if we deleted the
hypothetical controller actor, complexity would not reappear across callers.
The actor's only job would be to publish a worker pointer that a `Run`
goroutine already owns. That is a pass-through actor, which the architecture
review skill explicitly rejects.

## Decision

- `migration.Service.mu` is removed. The worker pointer is published through
  `atomic.Pointer[Worker]` for lock-free read by `SubmitJob`.
- `cancelFn` and `workerWG` remain plain fields because they are only
  accessed by the `Run` goroutine.
- `running` state is derived from `worker.Load() != nil` rather than carried
  as a separate atomic flag. The test helper `workerRunningForTest` uses the
  same derivation.
- `migration.Worker` and its trigger / ticker semantics are not changed.
- `Trigger` keeps silent-drop semantics; the ticker fallback remains the
  multi-node safety net. A `MigrationTriggerDroppedTotal`-style metric is out
  of scope and may be revisited if the silent drop ever becomes a debugging
  obstacle.

## Consequences

- The change is purely a publication mechanism swap. Existing
  `service_test.go` and `worker_test.go` semantics are preserved; the test
  surface does not move.
- `SubmitJob` callers see one fewer lock acquisition on the hot path.
- Future architecture reviews that propose actor consolidation for
  `migration.Service` should weigh their argument against the points in this
  ADR. A proposal that introduces new in-memory job registry state may change
  the calculus; a proposal that only changes pointer-publication shape does
  not.
- This ADR scopes only `internal/migration`. The lifecycle service has a
  similar overall shape but maintains its own `Worker.mu`-protected stats
  surface, so a separate decision applies and may reach a different
  conclusion.

## Out of scope

- `internal/lifecycle.Service` — covered by its own analysis.
- Common JobActor abstraction across scrubber, lifecycle, and migration —
  the "two-adapter rule" is not yet satisfied because migration intentionally
  does not become a JobActor adapter.
- Surfacing `Trigger` silent-drop counts as a metric.
- Replacing the ticker fallback with explicit follower-to-leader forwarding
  for `SubmitJob`.
- `JobStore.SaveCursor` writes the per-bucket pagination cursor directly to
  local BadgerDB and bypasses the meta-Raft FSM (`internal/migration/store.go`
  has the explicit `// bypasses Raft` annotation). A leader change between
  scan pages can therefore resume from an earlier cursor on the new leader.
  This is an unrelated durability gap that exists with or without this ADR —
  the actor-reject argument depends only on the absence of in-memory
  registry state, not on FSM replication of cursor writes.
