# ADR 0013: Lifecycle Service Lock-Free Publication (Actor Reject)

## Status

Accepted (2026-05-17).
Closes the reservation in ADR 0012 about `internal/lifecycle`.
Related: ADR 0011 (Bucket Lifecycle Policy via FSM Command),
ADR 0012 (Migration Service Lock-Free Publication),
`docs/architecture/scrubber-director-actor.md`,
`docs/architecture/lock-free-audit.md`.

## Context

ADR 0012 deepened `internal/migration.Service` by removing its mutex and
publishing the worker pointer through `atomic.Pointer[Worker]`. The decision
explicitly reserved `internal/lifecycle.Service`:

> *The lifecycle service has a similar overall shape but maintains its own
> `Worker.mu`-protected stats surface, so a separate decision applies and may
> reach a different conclusion.*

This ADR records that follow-up. The conclusion is the same shape as ADR 0012,
extended with a worker-side lock-free transition for the executor stats surface
so that no mutex remains in either type.

### What lifecycle actually looks like

`internal/lifecycle.Service` holds **only executor-lifecycle bookkeeping**:
`running bool`, `worker *Worker`, `cancelFn`, plus a `sync.WaitGroup`. The
only cross-goroutine state is the `worker` pointer, which is published by the
`Run` goroutine (in `reconcile -> start/stop`) and read by external `Status`
callers from the admin HTTP endpoint.

`Service.start` and `Service.stop` are invoked exclusively from `reconcile`,
which is itself called only from inside `Run`. `Run` is started in one place,
`go state.lifecycleSvc.Run(ctx)` in `internal/serveruntime/boot_phases_node_services.go`.
The `running` / `cancelFn` / `workerWG` fields are therefore single-goroutine
state with one external reader: `Status`.

`internal/lifecycle.Worker` keeps the executor cycle counters. Three counters
(`ObjectsChecked`, `Expired`, `VersionsPruned`) are already mutated with
`atomic.AddInt64`. The only field that was protected by `Worker.mu` is
`stats.LastRun time.Time`, which the cycle writes and admin `Status` reads.
The `cancel context.CancelFunc` field captured in `Worker.Run` was never read
back: `Worker.Stop` has no production callers, because lifecycle stop is
driven by cancelling the workerCtx from `Service.stop`.

### Why the scrubber-Director pattern does not transfer

The scrubber Director consolidated four in-memory maps
(`sources`, `verifiers`, `sessions`, `dedup`) into a single controller actor
because their mutations were split between mutex critical sections and the
worker loop. lifecycle has no analogous in-memory registry: `Stats` is one
time field plus three counters, all of which have a lock-free atomic shape.

Apply the deletion test from the architecture review skill. If a hypothetical
controller actor were deleted from `lifecycle.Service`, complexity would not
reappear across N callers. The actor would only publish a worker pointer and
serialise admin-side reads of cycle counters that atomic operations already
serialise correctly. That is a pass-through actor, which the architecture
review skill explicitly rejects, and it is the same conclusion ADR 0012
reached for migration.

The remaining `Worker.mu` is even narrower: it serialises one `time.Time`
write against one read. An `atomic.Int64` of unix nanoseconds, identical in
shape to `scrubber.liveSession.doneAt`, expresses the same invariant without
the lock and without introducing an actor.

## Decision

- `lifecycle.Service.mu` is removed. The worker pointer is published through
  `atomic.Pointer[Worker]` for lock-free read by `Status`.
- `lifecycle.Service.running` is removed. Running state is derived from
  `worker.Load() != nil`, matching ADR 0012.
- `Service.cancelFn` and `Service.workerWG` remain plain fields because they
  are only accessed by the `Run` goroutine via `reconcile -> start/stop`.
- `worker.Store(nil)` happens only in `stop()` after `workerWG.Wait()`. There
  is no `defer worker.Store(nil)` inside the executor goroutine. This matches
  the migration shape and keeps publication ownership in `stop()`.
- `lifecycle.Worker.mu` is removed. `Stats.LastRun` is published through
  `lastRunNano atomic.Int64` storing `time.UnixNano`; `0` means "never run".
  `Stats()` translates `0` to a zero-value `time.Time{}` so callers that
  rely on `IsZero` see the same behaviour. The three existing counter fields
  are converted from raw `int64` to `atomic.Int64` for type-level consistency.
- `lifecycle.Worker.Stop` and `lifecycle.Worker.cancel` are removed. They are
  unreferenced in production code; cancellation flows through the workerCtx
  cancellation in `Service.stop`. The associated `_test.go` references, if
  any, switch to cancelling the parent context.
- `Service.workerRunningForTest` continues to exist for tests but reads
  `worker.Load() != nil` instead of the deleted `running` field.

## Consequences

- The change is purely a publication-mechanism swap. Existing
  `service_test.go` and `worker_test.go` semantics are preserved; the
  externally observable test surface, including the `Status().LastRun.IsZero()`
  assertion in `service_test.go`, does not move.
- `Status` callers see one fewer lock acquisition on the admin path.
- The `Worker` type loses two public symbols (`Stop`, the documented intent of
  graceful executor shutdown was already delegated to ctx cancellation).
- Future architecture reviews that propose actor consolidation for
  `lifecycle.Service` should weigh their argument against the points in this
  ADR and ADR 0012. A proposal that introduces new in-memory state — for
  example a pending-deletes queue or per-bucket cursors — may change the
  calculus; a proposal that only changes the publication shape of the worker
  pointer or the cycle stats surface does not.
- ADR 0012 and ADR 0013 together establish the **lock-free publication
  pattern** for leader-only executor services in this codebase. A third sibling
  appearing later (for example a future `compaction.Service`) starts from this
  pattern, not from the scrubber Director actor.

## Out of scope

- Common `JobActor` abstraction across scrubber, lifecycle, and migration —
  the two-adapter rule is still not satisfied because neither lifecycle nor
  migration becomes a JobActor adapter.
- Per-bucket cursor durability for the lifecycle executor. The current
  executor scans every bucket from the start on every interval; there is no
  cursor to lose on leader change. This stays out of scope.
- Rate-limit configuration surface (`rate.NewLimiter(100, 10)`) — separate
  concern.
- Splitting the executor into per-bucket workers (executor pool). Operationally
  unchanged, and the current single-goroutine cycle is well within the
  budget for the workloads we measure.
