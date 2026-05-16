# ADR 0014: Storage Operations Capability Plan Cache

## Status

Accepted (2026-05-17).
Related: ADR 0001 (Storage Mutation Results Facade),
ADR 0012 (Migration Service Lock-Free Publication),
ADR 0013 (Lifecycle Service Lock-Free Publication),
`docs/architecture/lock-free-audit.md`,
`CONTEXT.md` — Storage Decorator Capability Plan section.

## Context

The storage decorator capability plan (CONTEXT.md, ADR 0001) is the
`Operations` facade's internal plan for discovering optional capabilities
across the decorated backend stack. Before this ADR the plan cache used
`sync.RWMutex` plus a `[]uint64` generation snapshot. Every call to
`planForCall` allocated the slice to compare against cached state, even on
cache hit.

Two compounding issues:

1. **Hot-path allocation.** Every mutating S3 operation traverses
   `o.planForCall()` across 12 call sites (`operations_versioning.go` ×7,
   `operations_policy.go` ×3, `operations_acl.go` ×1, `operations_copy.go` ×1).
   Allocating a `[]uint64` on every fast-path call added GC pressure
   proportional to S3 write throughput.

2. **Cache scope.** Result-shape wrappers (`SwappableBackend`, `CachedBackend`,
   `wal.Backend`, `pullthrough.Backend`) constructed a fresh `*Operations`
   per call via `NewOperations(inner)` for `PutObjectWith*Result`. Each
   construction re-walked the chain to build a plan, defeating any
   per-instance cache.

The chain's only Generation() source is `SwappableBackend.gen`, bumped on
`Swap()` (rare). `docs/architecture/lock-free-audit.md` previously
catalogued the operations mutex under "FSM/state-machine consistency" —
accepted on the multi-map atomic-read argument. That argument collapsed
once the fast path was measured at 1 alloc + RLock rather than 0 alloc + no
lock.

## Decision

- The capability plan and the ACL capability plan are each published
  through an `atomic.Pointer` and validated against an independent
  `atomic.Uint64` generation counter sampled from the chain's single
  Generation() source. `planForCall` and `aclPlanForCall` are zero-alloc
  and lock-free on cache hit. Bench (Apple M3): 7.7 ns/op, 0 B/op,
  0 allocs/op.

- `NewOperations` walks the chain at construction and panics if more than
  one backend in the chain implements `operationPlanGeneration`. This locks
  in `SwappableBackend` as the sole source of cache-invalidating bumps. Any
  future wrapper that needs to invalidate the plan must either reuse
  `SwappableBackend.Swap` as the mutation point or extend the design before
  adding a second source.

- Each cache tracks its **own** generation. `planGen` invalidates `plan`
  only; `aclPlanGen` invalidates `aclPlan` only. Both observe the same
  upstream Generation() source but rebuild independently. An earlier
  mid-flight design shared one counter; that allowed rebuilding the ACL
  cache to overwrite the shared counter and make a stale main plan look
  fresh on the next read. Independent counters per cache eliminate that
  bug class.

- Result-shape wrappers hold a long-lived `*Operations` over their inner
  backend rather than constructing one per call:
  - `SwappableBackend.ops atomic.Pointer[Operations]` — reset on `Swap`.
  - `CachedBackend.ops`, `wal.Backend.ops`, `pullthrough.Backend.ops` — set
    once at construction; the inner backend never changes for these
    wrappers so the cache lives until the wrapper does.

- `SwappableBackend.Swap` orders its writes
  `ops.Store(nil); inner.Store(b); gen.Add(1)` so a reader that observes
  the new inner cannot also observe the stale ops, and any racing
  `planForCall` on the freshly-built ops sees the new generation and
  invalidates correctly.

- `SwappableBackend.cachedOps` uses a generation seqlock plus CAS
  publication. If `gen` bumps between sampling `inner` and publishing the
  freshly-built `*Operations`, the build is discarded and retried.
  CompareAndSwap on a nil slot ensures we never overwrite an entry a
  racing reader already published or a later `Swap` reset to nil.

## Consequences

- Per-mutating-S3-op allocation count drops by one heap slice. GC pressure
  on write-heavy workloads decreases proportional to PUT/COPY/multipart
  throughput. Three regression benchmarks
  (`BenchmarkOperationsPlanForCallFastPath`,
  `BenchmarkOperationsPlanForCallFastPathNoGenSource`,
  `BenchmarkOperationsACLPlanForCallFastPath`) guard the 0-alloc property.

- `internal/storage/operations.go` is removed from the audit's mutex
  inventory. The transition is recorded in the audit's "Changes In This
  Audit" section alongside `CachedBackend`.

- ADR 0001 (Storage Mutation Results Facade) remains the source of truth
  for what the plan contains and how callers reach it. This ADR scopes
  only the caching mechanism.

- The lock-free publication pattern family in this codebase now has three
  distinct shapes:
  - **Whole-state CoW snapshot under a single applier** (IAM Store,
    ADR 0007): readers load an immutable `*State` and read fields directly.
  - **Worker-pointer publication for leader-only services** (migration,
    lifecycle, ADR 0012 / ADR 0013): one `atomic.Pointer[Worker]` reset on
    leadership transitions; running state is derived from `Load() != nil`.
  - **Generation-validated cache for capability discovery** (this ADR):
    `atomic.Pointer` plus an `atomic.Uint64` validated against a single
    upstream Generation() source; independent counters per cache.

- The "two-adapter rule" the prior ADRs flagged for the generic CoW
  pattern is satisfied at the *whole-state CoW* shape (IAM Store +
  Operations capability plan, both validated against version counters).
  A future third adapter — for example a `PackedBackend` index snapshot —
  starts from this pattern, not from the scrubber Director actor or the
  worker-pointer publication of ADR 0012 / ADR 0013.

- Future architecture reviews that propose actor consolidation for
  `Operations` should weigh their argument against this ADR. A proposal
  that introduces new cross-instance state may change the calculus; a
  proposal that only changes the publication shape of the existing caches
  does not.

## Out of scope

- `internal/storage/packblob.PackedBackend.mu` — the audit already flags
  this as a conditional follow-up ("if packed small object reads become a
  hot-path bottleneck, convert this to the same immutable snapshot pattern
  used by `CachedBackend`"). This ADR establishes the pattern; measurement
  (pprof mutex profile, small-object read throughput) triggers the
  conversion, not architectural review alone.

- `internal/policy/CompiledPolicyStore` read/write path separation — a
  smaller, separate concern. Lower expected impact than the audit-flagged
  follow-ups.

- A generic CoW abstraction shared across IAM, Operations, and a future
  third adapter — the three shapes are similar enough to learn from each
  other but different enough that a generic wrapper would obscure the
  per-adapter discovery rules. Concrete sharing remains a copy-paste
  between adapters, consistent with how ADR 0012 / ADR 0013 treat
  migration and lifecycle.

- `SwappableBackend.PutObjectWithACL` and the package-level
  `putObjectWithACLOnBackend` — these use direct type assertions on the
  outer backend rather than walking the chain. The plan cache deepening
  does not change that behavior; semantic differences between
  outermost-only and chain-walking ACL discovery remain a separate
  question.
