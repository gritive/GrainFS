# Storage Operations Facade

**Status:** Proposed.

## Problem

Upper layers currently discover optional storage backend capabilities directly.
The same probing pattern appears in server handlers, volume code, snapshot
setup, cache wrappers, WAL wrappers, and recovery write gating. That spreads
storage side-effect ordering across callers.

Examples:

- Versioned object reads/deletes require unwrap/type assertions in server code.
- Cache and WAL wrappers must re-declare optional interfaces to preserve side
  effects.
- Volume block I/O probes partial I/O, deferred put, and read/write preferences.
- Recovery write gating must manually mirror every mutating capability.

The result is a shallow storage interface: callers must understand both the
method they want and the decorated backend stack underneath it.

## Decision

Introduce a storage operations facade as the upper-layer entry point for
meaningful storage actions. `storage.Backend` remains the low-level primitive
implemented by LocalBackend, DistributedBackend, ClusterCoordinator, and
decorators.

The facade owns capability lookup and storage side-effect ordering. Callers
should stop unwrapping decorated backends directly.

## Data Flow

```text
HTTP handler
    |
    v
storage.Operations
    |
    +-- outer-first adapter lookup
    |       |
    |       +-- RecoveryWriteGate blocks writes
    |       +-- CachedBackend invalidates cache
    |       +-- wal.Backend records PITR entries
    |
    +-- safe fallback only through Operations
            |
            +-- PutObjectWithACL fallback: PutObject -> SetObjectACL
            +-- CopyObject fallback: GetObject -> PutObjectWithACL/PutObject
```

`server.Server` should hold `storage.Operations` as its primary storage entry
point for request handlers. Raw `storage.Backend` access should not remain in
server handlers after the first implementation slice, except where the
operation is already part of the base `storage.Backend` contract and has no
optional capability probing.

Server construction still needs storage-shaped dependencies for non-handler
subsystems such as snapshot management, volume management, and Iceberg catalog
wiring. Pass those dependencies through an explicit composition type instead of
giving handlers a raw backend escape hatch:

```go
type ServerStorage struct {
    Ops           *storage.Operations
    VolumeBackend storage.Backend
    Snapshotable   storage.Snapshotable
    DBProvider     storage.DBProvider
}
```

Handlers use `Ops`. `Server.New` uses the other fields only while wiring
subsystems.

## Initial Scope

The first implementation should be intentionally narrow and remove server-side
probing first:

- `PutObjectWithACL`
- `CopyObject`
- `GetObjectVersion`
- `HeadObjectVersion`
- `ListObjectVersions`
- `DeleteObjectVersion`
- `DeleteObjectReturningMarker`
- `SetBucketVersioning`
- `GetBucketVersioning`
- `SetBucketPolicy`
- `GetBucketPolicy`
- `DeleteBucketPolicy`

This first server slice also owns the policy store/cache path. To avoid an
upward dependency from `internal/storage` to `internal/server`, policy parsing,
compilation, and in-memory cache state should move into `internal/policy`.
Both `internal/server` and `internal/storage` may depend on that lower package.

Later expansions can cover snapshot operations and volume block I/O once the
server seam is stable.

## Lookup Rules

All lookup is outer-first.

Read-only capabilities may unwrap until an adapter is found.

Mutating capabilities are also outer-first, but inner fallback is only allowed
when the facade can preserve the same side-effect ordering through existing
facade operations. Directly jumping to an inner mutating adapter can bypass
cache invalidation, WAL recording, or recovery write gating.

Examples:

- `PutObjectWithACL`: if no atomic adapter is present, fallback may call
  facade `PutObject` followed by facade `SetObjectACL`. If `SetObjectACL`
  fails after `PutObject` succeeds, the facade must attempt rollback by
  hard-deleting the newly-created version. If hard-delete is unavailable, return
  an explicit rollback failure.
- `CopyObject`: `Operations.CopyObject(ctx, CopyObjectRequest)` owns S3
  CopyObject semantics, including metadata directive, copy-source
  preconditions, ACL/versioned source behavior, and delete-marker source
  handling. `storage.Copier` remains a lower-level optimization adapter for
  simple metadata-only copies, while the facade's private copy acceleration
  path carries the already-validated request-shaped fields needed by backends
  such as packed blobs. Optimized adapter paths and stream-copy fallback must
  produce the same
  externally-observable result.
- `DeleteObjectVersion`: no generic fallback; unsupported if no safe adapter is
  found.
- `DeleteObjectReturningMarker`: may fall back to `DeleteObject` with an empty
  marker ID, matching existing behavior where needed.
- Bucket policy/versioning setters have no fallback.

```text
operation requested
    |
    +-- adapter exists on outer chain? ---- yes --> call adapter
    |
    no
    |
    +-- safe fallback exists? ------------ yes --> call fallback via Operations
    |
    no
    |
    +-- return UnsupportedOperation{Op, Reason}
```

## Implementation Shape

`storage.Operations` is the single public entry point, but the implementation
should stay split by operation family:

- `operations.go`: struct, constructor, adapter lookup, shared errors.
- `operations_acl.go`: object ACL and atomic put behavior.
- `operations_copy.go`: S3 CopyObject request semantics and fallback.
- `operations_versioning.go`: versioned object reads, listing, and deletes.
- `operations_policy.go`: bucket policy persistence and policy cache behavior.

This keeps the Interface deep while preserving Implementation locality.

## Performance Constraints

`Operations` should build a capability plan during construction instead of
walking the decorated backend chain on every request. The plan records
outer-first adapters and whether each safe fallback is available.

Dynamic wrappers such as swappable backends need versioned or lazy plan refresh.
Static chains may keep a fixed plan; dynamic wrappers should expose a
generation marker or equivalent signal so `Operations` can refresh stale
capability plans without putting a full chain walk on every hot-path call.

`CopyObject` fallback must stay streaming. Resolve metadata directive, ACL,
versioning, and delete-marker decisions before consuming the source body. Then
stream the source reader into the destination put path.

Policy authorization remains on the compiled in-memory hot path. The facade owns
policy CRUD persistence and cache synchronization, while auth middleware uses a
`policy.Authorizer` directly for `Allow` checks.

ACL fallback rollback must use only the `VersionID` returned by the successful
`PutObject` call. It should not issue a follow-up latest-version lookup to find
the rollback target. If the returned object has no `VersionID`, the facade must
return an explicit rollback failure rather than guessing.

## Error Model

Add a typed unsupported-operation error, with `errors.Is` compatibility for a
shared sentinel:

```go
var ErrUnsupportedOperation = errors.New("unsupported storage operation")

type UnsupportedOperationError struct {
    Op     string
    Reason UnsupportedReason
}
```

Facade methods should return this typed error when no safe adapter or fallback
exists. Protocol layers map it to their own response shape, such as S3
`NotImplemented`, while preserving non-unsupported domain errors such as
recovery write gating. The storage error must not carry HTTP status codes, S3
XML response codes, or protocol response text.

## Test Surface

The facade interface is the test surface.

Focused tests should cover decorated backend stacks:

- cache + WAL + distributed backend hard version delete preserves WAL and cache
  behavior.
- `storage.Operations` contract tests use spy decorators to verify
  outer-first lookup, unwrap avoidance, fallback ordering, and recovery gate
  blocking.
- atomic ACL path uses `PutObjectWithACL` when available.
- ACL fallback calls put then set without unwrapping around decorators.
- ACL fallback failure rolls back the newly-created version, or returns explicit
  rollback failure if hard-delete is unavailable.
- ACL rollback tests cover atomic path, fallback success, set-ACL failure with
  hard-delete rollback success, set-ACL failure with unsupported rollback, and
  recovery gate active.
- CopyObject facade tests cover metadata directive, preconditions, ACL,
  versioned source restore, delete-marker source behavior, same-source no-op
  rejection, and wrapper ordering across optimized and fallback paths.
- CopyObject server integration tests cover encoded source parsing,
  `versionId`, metadata directive validation, source conditional headers,
  unsupported user metadata, and response XML.
- Policy parser, compiler, cache, and benchmarks move with the code into
  `internal/policy`; server keeps HTTP bucket-policy integration tests.
- Server constructor tests cover `ServerStorage` wiring for volume manager,
  snapshot availability, Iceberg catalog auto-wiring, and handler calls through
  `Operations` instead of raw backend probing.
- unsupported mutating capabilities return `ErrUnsupportedOperation`.

The implementation file should include a short ASCII diagram comment mirroring
the mutating operation decision tree above.
