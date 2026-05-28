# ADR 0015: LocalBackend Is a Test Fixture, Not a Production Backend

## Status

Accepted (2026-05-28).
Related: ADR 0001 (Storage Mutation Results Facade),
PR #581 (`v0.0.379.0` — DEK data-sealing cipher → XAES-256-GCM),
PR #596 (`v0.0.393.0` — R1: DEK-sealed WAL/packblob/PUT-pipeline + at-rest format 4),
TODOS `At-rest unification remainder — static→DEK`.

## Context

`internal/storage/LocalBackend` (declared across `local.go`, `multipart.go`, `append.go`,
with helpers in `encrypted_badger.go`) historically read as a candidate production storage
backend. Audit on 2026-05-28 established that it is no longer one:

1. **Zero production callers.** A repository-wide scan
   (`grep -rE 'storage\.[A-Za-z_]*LocalBackend' --include='*.go' cmd/ internal/`) returns no
   non-`_test.go` consumer outside `internal/storage/` itself. Neither `cmd/grainfs/**`
   nor `internal/serveruntime/**` instantiates any of the seven exported constructors
   (`NewLocalBackend`, `NewEncryptedLocalBackend`, `NewMultiRootLocalBackend`,
   `NewMultiRootLocalBackendWithDataWAL`, `NewLocalBackendWithDataWAL`,
   `NewEncryptedLocalBackendWithDataWAL`, `NewLocalBackendWithDEKKeeper`).

2. **Wide test-fixture usage.** Approximately 75 external `*_test.go` files across
   `internal/server`, `internal/nfs4server`, `internal/cluster`, `internal/volume`,
   `internal/vfs`, `internal/nbd`, etc. instantiate `LocalBackend`. Many `*_test.go`
   files inside `internal/storage/` also depend on its package-private surface.
   Removing or relocating the type would force a ~100-file import rewrite plus a
   same-package unexported-access export pass.

3. **The real production storage path runs through the cluster stack.**
   `ClusterCoordinator → DistributedBackend` (`cluster.NewDistributedBackendForGroup`,
   `boot_phases_storage_runtime.go:353`) is the only backend wired during startup.
   Cluster metadata persists as data-group raft FSM materialized values sealed by
   `FSM.sealValue` via the DEK adapter when present. Even the single-node case routes
   through one-node raft.

4. **The misclassification has already caused churn.** The TODOS entry
   "Remove dead `encrypted_badger.go` / `storage.LocalBackend`" read as a "surgical
   dead-code removal" but it is not — it would touch ~100 files. An earlier at-rest
   unification slice ("Slice 4") was wrongly scoped against `encrypted_badger.go` as
   if it were the live metadata-at-rest path; the live path is the data-group FSM.

The risk this ADR addresses is that a future contributor — without this audit context — adds
a production caller of `LocalBackend`, reintroducing it as a live storage path. Today the
production binary already excludes `LocalBackend` via Go's dead-code elimination (no caller in
`main`); the gap is **intent**, not behavior.

## Decision

1. Add a **file-header marker comment** to the four files that constitute the fixture
   (`local.go`, `multipart.go`, `append.go`, `encrypted_badger.go`), separated from
   `package storage` by a blank line so the marker is a regular file-header comment and
   does **not** attach as the package doc-comment (which would mislead readers of `go doc`
   / `pkg.go.dev`, since `internal/storage` contains many production symbols).

2. Add a **CI-gateable production-caller assertion** to the verification block of this PR
   and to future verification of structurally adjacent work:

   ```bash
   hits=$(grep -rE 'storage\.[A-Za-z_]*LocalBackend' --include='*.go' cmd/ internal/ \
     | grep -v '_test.go' | grep -v 'internal/storage/')
   [ -z "$hits" ] || { echo "FAIL: production caller of LocalBackend found: $hits"; exit 1; }
   ```

   The assertion is the only mechanism keeping the marker honest; promoting it to a `make`
   target or GitHub Actions step is captured as a follow-up.

3. Resolve the misleading TODOS bullets: drop the stale DEK→XAES item (shipped v0.0.379.0)
   and drop the misleading dead-code removal item (replaced by this ADR + the marker).

## Consequences

- **Reviewers can flag a new production import of `LocalBackend` in code review** with this
  ADR as the citation.
- **Production binary unchanged.** Dead-code elimination already excludes `LocalBackend` from
  the linked binary; this PR adds no runtime code.
- **Future restructure remains open.** If a structural need arises (e.g., a contributor
  proposes a real production caller, or test-fixture isolation becomes a hard requirement),
  `LocalBackend` and friends can be moved to an `internal/storage/testlocal/` package — see
  "Alternatives Considered" §A.
- **The grep assertion must remain truthful.** If `LocalBackend` ever acquires an alias or a
  package-relocation, the assertion must be updated in lockstep. If it is removed without a
  replacement, the marker is no longer enforceable.

## Alternatives Considered

- **A) Move to `internal/storage/testlocal/`.** Strong, compile-unit isolation: the type
  cannot be imported by a production package even by mistake. Rejected: ~100-file import
  rewrite plus a same-package unexported-access export pass. The marginal isolation does not
  justify the cost while no production caller threat is concrete.
- **B) `//go:build testfixture` tag.** Compile-time exclusion from production builds.
  Rejected: not an idiomatic Go pattern, fragile to a caller that forgets the tag, and
  introduces `Makefile` complexity.
- **D) CI-only lint rule, no in-file marker.** Detection-only, no in-file context for
  future readers. Rejected: half-measure — the marker carries the intent, the lint rule
  carries the enforcement; both have non-zero value, take both.

## Related

- ADR 0001 — Storage Mutation Results Facade (the production `Operations` decorator pattern).
- PR #581 (`64c0f6ba` v0.0.379.0) — DEK data-sealing cipher → XAES-256-GCM.
- PR #596 (`079561b5` v0.0.393.0) — R1 unification slice (DEK-sealed WAL/packblob/PUT-pipeline).
- TODOS `At-rest unification remainder — static→DEK` (R-FSM/R2/R3) — the slices that
  *do* target the production metadata-at-rest path.
