# ADR 0008: Drop --access-key/--secret-key flags — IAM-only auth

Status: Accepted (2026-05-08)
Supersedes: ADR 0007 (IAM Foundation) — flag-mode bootstrap path
Related: ADR 0006 (admin UDS / dashboard HTTP), ADR 0007 (IAM Foundation)

## Context

ADR 0007 introduced the IAM Foundation (ServiceAccount + AccessKey +
Grant) and added a transitional `--access-key`/`--secret-key` bootstrap
shim on `grainfs serve`: when the flags were set on an empty IAM store,
the leader auto-created a `sa-default` SA, a wildcard Admin grant, and
flipped a sticky `auth_enabled` bit. Once the bit was on, IAM was the
sole authority; the flags became a redundant front door whose only
remaining purpose was the first-time bootstrap.

The shim carried three roles that have all been superseded:

1. **Static creds bootstrap** — covered by IAM AccessKey now.
2. **IAM SA bootstrap** — admin UDS already exposes
   `POST /v1/iam/sa`, which is the canonical bootstrap path for every
   subsequent SA. There is no reason the *first* SA must use a different
   path.
3. **NoAuth signal** — anonymous mode was always a development-only
   foot-gun. The sticky bit prevented regression once any SA existed,
   so the bit's only real job was guarding against an empty store.

Keeping the flags also kept three pieces of dead complexity alive: a
goroutine in `serveruntime/run.go` that polled for IAM-store empty +
flag-set + leader, an `if authEnabled` branch in every authz layer,
and the snapshot's `authBit` byte. Each of those had at least one
issue: the goroutine had a known race window between leader election
and propose, the authz branches doubled the test matrix, and the
authBit prevented snapshot evolution.

## Decision

Remove the `--access-key`/`--secret-key` flags from `grainfs serve` and
route every bootstrap through the admin UDS:

- **First-SA bootstrap is atomic.** New `MetaCmdType.IAMInitFirstSA`
  (= 31) carries a composite `InitFirstSAPayload` that wraps the
  `SACreatePayload` + `KeyCreatePayload` + `GrantWildcardPutPayload`
  blobs. A single FSM Apply commits all three records. Partial state
  on FSM crash is impossible.
- **Race guard is `DefaultSAID = "sa-default"`.** Concurrent admin
  UDS callers on an empty cluster both propose with the same fixed
  `sa_id`. Raft FIFO serializes; the second Apply sees the SA already
  exists and idempotent-skips. The losing admin API handler detects
  the race by re-reading the store post-Apply and returns
  `409 Conflict` with a message pointing at the regular non-bootstrap
  path.
- **Sticky `auth_enabled` bit is removed.** `authzMiddleware` always
  evaluates the IAM Layer. Anonymous mode is gone. Pre-bootstrap S3
  traffic returns 401.
- **Snapshot bumped to v3.** The `authBit` byte is dropped from the
  binary layout. v1/v2 readers are not supported (pre-1.0 product, no
  migration path).
- **Admin UDS perm model is unchanged.** The socket is already
  hard-failed at `chmod 0660` with optional `--admin-group` chown
  (ADR 0006). Multi-operator setups remain supported.
- **Out-of-scope flags are kept.** `--upstream-access-key`,
  `--src-access-key`, `--dst-access-key` continue to exist; they
  authenticate to *external* S3 endpoints and have nothing to do with
  the `GrainFS` data-plane auth.

E2E coverage:

- All 14+ existing e2e tests using inline `--access-key`/`--secret-key`
  migrated to a `bootstrapAdminViaUDS()` helper that POSTs to
  `/v1/iam/sa` and returns the credential pair.
- New `iam_bootstrap_test.go` covers F1-F4: first-SA wildcard grant,
  second-SA no auto-grant, pre-bootstrap S3 returns 401,
  post-bootstrap S3 verbs work end-to-end.

## Consequences

Positive:

- One bootstrap path. The bootstrap state machine is `IAMInitFirstSA`
  or `SACreate` + `KeyCreate`, and the dispatch is a one-line check
  against `Store.IsEmpty()`.
- No anonymous mode means one fewer security failure mode to audit.
  The threat model collapses to "is the admin UDS reachable" + "is the
  IAM grant correct".
- Snapshot v3 is one byte smaller per snapshot and easier to evolve;
  the conditional `authBit` reader path is gone.
- The bootstrap shim goroutine and its leader-election race are gone.

Negative:

- BREAKING for anyone running anonymous mode (no SA, sticky=false).
  All S3 traffic 401s after upgrade until an operator runs
  `grainfs iam sa create admin --endpoint <data>/admin.sock`. Per the
  pre-1.0 status, no migration tooling is shipped; the CHANGELOG
  documents the manual step.
- Operators must understand the admin UDS perm model up front. ADR
  0006 already required this for `grainfs cluster ...`, but the
  product previously let users skip it for the IAM-less case.
- `IAMInitFirstSA` introduces a new MetaCmdType value that older
  binaries cannot replay. Mixed-version clusters must finish the
  rolling upgrade before any operator runs the first `sa create`.

## Alternatives Considered

**Keep the flag but mark deprecated.** Rejected. Deprecation prolongs
dual-path complexity without buying anything: the admin UDS path
already worked, and operators only had to call it once per cluster.

**Auto-generate first admin SA at startup, log secret to stderr.**
Rejected. Stderr leaks to journald, syslog, container logs, and
process supervisor capture. Secrets in logs is the textbook way to
turn a one-time credential into a persistent compromise.

**Keep sticky bit, just stop checking it.** Rejected. Dead code in a
security-critical path is worse than absent code; future readers
would have to repeatedly determine that the bit is in fact unused.

## Implementation references

- Spec: `docs/superpowers/specs/2026-05-08-drop-access-key-flag.md`
- Plan: `docs/superpowers/plans/2026-05-08-drop-access-key-flag.md`
- Code: `internal/iam/init_first_sa.go`,
  `internal/iam/admin_api.go` (HandleSACreate dispatch),
  `internal/iam/snapshot.go` (v3 layout),
  `internal/iam/iampb/iam.fbs` (`InitFirstSAPayload`),
  `internal/cluster/clusterpb/cluster.fbs`
  (`MetaCmdType.IAMInitFirstSA = 31`),
  `cmd/grainfs/serve.go` (flags removed),
  `internal/serveruntime/{config,run}.go` (Bootstrap fields and
  shim goroutine removed),
  `tests/e2e/iam_bootstrap_test.go` (F1-F4 acceptance),
  `tests/e2e/iam_helpers_test.go` (`bootstrapAdminViaUDS`).

## See Also

- ADR 0006 — admin UDS contract that this ADR makes the sole bootstrap
  path.
- ADR 0007 — IAM Foundation. This ADR supersedes 0007's bootstrap-shim
  + sticky-bit decisions; the SA / AccessKey / Grant model is
  otherwise unchanged.
