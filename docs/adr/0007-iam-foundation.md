# ADR 0007: IAM Foundation (ServiceAccount + Bucket Grants)

Status: Accepted (2026-05-08)
Supersedes: —
Related: ADR 0006 (CLI uses admin UDS, dashboard uses HTTP)

## Context

GrainFS originally accepted a single `--access-key/--secret-key` pair on
the `serve` CLI and gated all S3 traffic on that one credential. That
shape is sufficient for a single-tenant deployment but cannot satisfy
four downstream features that share the same primitive: per-bucket
quotas, multi-team buckets, audit attribution, and bucket policies that
allow/deny per principal. Each of those features needs a stable
"principal" identity, scoped credentials, and a way to revoke a single
team's access without rotating the cluster-wide key.

We considered two alternative shapes (see "Alternatives Considered"
below). Both required either re-encoding state on every change or
shipping a partial model that downstream features would have to redo.
The accepted approach lands the full ServiceAccount + AccessKey + Grant
model in one PR so all four downstream features build on the same
primitive.

The full design narrative — including invariants, threat model, snapshot
format, and a per-decision Open Question log — lives at
`docs/superpowers/specs/2026-05-08-iam-foundation-design.md`.

## Decision

We added an in-memory IAM store backed by the cluster meta-Raft FSM:

- **ServiceAccount** — opaque principal with UUID v7 id and admin-supplied
  name. Created/deleted via raft-replicated `MetaCmdType` 21/22.
- **AccessKey** — SigV4 credential pair scoped to one SA. Plaintext
  secret_key never touches disk; only the AES-256-GCM ciphertext is
  persisted, bound to the SA id via AAD. Rotated/revoked via raft
  (`MetaCmdType` 23/24). Status flips active → revoked but the row stays
  in the store so audit trail and snapshot restore remain consistent.
- **Grant** — per-(SA, bucket) role assignment. Three roles (Read /
  Write / Admin) cover the matrix in `internal/iam/types.go`'s
  `RoleAllows`. Wildcard grants (`Bucket = "*"`) exist for one purpose
  only: the bootstrap default SA's cluster-wide Admin. The admin API
  rejects wildcard grants on any other SA.
- **Sticky `auth_enabled` bit** — once any SA is registered, the bit
  flips on permanently. Deleting all SAs does NOT clear it. This
  prevents accidental regression to anonymous mode in production.
- **Bootstrap shim** — on a startup with `--access-key/--secret-key`
  flags AND an empty IAM store, the leader proposes a default SA
  (`sa-default`) + wildcard Admin grant + AuthEnable. Idempotent on the
  apply path, so follower replays and restart races converge.

State organization:

- `iam.Store` keeps a COW-projected `iamState` behind
  `atomic.Pointer[iamState]`. Read-only callers (SigV4 verifier, authz
  middleware) take the pointer once and walk it lock-free; writers
  serialize through `mu`, build a new state copy, and CAS-publish.
- The canonical write path is `iam.Applier`, called from the meta-FSM
  apply loop. The same `Applier` runs during snapshot restore.

Audit:

- `iam.AuditLogger` wraps a pluggable `AuditEmitter`. The default
  emitter is a zerolog wrapper (`event=iam_audit`); operators can
  swap in alternative emitters (e.g., a future incident store) without
  touching the authz middleware.

CLI / admin surface:

- The admin HTTP API mounts under `/v1/iam/*` on the **admin UDS** (per
  ADR 0006). It deliberately does NOT live on the data-plane HTTP port,
  so a misconfigured firewall cannot expose IAM mutation endpoints to
  the internet.
- The `grainfs iam` CLI subcommand mirrors the cluster CLI's pattern:
  `--endpoint <data-dir>/admin.sock`, fail-fast on `http://`/`https://`
  schemes, raw JSON pass-through output for piping.

## Consequences

Positive:

- Four downstream features (per-bucket quotas, multi-team buckets,
  audit attribution, principal-aware bucket policies) can land
  independently on the same primitive without each rebuilding it.
- Single PR review cost: one design doc + one plan + 32 task commits in
  five phases, instead of N partial migrations spread over N quarters.
- Snapshot v1 includes a dedicated revoked-AKs section so revocation
  survives raft snapshot/restore cycles. Plaintext secret never enters
  the snapshot; restore re-derives it from the wrapped ciphertext.

Negative:

- Hot-path SigV4 verify dereferences an `atomic.Pointer` and walks the
  in-memory map every request. Measured no regression on the existing
  `s3auth.CachingVerifier` benchmark (signing key cache amortizes the
  4-round HMAC derivation), but worth re-measuring at >10k SAs.
- Decrypted secret_key plaintext lives in memory for the lifetime of
  the process. We accepted this over the alternative (decrypt per
  request) because secret_key is also held by every SigV4 client; an
  attacker with process memory access already has bigger problems.
- The wildcard-grant escape hatch on bootstrap is a "policy hole" by
  construction. It is gated to one SA id (`sa-default`) at the admin
  API layer.

## Alternatives Considered

**Approach B: layered shipping.** Ship just SA + key in v1, defer
grants to v2. Rejected because the four downstream features all need
grants; deferring means each feature would mock or re-design grants.

**Approach C: bucket-attached grants.** Store grants as part of bucket
metadata instead of a separate `grants` map. Rejected because grant
queries are SA-scoped (an admin asks "what does alice have access to?")
much more often than bucket-scoped, and bucket metadata changes at a
different rate from grants.

**Approach D: external IAM (Keycloak / Auth0).** Rejected for v1
because GrainFS is a single-binary product; introducing an external
dependency for first-launch IAM would push deployments toward managed
infra. Future work can add SAML/OIDC bridging on top of the SA model.

## Implementation references

- Design doc: `docs/superpowers/specs/2026-05-08-iam-foundation-design.md`
- Plan: `docs/superpowers/plans/2026-05-08-iam-foundation.md`
- Code: `internal/iam/`, `internal/iam/iampb/`,
  `internal/cluster/clusterpb/cluster.fbs` (MetaCmdType 21..28),
  `internal/serveruntime/iam_admin.go`, `cmd/grainfs/iam.go`.

## See Also

- ADR 0006 — admin UDS / dashboard HTTP separation. IAM admin endpoints
  follow the same UDS-only contract.
