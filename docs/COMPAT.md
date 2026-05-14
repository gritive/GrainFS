# GrainFS Rolling Upgrade Compatibility Policy

> **Status:** Slice 1 — CI compat lane shipped

## Policy

GrainFS supports **N → N+1 single-step rolling upgrades** within a minor version boundary.
Version format: `0.0.X.Y` where `X` is the minor segment tracked in `CHANGELOG.md`.

| Direction | Supported? |
|-----------|-----------|
| N → N+1 rolling upgrade | ✅ yes |
| N+1 → N downgrade | ❌ no |
| N → N+2 skip-version upgrade | ❌ no |

During a rolling upgrade, a mixed cluster (some N nodes, some N+1 nodes) must handle
S3 reads/writes without data loss or errors.

## Running the Compat Suite

```bash
# Build current binary first, then run compat tests
make test-compat

# With a specific previous-version binary:
COMPAT_PREV_BIN=/path/to/grainfs-prev make test-compat
```

Without `COMPAT_PREV_BIN`, tests that require a previous binary are **skipped** (not failed).
The CI compat lane is meant to be wired to a `COMPAT_PREV_BIN` pointing at the last released binary.

## Version Detection

`tests/compat/prevtag_test.go` parses `CHANGELOG.md` to determine the previous version.
`CHANGELOG.md` format: `## [0.0.X.Y] - date — description`

The second entry is the previous version. If parsing fails or only one version exists,
`prevVersion()` returns `""`.

## Scenarios

| # | Go test function | Description | Status |
|---|-----------------|-------------|--------|
| 1 | `TestForwardRead` | N-1 writes data; N reads it back after in-place restart | live |
| 2 | `TestMixedClusterRolling` | 2-node cluster: node 0 = N-1, node 1 = N; write on N-1, read on N | live |
| 3 | `TestSnapshotForwardCompat` | N-1 creates snapshot; N restores it; data intact | live |
| 5 | `TestInstallSnapshotPath` | N-1 cluster runs; N node joins and receives InstallSnapshot RPC | live |
| 6 | `TestRestartToOlderBinary` | Canary: documents behavior when N-1 binary reads N-format data | live |
| 7 | `TestHeadSnapshotReject` | HEAD-format snapshot rejected by older binary via version header | stubbed (Slice 3) |

> Scenario 4 (FSM divergence detection via StateHash) is deferred to a separate PR (Slice 2+).

## Developer Guide

### When to add a compat test

Add a compat test whenever you change any of the following in a way that existing data may be affected:

- BadgerDB key/value schemas (`internal/badgerrole`, `internal/cluster/meta_fsm.go`)
- Raft log entry encoding (FlatBuffers schemas in `internal/**/*.fbs`)
- Snapshot format (`internal/snapshot`)
- S3/IAM wire protocol

### How to add a test

1. Add a new `Test*` function to `tests/compat/scenario_*.go` (with `//go:build compat`)
2. Call `prevBinary(t)` at the top — this skips the test if `COMPAT_PREV_BIN` is not set
3. Use `startCompatCluster` or `startGrainfsNode` for process management
4. Commit the test **before** merging the breaking change

### What "forward-compat" means

A new binary (N) **must** be able to read and operate on data written by the previous binary (N-1).
It does **not** need to write data readable by N-1 (downgrade is unsupported).

### Rolling upgrade procedure (production)

1. Build new binary and place it on each host
2. For each node (one at a time):
   a. `SIGTERM` the current node process
   b. Start the new binary in-place (same `--data` dir)
   c. Wait for the node to rejoin the cluster and catch up via Raft
   d. Verify S3 health before proceeding to the next node
3. All nodes upgraded? Run smoke test to confirm cluster health.
