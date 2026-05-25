# Rolling Upgrade Slice 4: Finalize Machinery Design

> **Status:** Design document (pre-implementation)
> **Parent:** Slice 1 shipped CI compat lane (`tests/compat/`)
> **Scope:** finalize command, StateHash FSM divergence detection, snapshot version headers

## Motivation

Slice 1 ships the compat test lane and a forward-read/mixed-cluster safety net.
Slice 4 closes the remaining gaps needed for production-grade rolling upgrades:

1. **`upgrade finalize` command**: operator gate that confirms all nodes are on N+1
2. **StateHash**: Raft FSM state fingerprint for divergence detection (Scenario 4)
3. **Snapshot version header**: prevents older binaries from restoring incompatible snapshots (Scenario 7)
4. **Drain/rollback procedure**: runbook for partial upgrade rollback

---

## Feature 1: `upgrade finalize` Command

### Problem

After a rolling upgrade completes, there is no operator-visible signal that all nodes have
successfully upgraded and the cluster is "clean". Operators must check individually.

### Design

Add a new CLI subcommand `grainfs upgrade finalize` that:

1. Contacts every peer via the admin API to query its binary version
2. Asserts all nodes report the same minor version (`0.0.X.*`)
3. If unanimous: writes a `ClusterVersionFinalized` Raft log entry (no-op FSM action,
   but visible in audit log)
4. Returns 0 on success, non-zero with a detailed error on failure

```
$ grainfs upgrade finalize --data ./data --endpoint http://127.0.0.1:9000
Checking 3 nodes...
  n1: 0.0.189.0 ✓
  n2: 0.0.189.0 ✓
  n3: 0.0.189.0 ✓
Upgrade finalized at version 0.0.189.0
```

### Admin API additions needed

`GET /admin/version` returns `{"version":"0.0.189.0","node_id":"n1"}` over admin UDS.

### Raft log entry

```go
// FinalizeMigration records that all nodes have converged to the same version.
// It is a no-op from the FSM's perspective but creates an audit trail.
type FinalizeMigration struct {
    Version string
    At      time.Time
}
```

### Rollout gates (future)

`finalize` can optionally reject if any node reports an error or lagging Raft log.
A `--min-apply-lag 0` flag would enforce this.

---

## Feature 2: StateHash for FSM Divergence Detection (Scenario 4)

### Problem

If two nodes apply the same Raft entries but produce different FSM states (e.g., due to
non-deterministic map iteration), silent divergence goes undetected until a read diverges.

### Design

After each snapshot installation and at regular heartbeat intervals, each node computes
a `StateHash` of its FSM state and gossips it via Raft heartbeat extensions.

#### Hash computation

The `MetaFSM` exposes a `StateHash() (string, error)` method:

```go
func (f *MetaFSM) StateHash() (string, error) {
    h := sha256.New()
    // Sort-then-hash each sub-state:
    if err := hashBuckets(h, f.buckets); err != nil { return "", err }
    if err := hashObjects(h, f.objectLatest); err != nil { return "", err }
    if err := hashIAM(h, f.iamStore); err != nil { return "", err }
    // ... other stable sub-states
    return hex.EncodeToString(h.Sum(nil)), nil
}
```

Each `hash*` function iterates its keys in sorted order to ensure determinism.

#### Deferral Reason

- `MetaFSM` fields (`buckets`, `objectLatest`, etc.) use `map[string]T`, so the
  implementation must sort keys for every stable field.
- The hash code must exclude ephemeral fields such as `activePlan` and
  `loadSnapshot`.
- Requires a complete inventory of "stable" vs "ephemeral" FSM fields first.
- A bad hash implementation would produce false positives, disrupting healthy clusters.

#### Detection + alerting

After a node detects a StateHash mismatch via gossip:
1. Logs `WARN fsm_divergence node_id=X expected=<hash> got=<hash>`
2. Emits a `grainfs_fsm_divergence_total` Prometheus counter
3. Does NOT self-fence (would cause cascading failures)
4. Operator must investigate and potentially do a full snapshot restore

---

## Feature 3: Snapshot Format Header

### Problem

Scenario 7 (`TestHeadSnapshotReject`) proves that an older binary must not silently
restore a snapshot written by a newer binary. The risk: an N+1 snapshot contains fields
unknown to N, and N silently ignores them, causing data loss on restore.

### Design

New snapshots use a fixed binary envelope before a zstd-compressed JSON payload:

| Field | Encoding |
|-------|----------|
| magic | 8-byte ASCII `GFSNAP01` |
| min_reader_format | `uint32` big-endian |
| writer_format | `uint32` big-endian |
| written_at_unix_nano | `int64` big-endian |
| payload | zstd-compressed JSON snapshot |

Snapshot format integers control compatibility, not `GrainFS` binary semver.
The current writer and reader format are both `1`.

Restore path:

```go
func readSnapshotFromReader(r io.Reader) (*Snapshot, error) {
    br := bufio.NewReader(r)
    prefix, err := br.Peek(2)
    if err != nil {
        return nil, ErrUnsupportedSnapshotFormat
    }
    if bytes.Equal(prefix, []byte{0x1f, 0x8b}) {
        return nil, ErrUnsupportedSnapshotFormat
    }

    header, err := br.Peek(snapshotHeaderLen)
    if err != nil {
        return nil, ErrUnsupportedSnapshotFormat
    }
    if !bytes.Equal(header[:8], snapshotMagic[:]) {
        return nil, ErrUnsupportedSnapshotFormat
    }
    minReader := binary.BigEndian.Uint32(header[8:12])
    if minReader > currentSnapshotReaderFormat {
        return nil, ErrUnsupportedSnapshotFormat
    }
    br.Discard(snapshotHeaderLen)
    return decodeSnapshotZstd(br)
}
```

If `min_reader_format` exceeds the current reader format, restore fails with
`ErrUnsupportedSnapshotFormat` before any backend mutation. The admin restore API maps
that error to `409 Conflict` with an operator-readable hint.

The reader also rejects unknown envelopes. It detects legacy gzip-only snapshots
by gzip magic before header parsing and returns `ErrUnsupportedSnapshotFormat`.

### Rollout

The first binary that includes this feature writes the envelope.
Old binaries do not write the envelope, and current binaries reject
gzip-only legacy snapshots after the zstd conversion. Old binaries presented with
an envelope fail gzip parsing and return non-200 from restore; this is intentional
because `GrainFS` does not support downgrade restore.

---

## Feature 4: Drain/Rollback Procedure

### Problem

If an upgrade is partially complete (e.g., 2 of 3 nodes upgraded) and a critical bug is
discovered in the new binary, the operator needs a safe rollback path.

### Procedure

**Partial rollback (mixed cluster → all old)**

1. Stop all N+1 nodes
2. Restart each stopped N+1 node with the N binary. It replays Raft entries
   written by N+1 nodes; compat tests must enforce N-compatible forward entries.
3. If any node fails to restart: restore from the last N-era snapshot

**Full rollback (all N+1 → all N)**

Safe only if the Raft log contains no N+1-only data format.
The `upgrade finalize` command (Feature 1) deliberately does NOT write any incompatible
entries, so rollback from a finalized N+1 cluster is still possible if N can read N+1 data.

**Rollback gate**

`grainfs upgrade rollback --to-version 0.0.188.0` (future):
1. Checks all nodes still have N-era data (no irrecoverable N+1 entries)
2. If safe: prints rollback steps
3. If rollback is unsafe: print the N+1-only entries and why they block rollback.

---

## Implementation Order (Slice 4)

| Step | Task | Complexity |
|------|------|------------|
| 4.1 | `GET /admin/version` endpoint | Low |
| 4.2 | `grainfs upgrade finalize` CLI + test | Medium |
| 4.3 | `GFSNAP01` snapshot envelope + write path | Medium |
| 4.4 | Restore-path format check | Low |
| 4.5 | Enable `TestHeadSnapshotReject` (remove t.Skip) | Low |
| 4.6 | `MetaFSM.StateHash()` implementation | High |
| 4.7 | StateHash gossip + divergence detection | High |
| 4.8 | Drain/rollback CLI skeleton | Medium |

---

## Open Questions

1. **StateHash scope**: which ephemeral fields should the hash exclude?
   They affect cluster behavior but may differ legitimately between nodes.
2. **Hash frequency**: per-snapshot-install only, or also at Raft heartbeat (expensive)?
3. **min_reader_format policy**: do we bump it on every breaking snapshot payload change,
   or only on major format revisions?
4. **Rollback support window**: how many previous versions should support rollback?
   Current proposal: N-1 only (matches the compat test policy).
