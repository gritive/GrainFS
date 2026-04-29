# Raft Membership Change — §4.4 Violations in Current Code

This document records the three §4.4 violations found in the existing
`AddPeer`/`RemovePeer`/`applyConfigChange` implementation before PR-A fixes them.

## Violation 1: Config applied on commit, not on append

**Location:** `raft.go:500-503` (applyLoop `IsConfigChange` block)

Ongaro 2014 §4.4:
> "each server adds the new configuration entry to its log and uses that
> configuration *immediately*, regardless of whether the entry has been committed"

The existing code calls `applyConfigChange` inside `applyLoop`, which runs only
after `lastApplied` advances past `commitIndex`. At that point the entry is already
committed and replicated. This violates the invariant that quorum calculations must
switch to the new config the moment the entry is **appended to the log**, not when
it is committed.

**Why it matters:** If a leader proposes `AddVoter(D)` to a 3-node cluster {A,B,C},
the entry must be appended with the new quorum of 3-out-of-4 taking effect
immediately. If the leader only switches after commit, it calculates quorum using the
old 3-node set while D might already be included in the new set — a split-brain
window exists until the entry commits.

**Fix (PR-A):**
- Remove the `IsConfigChange` block from `applyLoop` entirely.
- Call the new `applyConfigChangeLocked` function from:
  - `HandleAppendEntries`: after each new entry is appended to the log.
  - `flushBatch`: after the leader appends entries to its own log.

## Violation 2: No concurrent-change guard

**Location:** `AddPeer` / `RemovePeer` — no `pendingConfChangeIndex` field

Ongaro §4.4: "there must be at most one outstanding (uncommitted) configuration
change at a time."

The existing code has no guard. Two concurrent `AddPeer` calls can each get through
`Propose`, resulting in two ConfChange entries both in-flight. Depending on timing,
the second entry can be appended before the first is committed, causing the cluster
to believe it has two simultaneous config transitions in progress.

**Fix (PR-A):**
- Add `pendingConfChangeIndex uint64` field to `Node` (0 = none).
- `AddVoter`/`RemoveVoter`/`AddLearner`/`PromoteToVoter` check `pendingConfChangeIndex != 0`
  and return `ErrConfChangeInProgress` if true.
- Set `pendingConfChangeIndex = entry.Index` when the ConfChange entry is appended.
- Clear `pendingConfChangeIndex = 0` when that index is committed in `applyLoop`.

## Violation 3: Snapshot does not preserve configuration

**Location:** `HandleInstallSnapshot` (`raft.go:1473-1521`) and `InstallSnapshotArgs`

`InstallSnapshotArgs` has no `Servers` field. When a lagging follower receives a
snapshot and discards its entire log (`n.log = nil`), the config changes that were
captured in those log entries are lost. After restart the follower's `config.Peers`
reverts to whatever was in the initial `Config` passed to `NewNode` — typically the
bootstrap peers, missing any members added after bootstrap.

**Fix (PR-A):**
- Add `Servers []Server` to `InstallSnapshotArgs` and `SnapshotMeta`.
- In `replicateTo` (the snapshot-send path), populate `args.Servers = n.config.Peers`
  (wrapped as `[]Server`).
- In `HandleInstallSnapshot`, restore `n.config.Peers` from `args.Servers` after
  discarding the log.

---

## Tests closed by PR-A

1. `TestConfChange_AppliesOnAppendBothLeaderAndFollower` — verifies config.Peers
   updated on both leader and follower **before** commit (Violations 1).
2. `TestConfChange_RejectsConcurrent` — second ConfChange returns
   `ErrConfChangeInProgress` while first is pending (Violation 2).
3. `TestConfChange_LearnerNotInQuorum` — learner added via `AddLearner` is excluded
   from quorum calculation.
4. `TestConfChange_SnapshotPreservesConfig` — config survives snapshot/restore cycle
   (Violation 3).
5. `TestConfChange_MixedVersionRejected` — cluster refuses ConfChange when a
   mixed-version peer (without `LogEntry.Type` support) is detected.
