## Status

This package is **M1 PR 1** of a multi-phase rewrite documented in
`../../docs/superpowers/plans/2026-05-08-raft-actor-redesign.md`. It is
**not for production use**. The existing `internal/raft` package remains
canonical; callers are not migrated until M5 (~month 5-6). The plan
covers ~28 PRs across 5 milestones; this PR proves the actor model
compiles and a single `Propose` round-trips end-to-end.

## Why this exists

`internal/raft/raft.go` protects its state with a single `n.mu`
`sync.RWMutex` that touches roughly 88 public methods. The goal is to
replace that with a single-goroutine actor + channel design so state
transitions are easier to reason about and audit. Throughput is
explicitly not the goal: a 2026-05-08 measurement (PR #240) confirmed
that removing `n.mu` left throughput neutral — mutex contention is a
symptom, not the binding cost.

## Model rationale

Hot-path reads (`State`, `Term`, `IsLeader`, `LeaderID`,
`CommittedIndex`) are called at 100+ sites in the cluster stack. A
channel round-trip per call (~µs) would be a measurable regression
relative to a `RLock`/`RUnlock` pair (~ns). The solution — borrowed
from etcd-io/raft's `SoftState` publication pattern — is an
`atomic.Pointer[readState]` that the actor goroutine updates after every
state transition:

```go
// After every mutation the actor calls publish():
func (n *Node) publish() {
    n.rs.Store(n.st.snapshot()) // atomic; readers see a coherent snapshot
}

// Hot-path public method — no channel:
func (n *Node) IsLeader() bool { return n.rs.Load().isLeader }

// Mutating method — sends a command and waits:
func (n *Node) ProposeWait(ctx context.Context, cmd []byte) (uint64, error) {
    reply := make(chan proposalResult, 1)
    n.cmdCh <- command{kind: cmdPropose, proposeCommand: cmd, proposeReply: reply}
    res := <-reply
    return res.index, res.err
}

// Fire-and-forget variant — enqueues on cmdCh and returns:
func (n *Node) Propose(cmd []byte) error {
    n.cmdCh <- command{kind: cmdPropose, proposeCommand: cmd}
    return nil
}
```

The actor goroutine (`actor.go::run`) is the only writer of `actorState`
and the only caller of `publish`. Readers never touch `actorState`.

## Capability matrix (M1 final state + M2 prep fixes)

| Capability | Status | PR |
|---|---|---|
| Single-voter Propose round-trip | ✅ | PR 1 |
| Equivalence harness (v1 ↔ v2) | ✅ | PR 2-3 |
| Inbound RequestVote (Raft §5.4) | ✅ | PR 4 |
| Inbound AppendEntries (heartbeat path) | ✅ | PR 5a |
| Election state machine + heartbeat | ✅ | PR 5a |
| Multi-voter election (3+ nodes) | ✅ | PR 5a-5b |
| 3-voter election equivalence | ✅ | PR 5b |
| Log replication (happy path) | ✅ | PR 6a |
| Log replication 3-voter equivalence | ✅ | PR 6a |
| Log conflict handling + truncation | ✅ | PR 6b |
| Conflict-term hint backoff (§5.3 optimization) | ✅ | PR 6b |
| Bootstrap / Configuration() (read API) | ✅ | PR 7 |
| Leader no-op on election (§5.4.2) | ✅ | PR 8 |
| MaxEntriesPerAE batch cap | ✅ | PR 8 |
| Log Matching e.Index validation | ✅ | PR 8 |
| Conflict-hint O(log N) binary search | ✅ | PR 8 |
| Membership change (AddVoter / RemoveVoter / etc.) | ⏳ stub | M2 |
| LogStore interface (in-memory backing) | ✅ | PR 9 |
| LogStore persistence (BadgerDB backing) | ✅ | PR 10 |
| Crash-recovery wiring (LogStore → replay) | ⏳ | PR 11 |
| Snapshots | ⏳ | M2 |
| Joint consensus (§4.3) | ⏳ | M2 |
| ReadIndex linearizable reads | ⏳ | M2 |
| Property-based tests + chaos suite | ⏳ | M3 |
| Production caller migration | ⏳ | M5 |

### FSM consumer note on LogEntryNoOp

`becomeLeader` appends a `LogEntryNoOp` entry (type=3, Command=nil) immediately
on election (Raft §5.4.2). This entry is delivered on `ApplyCh` like any other
committed entry. **FSM consumers must check `e.Type == LogEntryNoOp` and skip
these entries** — they carry no application payload. In a 3-voter cluster, the
first entry applied after election is always the no-op at index 1; the first
user-visible `ProposeWait` return value is therefore index 2.

## Caller migration (deferred)

Callers in `internal/cluster` and `internal/serveruntime` continue to
import `internal/raft`. They are not touched until M5, where a
per-package phased flip lands: `cluster` first, then `serveruntime`,
then remaining packages, and finally `internal/raft` (v1) is deleted and
the `v2` import path is renamed to take its place. Until then, any
changes to the external API in v2 are independent of v1 and do not
affect production.

## LogStore

`LogStore` (defined in `logstore.go`) is the interface through which the actor
goroutine reads and writes the Raft log. The current implementation is
`memLogStore` — an in-memory slice owned exclusively by the actor goroutine
(no locking needed). PR 10 adds `badgerLogStore` (in `logstore_badger.go`),
a durable implementation backed by BadgerDB. It uses a prefix-scoped key
layout (`prefix || be64(idx)`) so multiple Raft groups can share one DB, and
encodes each entry as a compact 21-byte binary header plus payload (no JSON).
`badgerLogStore` is not yet wired into `NewNode`; `memLogStore` remains the
default until PR 11 adds the config field and crash-recovery replay from disk.
PR 11 wires crash-recovery (replay from `applied+1` to `commitIndex` on
restart). Callers of `Node` do not interact with `LogStore` directly; it is an
implementation detail of the actor.

## Read this if you're touching this package

- The actor goroutine in `actor.go::run()` is the **sole writer** of
  `actorState`. Public methods **must not** read or write `actorState`
  directly. They either send a command on `cmdCh` (mutating) or call
  `rs.Load()` (read-only).

- Multi-field reads across two separate `rs.Load()` calls are **not
  atomic**. `if n.IsLeader() { x := n.Term() }` sees two independent
  snapshots that may differ. Callers needing a coherent multi-field view
  must load once: `snap := n.rs.Load(); use snap.isLeader, snap.term`.
  The 88-method classification audit (in progress during M1) identifies
  which callers need composite snapshots.

- On `Stop`, `applyCh` is closed before `doneCh`. A consumer using
  `for range n.ApplyCh()` will see the channel close before `Stop()`
  returns — no additional synchronisation is needed on the consumer side.

- Test scope is single-node `Propose` round-trip only. Channel-
  correctness corner cases (shutdown races, backpressure, post-stop
  calls, context cancellation paths) are deferred to the M2-M3 chaos
  suite per plan D6.

- After `Start()` the actor goroutine schedules and publishes the
  bootstrap `readState` asynchronously. A caller doing
  `n.Start(); n.Propose(...)` immediately can race the bootstrap and
  see `ErrNotLeader`. Callers that need the leader transition before
  proposing should poll `n.IsLeader()` or watch `ApplyCh()`. Tests in
  `node_test.go` use a `waitFor` helper.
