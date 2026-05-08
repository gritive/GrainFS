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

## What's here vs what's coming

**In PR 1:**
- Actor goroutine skeleton (`actor.go`) — command loop, `handle` dispatch, `handlePropose`
- `readState` + `actorState` structs (`state.go`), `publish`/`snapshot`
- `Node` struct, `NewNode`/`Start`/`Stop` lifecycle (`node.go`)
- `Propose` (fire-and-forget) + `ProposeWait` (ctx-aware blocking) (`propose.go`)
- Single-voter auto-leader bootstrap (Peers empty → Leader at term 1)
- Read-mostly methods: `State`, `Term`, `IsLeader`, `LeaderID`, `CommittedIndex`, `ApplyCh`
- Single-node round-trip test (`node_test.go`)

**Not yet (per plan):**
- Election timer, vote RPC, AppendEntries — PR 4-5
- Persistence (LogStore adapter) — PR 6+
- Snapshots — PR 6+
- Configuration changes / joint consensus — PR 10+
- Equivalence harness skeleton — PR 2-3
- Full property tests + chaos port — M3
- Per-package import flip — M5

## Caller migration (deferred)

Callers in `internal/cluster` and `internal/serveruntime` continue to
import `internal/raft`. They are not touched until M5, where a
per-package phased flip lands: `cluster` first, then `serveruntime`,
then remaining packages, and finally `internal/raft` (v1) is deleted and
the `v2` import path is renamed to take its place. Until then, any
changes to the external API in v2 are independent of v1 and do not
affect production.

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
