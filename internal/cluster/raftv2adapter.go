package cluster

// raftv2adapter.go — thin adapter bridging *raftv2.Node to the RaftNode
// interface defined in raftnode.go.
//
// v1 ↔ v2 name/signature differences resolved here:
//
//   - v1 Close()         → v2 Stop()       : adapter wraps Stop
//   - v1 Snapshot()      : NOT on RaftNode; no caller in cluster uses it
//   - v1 SetTransport(rv,ae func)  → v2 SetTransport(Transport interface)
//                                  : adapter synthesises a v2TransportBridge
//   - v1 SetNoOpCommand  : v2 handles no-ops internally; adapter is a no-op
//   - v1 Peers()         : v2 has no Peers(); derive from Configuration()
//   - v1 WaitApplied     : v2 has no WaitApplied; implemented via polling
//                          CommittedIndex (see method doc)
//   - v1 RegisterObserver/DeregisterObserver : v2 has no observer pattern;
//                          adapter stubs log a once-warning and return
//
// Methods not on the RaftNode interface (v1-only, not called via this path):
//   JointSnapshotState, RestoreJointStateFromSnapshot, CompactLog,
//   SetInstallSnapshotTransport, SetTimeoutNowTransport, HandleRequestVote,
//   HandleAppendEntries, HandleInstallSnapshot, BatchMetrics, …
//
// M4 follow-up membership bridge:
//   AddVoter, AddVoterCtx, RemoveVoter — direct passthrough (v2 has them).
//   TransferLeadership — direct passthrough (Raft §3.10, implemented in
//     v0.0.143.0 / PR #288).
//   AddLearner, PromoteToVoter — passthrough to ErrNotImplemented (deferred
//     beyond M5; v2 has no learner support). Callers receive a clear error
//     instead of a silent skip.
//   ChangeMembership — sequenced bridge (not atomic; see WARN: in method body).
//   PeerMatchIndex — returns (0, false); v2 has no per-peer replication state.
//
// M5 PR 27 follow-up:
//   Handle{RequestVote,AppendEntries,InstallSnapshot,TimeoutNow} — translate v1
//     wire types to v2 native types and dispatch into raftv2.Node. See the
//     dedicated section near the bottom of this file for the two known
//     translation gaps (TimeoutNow.Term, InstallSnapshot.Servers vs Configuration).

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
	raftv2 "github.com/gritive/GrainFS/internal/raft/v2"
)

// raftV2Node wraps *raftv2.Node and exposes the v1-style RaftNode interface.
// It is the ONLY place in internal/cluster that imports internal/raft/v2.
type raftV2Node struct {
	n           *raftv2.Node
	once        sync.Once // for observer no-op warning
	applyOnce   sync.Once // for apply channel bridge goroutine
	applyBridge chan raft.LogEntry

	// bridge holds the v2.Transport adapter created by SetTransport. Stored on
	// the adapter so SetInstallSnapshotTransport can publish the IS callback
	// regardless of whether it is called before or after SetTransport (the
	// callback ultimately lives on the bridge via atomic.Pointer).
	bridge *v2TransportBridge
	// pendingIS holds an IS callback supplied via SetInstallSnapshotTransport
	// before SetTransport built the bridge. SetTransport drains it.
	pendingIS atomic.Pointer[installSnapshotFn]
}

// installSnapshotFn is the outbound v1-style InstallSnapshot send callback.
// Boxed via atomic.Pointer so it can be swapped lock-free.
type installSnapshotFn func(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)

// newRaftV2Node constructs the adapter. Callers must call SetTransport then
// Start before using the node.
func newRaftV2Node(n *raftv2.Node) *raftV2Node {
	return &raftV2Node{n: n}
}

// --- Lifecycle ---

func (a *raftV2Node) Start() { a.n.Start() }

// Close maps to v2's Stop(). v1 callers use Close(); v2's lifecycle method is Stop().
func (a *raftV2Node) Close() {
	metrics.RaftV2StopCount.Inc()
	a.n.Stop()
}

// --- Identity ---

func (a *raftV2Node) ID() string { return a.n.ID() }

// --- State reads ---

// State converts v2.NodeState to raft.NodeState. Both use the same integer
// values (Follower=0, Candidate=1, Leader=2) so a direct cast is safe.
func (a *raftV2Node) State() raft.NodeState { return raft.NodeState(a.n.State()) }

func (a *raftV2Node) Term() uint64           { return a.n.Term() }
func (a *raftV2Node) IsLeader() bool         { return a.n.IsLeader() }
func (a *raftV2Node) LeaderID() string       { return a.n.LeaderID() }
func (a *raftV2Node) CommittedIndex() uint64 { return a.n.CommittedIndex() }

// --- Configuration ---

// Configuration maps v2.Configuration to raft.Configuration. ServerSuffrage
// values are identical (Voter=0, NonVoter=1).
func (a *raftV2Node) Configuration() raft.Configuration {
	v2cfg := a.n.Configuration()
	servers := make([]raft.Server, len(v2cfg.Servers))
	for i, s := range v2cfg.Servers {
		servers[i] = raft.Server{
			ID:       s.ID,
			Suffrage: raft.ServerSuffrage(s.Suffrage),
		}
	}
	return raft.Configuration{Servers: servers}
}

// Peers derives peer addresses from Configuration(), filtering out self.
// v1 Peers() returns peer addresses (equal to IDs in the cluster model used
// by group lifecycle). v2 Configuration().Servers includes self.
func (a *raftV2Node) Peers() []string {
	cfg := a.n.Configuration()
	selfID := a.n.ID()
	peers := make([]string, 0, len(cfg.Servers))
	for _, s := range cfg.Servers {
		if s.ID != selfID {
			peers = append(peers, s.ID)
		}
	}
	return peers
}

// --- Bootstrapping ---

func (a *raftV2Node) Bootstrap() error {
	err := a.n.Bootstrap()
	if err != nil {
		metrics.RaftV2BootstrapOutcome.WithLabelValues("error").Inc()
	} else {
		metrics.RaftV2BootstrapOutcome.WithLabelValues("success").Inc()
	}
	return translateV2SentinelErr(err)
}

// --- Write path ---

func (a *raftV2Node) Propose(command []byte) error {
	return translateV2SentinelErr(a.n.Propose(command))
}

func (a *raftV2Node) ProposeWait(ctx context.Context, command []byte) (uint64, error) {
	start := time.Now()
	idx, err := a.n.ProposeWait(ctx, command)
	outcome := proposeOutcome(err)
	metrics.RaftV2ProposeCount.WithLabelValues(outcome).Inc()
	metrics.RaftV2ProposeLatency.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
	return idx, translateV2SentinelErr(err)
}

// translateV2SentinelErr maps a raftv2 sentinel error to its v1 equivalent so
// callers that use errors.Is(err, raft.ErrNotLeader) (or sibling sentinels)
// continue to match under v2. Errors that have no v1 counterpart (e.g.
// ErrProposalFailed, ErrConfChangeInFlight) pass through unchanged.
//
// PR 30 (v1 deletion) removes this shim; v2's sentinels become canonical.
func translateV2SentinelErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, raftv2.ErrNotLeader):
		return raft.ErrNotLeader
	case errors.Is(err, raftv2.ErrNoPeers):
		return raft.ErrNoPeers
	case errors.Is(err, raftv2.ErrAlreadyBootstrapped):
		return raft.ErrAlreadyBootstrapped
	}
	return err
}

// proposeOutcome maps a ProposeWait error to a bounded outcome label.
func proposeOutcome(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, raftv2.ErrNotLeader) {
		return "not_leader"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	return "error"
}

// --- Read path ---

func (a *raftV2Node) ReadIndex(ctx context.Context) (uint64, error) {
	idx, err := a.n.ReadIndex(ctx)
	return idx, translateV2SentinelErr(err)
}

// WaitApplied blocks until CommittedIndex >= index or ctx is done.
//
// v2 has no direct WaitApplied; we poll CommittedIndex. CommittedIndex
// advancing means the entry is committed, which is a close-enough proxy for
// "applied" in the smoke path (PR 22). A proper apply-notification mechanism
// will be added in PR 23.
func (a *raftV2Node) WaitApplied(ctx context.Context, index uint64) error {
	if index == 0 {
		return nil
	}
	start := time.Now()
	var err error
	for {
		if a.n.CommittedIndex() >= index {
			break
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
			goto done
		case <-time.After(5 * time.Millisecond):
		}
	}
done:
	metrics.RaftV2WaitAppliedLatency.Observe(time.Since(start).Seconds())
	return err
}

// --- Apply channel ---

// ApplyCh returns a channel of v1-typed raft.LogEntry values. Because the
// channel types differ (v2.LogEntry vs raft.LogEntry), a bridge goroutine
// copies entries. The bridge is started lazily on first call.
//
// The bridge filters out non-Command entries (ConfChange, JointConfChange,
// NoOp, Snapshot — v2 LogEntryType > 0). Those are Raft protocol entries
// that the v2 actor publishes for completeness but the v1-style FSM.Apply
// path is not equipped to decode them — forwarding would produce spurious
// "unmarshal command: empty data" Error logs on every leader election
// (v2's NoOp emission rate is higher than v1's). Cluster bookkeeping for
// membership changes runs internally inside v2 (per actor.go applyConfigEntry);
// the adapter only surfaces FSM-bound Command entries.
func (a *raftV2Node) ApplyCh() <-chan raft.LogEntry {
	a.applyOnce.Do(func() {
		ch := make(chan raft.LogEntry, 64)
		a.applyBridge = ch
		src := a.n.ApplyCh()
		go func() {
			for entry := range src {
				if entry.Type != raftv2.LogEntryCommand {
					continue
				}
				ch <- raft.LogEntry{
					Term:    entry.Term,
					Index:   entry.Index,
					Command: entry.Command,
					Type:    raft.LogEntryType(entry.Type),
				}
			}
			close(ch)
		}()
	})
	return a.applyBridge
}

// --- Transport wiring ---

// SetTransport accepts v1-style callback pair and wraps them in a v2.Transport.
// Must be called before Start().
func (a *raftV2Node) SetTransport(
	sendRequestVote func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error),
	sendAppendEntries func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error),
) {
	b := &v2TransportBridge{
		sendRV: sendRequestVote,
		sendAE: sendAppendEntries,
	}
	// Drain any IS callback that was wired before SetTransport built the bridge.
	if pending := a.pendingIS.Load(); pending != nil {
		b.sendIS.Store(pending)
	}
	a.bridge = b
	a.n.SetTransport(b)
}

// SetInstallSnapshotTransport stores the outbound InstallSnapshot send callback
// on the v2 adapter's transport bridge. If called before SetTransport, the
// callback is parked on the adapter and SetTransport applies it when building
// the bridge.
func (a *raftV2Node) SetInstallSnapshotTransport(
	send func(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error),
) {
	fn := installSnapshotFn(send)
	if a.bridge != nil {
		a.bridge.sendIS.Store(&fn)
		return
	}
	a.pendingIS.Store(&fn)
}

// v2TransportBridge adapts v1-style callback pair to the v2.Transport interface.
type v2TransportBridge struct {
	sendRV func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	sendAE func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
	// sendIS is wired through SetInstallSnapshotTransport. atomic.Pointer so
	// publication is lock-free; nil means the caller did not wire IS support
	// (SendInstallSnapshot returns ErrNotImplemented in that case).
	sendIS atomic.Pointer[installSnapshotFn]
}

func (b *v2TransportBridge) SendRequestVote(peer string, args *raftv2.RequestVoteArgs) (*raftv2.RequestVoteReply, error) {
	v1args := &raft.RequestVoteArgs{
		Term:           args.Term,
		CandidateID:    args.CandidateID,
		LastLogIndex:   args.LastLogIndex,
		LastLogTerm:    args.LastLogTerm,
		PreVote:        args.PreVote,
		LeaderTransfer: args.LeaderTransfer,
	}
	reply, err := b.sendRV(peer, v1args)
	if err != nil {
		return nil, err
	}
	return &raftv2.RequestVoteReply{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (b *v2TransportBridge) SendAppendEntries(peer string, args *raftv2.AppendEntriesArgs) (*raftv2.AppendEntriesReply, error) {
	entries := make([]raft.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = raft.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
			Type:    raft.LogEntryType(e.Type),
		}
	}
	v1args := &raft.AppendEntriesArgs{
		Term:         args.Term,
		LeaderID:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	}
	reply, err := b.sendAE(peer, v1args)
	if err != nil {
		return nil, err
	}
	return &raftv2.AppendEntriesReply{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictTerm:  reply.ConflictTerm,
		ConflictIndex: reply.ConflictIndex,
	}, nil
}

// SendInstallSnapshot satisfies v2.Transport. When SetInstallSnapshotTransport
// has wired an outbound callback (e.g. via RaftV2MetaQUICTransport), delegate
// to it after translating v2 wire types ↔ v1. Otherwise return
// ErrNotImplemented so the leader skips snapshot-based replication for this
// peer (pre-M6.2 behavior).
func (b *v2TransportBridge) SendInstallSnapshot(peer string, args *raftv2.InstallSnapshotArgs) (*raftv2.InstallSnapshotReply, error) {
	sendPtr := b.sendIS.Load()
	if sendPtr == nil || *sendPtr == nil {
		return nil, raftv2.ErrNotImplemented
	}
	// v2's Configuration is []string of voter IDs; v1's wire format carries
	// raft.Server entries (ID + Suffrage). The inbound side (HandleInstallSnapshot
	// in this adapter) only forwards IDs to v2 anyway, so synthesise voter
	// entries for outbound. See translation note near HandleInstallSnapshot.
	servers := make([]raft.Server, len(args.Configuration))
	for i, id := range args.Configuration {
		servers[i] = raft.Server{ID: id, Suffrage: raft.Voter}
	}
	v1args := &raft.InstallSnapshotArgs{
		Term:              args.Term,
		LeaderID:          args.LeaderID,
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Data:              args.Data,
		Servers:           servers,
	}
	reply, err := (*sendPtr)(peer, v1args)
	if err != nil {
		return nil, err
	}
	return &raftv2.InstallSnapshotReply{Term: reply.Term}, nil
}

// SendTimeoutNow satisfies v2.Transport. The cluster package does not yet wire
// a dedicated TimeoutNow RPC; returning an error causes TransferLeadership to
// degrade gracefully — the leader still steps down (Raft §3.10), and the
// transfer target wins the next natural election instead of an immediate one.
func (b *v2TransportBridge) SendTimeoutNow(_ string, _ *raftv2.TimeoutNowArgs) (*raftv2.TimeoutNowReply, error) {
	return nil, raftv2.ErrNotImplemented
}

// --- No-op stubs ---

// SetNoOpCommand is a no-op for v2. The actor emits no-op entries on leader
// election internally without requiring an externally supplied FSM payload.
func (a *raftV2Node) SetNoOpCommand(_ []byte) {}

// RegisterObserver is a no-op for v2. v2 has no observer pattern; leadership
// detection must use State() polling. A one-time warning is logged.
func (a *raftV2Node) RegisterObserver(_ chan<- raft.Event) {
	a.once.Do(func() {
		log.Warn().Msg("raftV2Node: RegisterObserver not supported by raft v2 — " +
			"leadership detection falls back to State() polling. See plan §M4.")
	})
}

// DeregisterObserver is a no-op matching RegisterObserver.
// v2 has no observer pattern — RegisterObserver also no-ops with a one-time
// warning. Deregister is silent because callers commonly pair Register/Deregister
// in defer blocks; warning on every Stop() path would be noise.
func (a *raftV2Node) DeregisterObserver(_ chan<- raft.Event) {}

// --- Membership: direct passthrough (v2 has these methods) ---

// AddVoter passes through to the v2 node's AddVoter implementation.
func (a *raftV2Node) AddVoter(id, addr string) error {
	return translateV2SentinelErr(a.n.AddVoter(id, addr))
}

// AddVoterCtx passes through to the v2 node's AddVoterCtx implementation.
func (a *raftV2Node) AddVoterCtx(ctx context.Context, id, addr string) error {
	return translateV2SentinelErr(a.n.AddVoterCtx(ctx, id, addr))
}

// RemoveVoter passes through to the v2 node's RemoveVoter implementation.
func (a *raftV2Node) RemoveVoter(id string) error {
	return translateV2SentinelErr(a.n.RemoveVoter(id))
}

// --- Membership: passthrough to ErrNotImplemented (v2 stubs) ---

// AddLearner passes through to v2. v2 returns ErrNotImplemented (deferred
// beyond M5; v2 currently has no learner support — see
// internal/raft/v2/node.go::AddLearner). The adapter surfaces this error so
// operators see a clear failure rather than a silent skip.
func (a *raftV2Node) AddLearner(id, addr string) error { return a.n.AddLearner(id, addr) }

// PromoteToVoter passes through to v2. v2 returns ErrNotImplemented (deferred
// beyond M5 with AddLearner).
func (a *raftV2Node) PromoteToVoter(id string) error { return a.n.PromoteToVoter(id) }

// TransferLeadership passes through to v2 (Raft §3.10, implemented in
// v0.0.143.0 / PR #288).
func (a *raftV2Node) TransferLeadership() error {
	return translateV2SentinelErr(a.n.TransferLeadership())
}

// --- PeerMatchIndex: v2 does not expose per-peer replication state ---

// PeerMatchIndex returns (0, false) for v2. v2 does not expose per-peer
// matchIndex; callers (DataGroupPlanExecutor.peerCaughtUp) must treat (0, false)
// as "not caught up" and either time out or skip the wait.
func (a *raftV2Node) PeerMatchIndex(_ string) (uint64, bool) { return 0, false }

// --- Membership: sequenced bridge (v2 has no atomic ChangeMembership) ---

// ChangeMembership sequences AddVoterCtx calls for each add then RemoveVoter
// calls for each remove. Returns the first error encountered; stops processing
// on the first failure.
//
// WARN: This is NOT atomic. v1's ChangeMembership used §4.3 joint consensus
// (all changes commit as one atomic unit). v2 sequences individual membership
// changes: a partial failure (e.g. AddVoterCtx succeeds then ctx is cancelled
// before RemoveVoter) leaves the cluster in an intermediate state. The caller
// is responsible for reconciling; no rollback of already-applied adds is
// attempted. If ctx.Done() fires mid-sequence, ctx.Err() is returned immediately
// without processing remaining entries.
func (a *raftV2Node) ChangeMembership(ctx context.Context, adds []raft.ServerEntry, removes []string) error {
	for _, add := range adds {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := a.n.AddVoterCtx(ctx, add.ID, add.Address); err != nil {
			return translateV2SentinelErr(err)
		}
	}
	for _, id := range removes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := a.n.RemoveVoter(id); err != nil {
			return translateV2SentinelErr(err)
		}
	}
	return nil
}

// --- Inbound RPC handlers (M5 PR 27) ---
//
// These methods accept v1 wire types (raft.*) and translate to v2 native types
// before dispatching to raftv2.Node. The translation is byte-semantically
// equivalent to v1's HandleRequestVote / HandleAppendEntries / etc., with two
// documented gaps:
//
//   1. TimeoutNow.Term: v1 wire carries no Term field. We synthesise
//      args.Term = receiver's currentTerm so v2's Raft §3.10 stale-term check
//      accepts the call. v2 also reads args.Leader (informational only); we
//      pass the v2 node's LeaderID() so log lines are not empty. The legitimate
//      stale-leader guard is lost on the v1 wire — same exposure v1 has by
//      construction. PR 30 may extend the wire format if needed.
//
//   2. InstallSnapshotArgs.Servers vs v2's .Configuration: v1 carries
//      []Server{ID, Suffrage}; v2 only stores []string (voter IDs). Suffrage
//      info is dropped on the boundary. v2 has no learner support
//      (AddLearner returns ErrNotImplemented) so this is currently acceptable.

func (a *raftV2Node) HandleRequestVote(args *raft.RequestVoteArgs) *raft.RequestVoteReply {
	v2args := &raftv2.RequestVoteArgs{
		Term:           args.Term,
		CandidateID:    args.CandidateID,
		LastLogIndex:   args.LastLogIndex,
		LastLogTerm:    args.LastLogTerm,
		PreVote:        args.PreVote,
		LeaderTransfer: args.LeaderTransfer,
	}
	reply := a.n.HandleRequestVote(v2args)
	return &raft.RequestVoteReply{Term: reply.Term, VoteGranted: reply.VoteGranted}
}

func (a *raftV2Node) HandleAppendEntries(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	entries := make([]raftv2.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = raftv2.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
			Type:    raftv2.LogEntryType(e.Type),
		}
	}
	v2args := &raftv2.AppendEntriesArgs{
		Term:         args.Term,
		LeaderID:     args.LeaderID,
		PrevLogIndex: args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: args.LeaderCommit,
	}
	reply := a.n.HandleAppendEntries(v2args)
	return &raft.AppendEntriesReply{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictTerm:  reply.ConflictTerm,
		ConflictIndex: reply.ConflictIndex,
	}
}

func (a *raftV2Node) HandleInstallSnapshot(args *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	cfg := make([]string, len(args.Servers))
	for i, s := range args.Servers {
		cfg[i] = s.ID
	}
	v2args := &raftv2.InstallSnapshotArgs{
		Term:              args.Term,
		LeaderID:          args.LeaderID,
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Configuration:     cfg,
		Data:              args.Data,
	}
	reply := a.n.HandleInstallSnapshot(v2args)
	return &raft.InstallSnapshotReply{Term: reply.Term}
}

// HandleTimeoutNow synthesises a v2 TimeoutNowArgs using the receiver's own
// term so v2's stale-term check accepts the call. See package-level note.
func (a *raftV2Node) HandleTimeoutNow() {
	v2args := &raftv2.TimeoutNowArgs{
		Term:   a.n.Term(),
		Leader: a.n.LeaderID(),
	}
	_ = a.n.HandleTimeoutNow(v2args)
}

// CreateSnapshot satisfies cluster.RaftV2Snapshotter. Forwards to v2's
// CreateSnapshot which persists the snapshot via the configured SnapshotStore
// and compacts the log up to lastIncludedIndex inside the actor goroutine.
func (a *raftV2Node) CreateSnapshot(lastIncludedIndex uint64, data []byte) error {
	return a.n.CreateSnapshot(lastIncludedIndex, data)
}

// SnapshotStatus satisfies cluster.RaftV2Snapshotter. Returns v1's
// raft.SnapshotStatus shape (Available / Index / Term / SizeBytes) computed
// from v2's LatestSnapshot. An absent snapshot yields a zero-value status
// (Available=false), matching v1 SnapshotManager.Status semantics.
func (a *raftV2Node) SnapshotStatus() (raft.SnapshotStatus, error) {
	snap, err := a.n.LatestSnapshot()
	if err != nil {
		return raft.SnapshotStatus{}, err
	}
	if snap == nil || snap.LastIncludedIndex == 0 {
		return raft.SnapshotStatus{}, nil
	}
	return raft.SnapshotStatus{
		Available: true,
		Index:     snap.LastIncludedIndex,
		Term:      snap.LastIncludedTerm,
		SizeBytes: len(snap.Data),
	}, nil
}

// compile-time check: *raftV2Node must satisfy RaftNode.
var _ RaftNode = (*raftV2Node)(nil)
