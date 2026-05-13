package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/raft"
)

// raftNodeAdapter wraps *raft.Node and exposes the cluster RaftNode interface.
type raftNodeAdapter struct {
	n           *raft.Node
	once        sync.Once // for observer no-op warning
	applyOnce   sync.Once // for apply channel bridge goroutine
	applyBridge chan raft.LogEntry
	started     atomic.Bool

	// bridge holds the raft.Transport adapter created by SetTransport. Stored on
	// the adapter so SetInstallSnapshotTransport can publish the IS callback
	// regardless of whether it is called before or after SetTransport (the
	// callback ultimately lives on the bridge via atomic.Pointer).
	bridge *raftTransportBridge
	// pendingIS holds an IS callback supplied via SetInstallSnapshotTransport
	// before SetTransport built the bridge. SetTransport drains it.
	pendingIS atomic.Pointer[installSnapshotFn]
}

// installSnapshotFn is the outbound InstallSnapshot send callback.
// Boxed via atomic.Pointer so it can be swapped lock-free.
type installSnapshotFn func(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)

// newRaftNodeAdapter constructs the adapter. Callers must call SetTransport then
// Start before using the node.
func newRaftNodeAdapter(n *raft.Node) *raftNodeAdapter {
	return &raftNodeAdapter{n: n}
}

// --- Lifecycle ---

func (a *raftNodeAdapter) Start() {
	a.started.Store(true)
	a.n.Start()
}

// Close maps the cluster lifecycle method to raft.Node.Stop.
func (a *raftNodeAdapter) Close() {
	metrics.RaftV2StopCount.Inc()
	if !a.started.Load() {
		return
	}
	a.n.Stop()
}

// --- Identity ---

func (a *raftNodeAdapter) ID() string { return a.n.ID() }

// --- State reads ---

func (a *raftNodeAdapter) State() raft.NodeState { return a.n.State() }

func (a *raftNodeAdapter) Term() uint64           { return a.n.Term() }
func (a *raftNodeAdapter) IsLeader() bool         { return a.n.IsLeader() }
func (a *raftNodeAdapter) LeaderID() string       { return a.n.LeaderID() }
func (a *raftNodeAdapter) CommittedIndex() uint64 { return a.n.CommittedIndex() }

// --- Configuration ---

func (a *raftNodeAdapter) Configuration() raft.Configuration {
	cfg := a.n.Configuration()
	servers := make([]raft.Server, len(cfg.Servers))
	for i, s := range cfg.Servers {
		servers[i] = raft.Server{
			ID:       s.ID,
			Suffrage: s.Suffrage,
		}
	}
	return raft.Configuration{Servers: servers}
}

// Peers derives peer addresses from Configuration(), filtering out self.
func (a *raftNodeAdapter) Peers() []string {
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

func (a *raftNodeAdapter) Bootstrap() error {
	err := a.n.Bootstrap()
	if err != nil {
		metrics.RaftV2BootstrapOutcome.WithLabelValues("error").Inc()
	} else {
		metrics.RaftV2BootstrapOutcome.WithLabelValues("success").Inc()
	}
	return translateRaftErr(err)
}

// --- Write path ---

func (a *raftNodeAdapter) Propose(command []byte) error {
	return translateRaftErr(a.n.Propose(command))
}

func (a *raftNodeAdapter) ProposeWait(ctx context.Context, command []byte) (uint64, error) {
	start := time.Now()
	idx, err := a.n.ProposeWait(ctx, command)
	outcome := proposeOutcome(err)
	metrics.RaftV2ProposeCount.WithLabelValues(outcome).Inc()
	metrics.RaftV2ProposeLatency.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
	return idx, translateRaftErr(err)
}

func translateRaftErr(err error) error {
	return err
}

// proposeOutcome maps a ProposeWait error to a bounded outcome label.
func proposeOutcome(err error) string {
	if err == nil {
		return "success"
	}
	if errors.Is(err, raft.ErrNotLeader) {
		return "not_leader"
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return "timeout"
	}
	return "error"
}

// --- Read path ---

func (a *raftNodeAdapter) ReadIndex(ctx context.Context) (uint64, error) {
	idx, err := a.n.ReadIndex(ctx)
	return idx, translateRaftErr(err)
}

// WaitApplied blocks until CommittedIndex >= index or ctx is done.
//
// v2 has no direct WaitApplied; we poll CommittedIndex. CommittedIndex
// advancing means the entry is committed, which is a close-enough proxy for
// "applied" in the smoke path (PR 22). A proper apply-notification mechanism
// will be added in PR 23.
func (a *raftNodeAdapter) WaitApplied(ctx context.Context, index uint64) error {
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

// The bridge filters out Raft protocol log entries (ConfChange,
// JointConfChange, NoOp) that cluster FSMs do not consume directly.
func (a *raftNodeAdapter) ApplyCh() <-chan raft.LogEntry {
	a.applyOnce.Do(func() {
		ch := make(chan raft.LogEntry, 64)
		a.applyBridge = ch
		src := a.n.ApplyCh()
		go func() {
			for entry := range src {
				if entry.Type != raft.LogEntryCommand && entry.Type != raft.LogEntrySnapshot {
					continue
				}
				ch <- raft.LogEntry{
					Term:    entry.Term,
					Index:   entry.Index,
					Command: entry.Command,
					Type:    entry.Type,
				}
			}
			close(ch)
		}()
	})
	return a.applyBridge
}

// --- Transport wiring ---

// SetTransport accepts callback pair and wraps them in a raft.Transport.
// Must be called before Start().
func (a *raftNodeAdapter) SetTransport(
	sendRequestVote func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error),
	sendAppendEntries func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error),
) {
	b := &raftTransportBridge{
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

// SetInstallSnapshotTransport stores the outbound InstallSnapshot send callback.
func (a *raftNodeAdapter) SetInstallSnapshotTransport(
	send func(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error),
) {
	fn := installSnapshotFn(send)
	if a.bridge != nil {
		a.bridge.sendIS.Store(&fn)
		return
	}
	a.pendingIS.Store(&fn)
}

// raftTransportBridge adapts callback pairs to the raft.Transport interface.
type raftTransportBridge struct {
	sendRV func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	sendAE func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
	// sendIS is wired through SetInstallSnapshotTransport. atomic.Pointer so
	// publication is lock-free; nil means the caller did not wire IS support
	// (SendInstallSnapshot returns ErrNotImplemented in that case).
	sendIS atomic.Pointer[installSnapshotFn]
}

func (b *raftTransportBridge) SendRequestVote(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
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
	return &raft.RequestVoteReply{
		Term:        reply.Term,
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (b *raftTransportBridge) SendAppendEntries(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
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
	return &raft.AppendEntriesReply{
		Term:          reply.Term,
		Success:       reply.Success,
		ConflictTerm:  reply.ConflictTerm,
		ConflictIndex: reply.ConflictIndex,
	}, nil
}

// SendInstallSnapshot satisfies raft.Transport.
func (b *raftTransportBridge) SendInstallSnapshot(peer string, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	sendPtr := b.sendIS.Load()
	if sendPtr == nil || *sendPtr == nil {
		return nil, raft.ErrNotImplemented
	}
	servers := make([]raft.Server, 0, len(args.Configuration)+len(args.Learners))
	for _, id := range args.Configuration {
		servers = append(servers, raft.Server{ID: id, Suffrage: raft.Voter})
	}
	for id := range args.Learners {
		servers = append(servers, raft.Server{ID: id, Suffrage: raft.NonVoter})
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
	return &raft.InstallSnapshotReply{Term: reply.Term}, nil
}

// SendTimeoutNow satisfies raft.Transport. The cluster package does not yet wire
// a dedicated TimeoutNow RPC; returning an error causes TransferLeadership to
// degrade gracefully — the leader still steps down (Raft §3.10), and the
// transfer target wins the next natural election instead of an immediate one.
func (b *raftTransportBridge) SendTimeoutNow(_ string, _ *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error) {
	return nil, raft.ErrNotImplemented
}

// --- No-op stubs ---

// SetNoOpCommand is a no-op. The actor emits no-op entries on leader
// election internally without requiring an externally supplied FSM payload.
func (a *raftNodeAdapter) SetNoOpCommand(_ []byte) {}

// RegisterObserver is a no-op; leadership detection uses State() polling.
func (a *raftNodeAdapter) RegisterObserver(_ chan<- raft.Event) {
	a.once.Do(func() {
		log.Warn().Msg("raftNodeAdapter: RegisterObserver not supported; leadership detection falls back to State() polling")
	})
}

// DeregisterObserver is a no-op matching RegisterObserver.
// v2 has no observer pattern — RegisterObserver also no-ops with a one-time
// warning. Deregister is silent because callers commonly pair Register/Deregister
// in defer blocks; warning on every Stop() path would be noise.
func (a *raftNodeAdapter) DeregisterObserver(_ chan<- raft.Event) {}

// --- Membership: direct passthrough (v2 has these methods) ---

// AddVoter passes through to the v2 node's AddVoter implementation.
func (a *raftNodeAdapter) AddVoter(id, addr string) error {
	return translateRaftErr(a.n.AddVoter(id, addr))
}

// AddVoterCtx passes through to the v2 node's AddVoterCtx implementation.
func (a *raftNodeAdapter) AddVoterCtx(ctx context.Context, id, addr string) error {
	return translateRaftErr(a.n.AddVoterCtx(ctx, id, addr))
}

// RemoveVoter passes through to the v2 node's RemoveVoter implementation.
func (a *raftNodeAdapter) RemoveVoter(id string) error {
	return translateRaftErr(a.n.RemoveVoter(id))
}

// --- Membership: learner support (live since M6.0, Path B) ---

// AddLearner passes through to v2's single-phase AddLearner. Path B:
// quorum math unchanged, learner immediately receives replication but
// never contributes to commit advance. See plan §M6.0.0 amendment.
func (a *raftNodeAdapter) AddLearner(id, addr string) error {
	return translateRaftErr(a.n.AddLearner(id, addr))
}

// PromoteToVoter passes through to v2's two-entry Path B promotion
// sequence. Returns raft.ErrLearnerNotCaughtUp when the learner is
// lagging (callers should wait + retry).
func (a *raftNodeAdapter) PromoteToVoter(id string) error {
	return translateRaftErr(a.n.PromoteToVoter(id))
}

// TransferLeadership passes through to v2 (Raft §3.10, implemented in
// v0.0.143.0 / PR #288).
func (a *raftNodeAdapter) TransferLeadership() error {
	return translateRaftErr(a.n.TransferLeadership())
}

// --- PeerMatchIndex: v2 does not expose per-peer replication state ---

// PeerMatchIndex returns (0, false) for v2. v2 does not expose per-peer
// matchIndex; callers (DataGroupPlanExecutor.peerCaughtUp) must treat (0, false)
// as "not caught up" and either time out or skip the wait.
func (a *raftNodeAdapter) PeerMatchIndex(_ string) (uint64, bool) { return 0, false }

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
func (a *raftNodeAdapter) ChangeMembership(ctx context.Context, adds []raft.ServerEntry, removes []string) error {
	for _, add := range adds {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := a.n.AddVoterCtx(ctx, add.ID, add.Address); err != nil {
			return translateRaftErr(err)
		}
	}
	for _, id := range removes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := a.n.RemoveVoter(id); err != nil {
			return translateRaftErr(err)
		}
	}
	return nil
}

// --- Inbound RPC handlers (M5 PR 27) ---
//
// These methods accept v1 wire types (raft.*) and translate to v2 native types
// before dispatching to raft.Node. The translation is byte-semantically
// equivalent to v1's HandleRequestVote / HandleAppendEntries / etc., with one
// documented gap:
//
//   1. TimeoutNow.Term: v1 wire carries no Term field. We synthesise
//      args.Term = receiver's currentTerm so v2's Raft §3.10 stale-term check
//      accepts the call. v2 also reads args.Leader (informational only); we
//      pass the v2 node's LeaderID() so log lines are not empty. The legitimate
//      stale-leader guard is lost on the v1 wire — same exposure v1 has by
//      construction. PR 30 may extend the wire format if needed.

func (a *raftNodeAdapter) HandleRequestVote(args *raft.RequestVoteArgs) *raft.RequestVoteReply {
	v2args := &raft.RequestVoteArgs{
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

func (a *raftNodeAdapter) HandleAppendEntries(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	entries := make([]raft.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = raft.LogEntry{
			Term:    e.Term,
			Index:   e.Index,
			Command: e.Command,
			Type:    raft.LogEntryType(e.Type),
		}
	}
	v2args := &raft.AppendEntriesArgs{
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

func (a *raftNodeAdapter) HandleInstallSnapshot(args *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	cfg := make([]string, 0, len(args.Servers))
	learners := make(map[string]string)
	for _, s := range args.Servers {
		if s.Suffrage == raft.NonVoter {
			learners[s.ID] = ""
			continue
		}
		cfg = append(cfg, s.ID)
	}
	if len(learners) == 0 {
		learners = nil
	}
	v2args := &raft.InstallSnapshotArgs{
		Term:              args.Term,
		LeaderID:          args.LeaderID,
		LastIncludedIndex: args.LastIncludedIndex,
		LastIncludedTerm:  args.LastIncludedTerm,
		Configuration:     cfg,
		Learners:          learners,
		Data:              args.Data,
	}
	reply := a.n.HandleInstallSnapshot(v2args)
	return &raft.InstallSnapshotReply{Term: reply.Term}
}

// HandleTimeoutNow synthesises a v2 TimeoutNowArgs using the receiver's own
// term so v2's stale-term check accepts the call. See package-level note.
func (a *raftNodeAdapter) HandleTimeoutNow() {
	v2args := &raft.TimeoutNowArgs{
		Term:   a.n.Term(),
		Leader: a.n.LeaderID(),
	}
	_ = a.n.HandleTimeoutNow(v2args)
}

// CreateSnapshot satisfies cluster.RaftV2Snapshotter. Forwards to v2's
// CreateSnapshot which persists the snapshot via the configured SnapshotStore
// and compacts the log up to lastIncludedIndex inside the actor goroutine.
func (a *raftNodeAdapter) CreateSnapshot(lastIncludedIndex uint64, data []byte) error {
	return a.n.CreateSnapshot(lastIncludedIndex, data)
}

// SnapshotStatus satisfies cluster.RaftV2Snapshotter. Returns v1's
// raft.SnapshotStatus shape (Available / Index / Term / SizeBytes) computed
// from v2's LatestSnapshot. An absent snapshot yields a zero-value status
// (Available=false), matching v1 SnapshotManager.Status semantics.
func (a *raftNodeAdapter) SnapshotStatus() (raft.SnapshotStatus, error) {
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

func (a *raftNodeAdapter) LatestSnapshot() (*raft.Snapshot, error) {
	snap, err := a.n.LatestSnapshot()
	if err != nil || snap == nil {
		return nil, err
	}
	return &raft.Snapshot{
		Index:   snap.LastIncludedIndex,
		Term:    snap.LastIncludedTerm,
		Servers: serversFromSnapshot(snap),
		Data:    append([]byte(nil), snap.Data...),
	}, nil
}

func serversFromSnapshot(snap *raft.Snapshot) []raft.Server {
	servers := make([]raft.Server, 0, len(snap.Configuration)+len(snap.Learners))
	for _, id := range snap.Configuration {
		servers = append(servers, raft.Server{ID: id, Suffrage: raft.Voter})
	}
	for id := range snap.Learners {
		servers = append(servers, raft.Server{ID: id, Suffrage: raft.NonVoter})
	}
	return servers
}

// compile-time check: *raftNodeAdapter must satisfy RaftNode.
var _ RaftNode = (*raftNodeAdapter)(nil)
