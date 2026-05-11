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
// Unbridged methods that v2 structurally cannot satisfy (inoperative in PR 22):
//   TransferLeadership, AddLearner, PromoteToVoter, ChangeMembership,
//   PeerMatchIndex — accessed via GroupBackend.RaftNode() type assertion which
//   returns nil under v2. DataGroupPlanExecutor is therefore inoperative under
//   v2 in this PR. See plan §M4 DONE_WITH_CONCERNS note.

import (
	"context"
	"errors"
	"sync"
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
}

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
	return err
}

// --- Write path ---

func (a *raftV2Node) Propose(command []byte) error { return a.n.Propose(command) }

func (a *raftV2Node) ProposeWait(ctx context.Context, command []byte) (uint64, error) {
	start := time.Now()
	idx, err := a.n.ProposeWait(ctx, command)
	outcome := proposeOutcome(err)
	metrics.RaftV2ProposeCount.WithLabelValues(outcome).Inc()
	metrics.RaftV2ProposeLatency.WithLabelValues(outcome).Observe(time.Since(start).Seconds())
	return idx, err
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
	return a.n.ReadIndex(ctx)
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
	a.n.SetTransport(&v2TransportBridge{
		sendRV: sendRequestVote,
		sendAE: sendAppendEntries,
	})
}

// v2TransportBridge adapts v1-style callback pair to the v2.Transport interface.
type v2TransportBridge struct {
	sendRV func(peer string, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	sendAE func(peer string, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
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

// SendInstallSnapshot satisfies v2.Transport. In PR 22 the cluster package
// does not wire snapshot transport for v2; returning an error causes the
// leader to skip snapshot-based replication for this peer.
func (b *v2TransportBridge) SendInstallSnapshot(_ string, _ *raftv2.InstallSnapshotArgs) (*raftv2.InstallSnapshotReply, error) {
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

// compile-time check: *raftV2Node must satisfy RaftNode.
var _ RaftNode = (*raftV2Node)(nil)
