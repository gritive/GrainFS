package cluster

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/gritive/GrainFS/internal/raft"
)

// applyChCloseStubNode is a minimal RaftNode whose ApplyCh() returns a
// channel under test control. All other methods are unused by
// RunApplyLoop's close-handling path and panic if invoked.
type applyChCloseStubNode struct {
	ch chan raft.LogEntry
}

func (s *applyChCloseStubNode) ApplyCh() <-chan raft.LogEntry { return s.ch }

// Lifecycle/identity/state — unused on the close-handling path.
func (s *applyChCloseStubNode) Start()                 {}
func (s *applyChCloseStubNode) Close()                 {}
func (s *applyChCloseStubNode) ID() string             { return "stub" }
func (s *applyChCloseStubNode) State() raft.NodeState  { return raft.Follower }
func (s *applyChCloseStubNode) Term() uint64           { return 0 }
func (s *applyChCloseStubNode) IsLeader() bool         { return false }
func (s *applyChCloseStubNode) LeaderID() string       { return "" }
func (s *applyChCloseStubNode) CommittedIndex() uint64 { return 0 }
func (s *applyChCloseStubNode) LastLogIndex() uint64   { return 0 }
func (s *applyChCloseStubNode) Configuration() raft.Configuration {
	return raft.Configuration{}
}
func (s *applyChCloseStubNode) Peers() []string                      { return nil }
func (s *applyChCloseStubNode) PeerMatchIndex(string) (uint64, bool) { return 0, false }
func (s *applyChCloseStubNode) Bootstrap() error                     { return nil }
func (s *applyChCloseStubNode) Propose([]byte) error                 { panic("unused") }
func (s *applyChCloseStubNode) ProposeWait(context.Context, []byte) (uint64, error) {
	panic("unused")
}
func (s *applyChCloseStubNode) ReadIndex(context.Context) (uint64, error) {
	panic("unused")
}
func (s *applyChCloseStubNode) WaitApplied(context.Context, uint64) error { panic("unused") }
func (s *applyChCloseStubNode) SetTransport(
	func(string, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error),
	func(string, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error),
) {
}
func (s *applyChCloseStubNode) SetInstallSnapshotTransport(
	func(string, *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error),
) {
}
func (s *applyChCloseStubNode) SetTimeoutNowTransport(
	func(string, *raft.TimeoutNowArgs) (*raft.TimeoutNowReply, error),
) {
}
func (s *applyChCloseStubNode) SetNoOpCommand([]byte)                {}
func (s *applyChCloseStubNode) RegisterObserver(chan<- raft.Event)   {}
func (s *applyChCloseStubNode) DeregisterObserver(chan<- raft.Event) {}
func (s *applyChCloseStubNode) AddVoter(string, string) error        { panic("unused") }
func (s *applyChCloseStubNode) AddVoterCtx(context.Context, string, string) error {
	panic("unused")
}
func (s *applyChCloseStubNode) RemoveVoter(string) error        { panic("unused") }
func (s *applyChCloseStubNode) AddLearner(string, string) error { panic("unused") }
func (s *applyChCloseStubNode) PromoteToVoter(string) error     { panic("unused") }
func (s *applyChCloseStubNode) TransferLeadership() error       { panic("unused") }
func (s *applyChCloseStubNode) ChangeMembership(context.Context, []raft.ServerEntry, []string) error {
	panic("unused")
}
func (s *applyChCloseStubNode) HandleRequestVote(*raft.RequestVoteArgs) *raft.RequestVoteReply {
	panic("unused")
}
func (s *applyChCloseStubNode) HandleAppendEntries(*raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	panic("unused")
}
func (s *applyChCloseStubNode) HandleInstallSnapshot(*raft.InstallSnapshotArgs) *raft.InstallSnapshotReply {
	panic("unused")
}
func (s *applyChCloseStubNode) HandleTimeoutNow()                   {}
func (s *applyChCloseStubNode) CreateSnapshot(uint64, []byte) error { panic("unused") }
func (s *applyChCloseStubNode) SnapshotStatus() (raft.SnapshotStatus, error) {
	return raft.SnapshotStatus{}, nil
}
func (s *applyChCloseStubNode) LatestSnapshot() (*raft.Snapshot, error) { return nil, nil }

// TestRunApplyLoop_ExitsOnApplyChClose asserts that DistributedBackend.RunApplyLoop
// returns promptly when the underlying RaftNode's ApplyCh is closed, instead of
// spinning on zero-value reads from a closed channel.
//
// Regression test for the data-plane Raft "empty data" log spam observed when
// the v2 adapter's ApplyCh bridge closes its outbound channel on v2 actor stop.
// Before the fix, the `case entry := <-b.node.ApplyCh()` arm fired forever with
// zero-value entries and the FSM emitted thousands of "decode command:
// unmarshal command: empty data" errors per second.
func TestRunApplyLoop_ExitsOnApplyChClose(t *testing.T) {
	ch := make(chan raft.LogEntry)
	node := &applyChCloseStubNode{ch: ch}

	b := &DistributedBackend{
		node:         node,
		snapRequests: make(chan raftSnapshotRequest),
		logger:       zerolog.Nop(),
	}

	done := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		b.RunApplyLoop(stop)
		close(done)
	}()

	// Close the apply channel; loop must observe ok=false and return.
	close(ch)

	select {
	case <-done:
		// pass
	case <-time.After(1 * time.Second):
		close(stop)
		t.Fatal("RunApplyLoop did not exit within 1s after ApplyCh close (spin bug regressed)")
	}
}
