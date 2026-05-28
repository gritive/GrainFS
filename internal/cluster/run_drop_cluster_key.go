package cluster

import (
	"context"
	"fmt"
	"time"
)

// DropClusterKeyDeps wires the leader-side cluster-key drop orchestration.
type DropClusterKeyDeps struct {
	SelfID  string
	Voters  func() (voters []string, configIndex uint64)
	AllVPN  func(voters []string) bool
	Propose func(ctx context.Context, typ MetaCmdType, payload []byte) error
}

// RunDropClusterKey executes PR-2b's one-way cluster-key drop flow: read the
// stamped voter set, verify D-cut4 (all voters present per-node certs), re-read
// the voter set, then propose DropClusterKeyAccept(87). Any precondition
// failure returns before proposing.
func RunDropClusterKey(ctx context.Context, deps DropClusterKeyDeps, timeout time.Duration) error {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	if deps.Voters == nil {
		return fmt.Errorf("RunDropClusterKey: voters reader is required")
	}
	if deps.AllVPN == nil {
		return fmt.Errorf("RunDropClusterKey: D-cut4 reader is required")
	}
	if deps.Propose == nil {
		return fmt.Errorf("RunDropClusterKey: proposer is required")
	}

	voters, configIdx := deps.Voters()
	if len(voters) == 0 {
		return fmt.Errorf("RunDropClusterKey: empty voter set")
	}
	if len(voters) == 1 {
		return fmt.Errorf("RunDropClusterKey: refusing on single-node cluster (voters=[%s])", voters[0])
	}
	if !deps.AllVPN(voters) {
		return fmt.Errorf("RunDropClusterKey: D-cut4 gate not satisfied")
	}
	currentVoters, _ := deps.Voters()
	if !voterSetsEqual(voters, currentVoters) {
		return fmt.Errorf("RunDropClusterKey: voter set changed during D-cut4 gate (%v -> %v) — retry after membership is stable", voters, currentVoters)
	}

	payload, err := encodeDropClusterKeyAcceptCmd(DropClusterKeyStamp{
		Voters:      append([]string(nil), voters...),
		ConfigIndex: configIdx,
	})
	if err != nil {
		return fmt.Errorf("RunDropClusterKey: encode: %w", err)
	}
	if err := deps.Propose(ctx, MetaCmdTypeDropClusterKeyAccept, payload); err != nil {
		return fmt.Errorf("RunDropClusterKey: propose: %w", err)
	}
	return nil
}

// DropClusterKey runs the complete-cutover flow while holding the meta-raft
// membership lock. That prevents a concurrent invite-join promotion from
// changing the voter set between the D-cut4 gate and the irreversible drop
// proposal.
func (m *MetaRaft) DropClusterKey(ctx context.Context, selfID string, timeout time.Duration) error {
	m.membershipMu.Lock()
	defer m.membershipMu.Unlock()

	raftConfig := NewMetaRaftConfigReader(m)
	return RunDropClusterKey(ctx, DropClusterKeyDeps{
		SelfID:  selfID,
		Voters:  raftConfig.EffectiveConfiguration,
		AllVPN:  m.FSM().AllVotersPresentsPerNode,
		Propose: m.Propose,
	}, timeout)
}
