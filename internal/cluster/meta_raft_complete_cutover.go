package cluster

import (
	"context"
	"fmt"
	"time"
)

// CompleteCutover runs the full Zero-CA cutover under one membership lock:
// present-flip, wait for every voter to register presents_per_node, then drop
// the shared cluster-key accept base.
func (m *MetaRaft) CompleteCutover(ctx context.Context, dialer AppliedIndexDialer, timeout time.Duration) error {
	if dialer == nil {
		return fmt.Errorf("CompleteCutover: applied-index dialer is required")
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if err := m.lockCutoverMembership(ctx); err != nil {
		return err
	}
	defer m.membershipMu.Unlock()

	raftConfig := NewMetaRaftConfigReader(m)
	selfID := m.cutoverSelfID()
	if err := RunPresentFlip(ctx, PresentFlipDeps{
		SelfID:           selfID,
		Voters:           raftConfig.EffectiveConfiguration,
		RegistrySPKIs:    func() map[string][32]byte { return m.FSM().PeerRaftAddrToSPKI() },
		ProposeWithIndex: m.ProposeWithIndex,
		WaitVoters: func(ctx context.Context, target uint64, voters []string) error {
			return WaitVotersApplied(ctx, target, voters, selfID, m.LastApplied, dialer, nil, 100*time.Millisecond)
		},
	}, 0); err != nil {
		return err
	}
	if err := waitAllVotersPresentPerNode(ctx, raftConfig.EffectiveConfiguration, m.FSM().AllVotersPresentsPerNode, 100*time.Millisecond); err != nil {
		return err
	}
	return RunDropClusterKey(ctx, DropClusterKeyDeps{
		SelfID:  selfID,
		Voters:  raftConfig.EffectiveConfiguration,
		AllVPN:  m.FSM().AllVotersPresentsPerNode,
		Propose: m.Propose,
	}, 0)
}

func (m *MetaRaft) cutoverSelfID() string {
	if m.cfg.RaftID != "" {
		return m.cfg.RaftID
	}
	return m.cfg.NodeID
}

func (m *MetaRaft) lockCutoverMembership(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if m.membershipMu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("CompleteCutover: acquire membership lock: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitAllVotersPresentPerNode(ctx context.Context, voters func() ([]string, uint64), all func([]string) bool, poll time.Duration) error {
	if poll <= 0 {
		poll = 100 * time.Millisecond
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	for {
		current, _ := voters()
		if all(current) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("CompleteCutover: D-cut4 gate not satisfied after present-flip: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
