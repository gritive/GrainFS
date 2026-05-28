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
// stamped voter set, verify D-cut4 (all voters present per-node certs), then
// propose DropClusterKeyAccept(87). Any precondition failure returns before
// proposing.
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
